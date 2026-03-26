"""
Historical replay backtesting for the disciplined decision engine.

Backtests run from local hourly model snapshots in
`logs/temperature_progression/<CITY>/<YYYY-MM-DD>.csv`.
Because historical orderbook snapshots are not stored in this project, market
prices are approximated with a deterministic lag model so timing/edge logic can
still be evaluated consistently across days.
"""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Dict, List, Optional

from kalshi_weather.config import CityConfig, DEFAULT_CITY
from kalshi_weather.data.historical import fetch_settlement


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normal_cdf(x: float, mu: float, sigma: float) -> float:
    if sigma <= 0:
        return 1.0 if x >= mu else 0.0
    z = (x - mu) / (sigma * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


@dataclass
class BacktestTrade:
    timestamp: datetime
    predicted_high_f: int
    model_prob: float
    market_prob: float
    edge: float
    confidence: float
    contracts: int
    pnl_dollars: float = 0.0
    cost_dollars: float = 0.0
    win: bool = False


@dataclass
class BacktestDayResult:
    date: str
    settlement_high_f: float
    final_predicted_high_f: int
    prediction_correct: bool
    lead_time_hours: Optional[float]
    settlement_source: str = "official"
    trades: List[BacktestTrade] = field(default_factory=list)

    @property
    def pnl_dollars(self) -> float:
        return sum(t.pnl_dollars for t in self.trades)

    @property
    def gross_cost_dollars(self) -> float:
        return sum(t.cost_dollars for t in self.trades)


@dataclass
class BacktestSummary:
    city_code: str
    days_requested: int
    days_tested: int
    prediction_accuracy: float
    avg_peak_prediction_lead_hours: float
    total_trades: int
    win_rate: float
    total_pnl_dollars: float
    roi: float
    starting_balance_dollars: float
    ending_balance_dollars: float
    return_pct: float
    synthetic_market_model: bool
    proxy_settlement_days: int
    day_results: List[BacktestDayResult] = field(default_factory=list)


class BacktestingEngine:
    """
    Replays hourly local logs with deterministic lagged-market approximation.
    """

    def __init__(
        self,
        *,
        city: Optional[CityConfig] = None,
        logs_dir: Path = Path("logs/temperature_progression"),
    ):
        self.city = city or DEFAULT_CITY
        self.logs_dir = logs_dir

    def _load_daily_rows(self, path: Path) -> List[Dict[str, str]]:
        with path.open("r", encoding="utf-8") as fp:
            rows = list(csv.DictReader(fp))
        rows.sort(key=lambda r: r.get("timestamp", ""))
        return rows

    def _hourly_snapshots(self, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        by_hour: Dict[datetime, Dict[str, str]] = {}
        for row in rows:
            ts_raw = row.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(ts_raw)
            except ValueError:
                continue
            hour_key = ts.replace(minute=0, second=0, microsecond=0)
            by_hour[hour_key] = row
        return [by_hour[h] for h in sorted(by_hour.keys())]

    def _simulate_day(
        self,
        *,
        date_str: str,
        snapshots: List[Dict[str, str]],
        settlement_high_f: float,
        min_confidence_threshold: float,
        min_edge_threshold: float,
        max_trades_per_day: int,
        base_size: float,
    ) -> Optional[BacktestDayResult]:
        if not snapshots:
            return None

        settlement_i = int(round(settlement_high_f))
        trades: List[BacktestTrade] = []
        predicted_history: List[tuple[datetime, int]] = []

        market_prob_prev: Optional[float] = None
        for row in snapshots:
            ts_raw = row.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(ts_raw)
            except ValueError:
                continue
            try:
                model_mean = float(row.get("final_model_mean") or row.get("raw_forecast_mean") or 0.0)
            except (TypeError, ValueError):
                continue
            try:
                model_std = float(row.get("final_model_std") or row.get("forecast_std") or 2.5)
            except (TypeError, ValueError):
                model_std = 2.5
            model_std = max(0.8, model_std)

            predicted_high = int(round(model_mean))
            predicted_history.append((ts, predicted_high))
            model_prob = _clamp(
                _normal_cdf(predicted_high + 0.5, model_mean, model_std)
                - _normal_cdf(predicted_high - 0.5, model_mean, model_std),
                0.001,
                0.999,
            )
            confidence = _clamp(1.0 - (model_std - 1.0) / 4.5, 0.05, 0.99)

            # Deterministic lagging market approximation.
            if market_prob_prev is None:
                market_prob = model_prob * 0.92
            else:
                market_prob = (0.78 * market_prob_prev) + (0.22 * model_prob)
            market_prob = _clamp(market_prob, 0.001, 0.999)
            edge = model_prob - market_prob

            market_prob_prev = market_prob
            if len(trades) >= max(0, max_trades_per_day):
                continue
            if model_prob < min_confidence_threshold:
                continue
            if edge < min_edge_threshold:
                continue

            edge_multiplier = max(1.0, edge / max(1e-6, min_edge_threshold))
            contracts = max(1, int(round(base_size * confidence * edge_multiplier)))
            trades.append(
                BacktestTrade(
                    timestamp=ts,
                    predicted_high_f=predicted_high,
                    model_prob=model_prob,
                    market_prob=market_prob,
                    edge=edge,
                    confidence=confidence,
                    contracts=contracts,
                )
            )

        if not predicted_history:
            return None

        final_predicted_high = predicted_history[-1][1]
        prediction_correct = final_predicted_high == settlement_i

        lock_ts: Optional[datetime] = None
        for idx, (ts, pred) in enumerate(predicted_history):
            if pred != settlement_i:
                continue
            if all(next_pred == settlement_i for _, next_pred in predicted_history[idx:]):
                lock_ts = ts
                break

        lead_time_hours: Optional[float] = None
        if lock_ts is not None:
            lead_time_hours = max(
                0.0,
                (predicted_history[-1][0] - lock_ts).total_seconds() / 3600.0,
            )

        for trade in trades:
            payout = 1.0 if trade.predicted_high_f == settlement_i else 0.0
            trade.pnl_dollars = (payout - trade.market_prob) * trade.contracts
            trade.cost_dollars = trade.market_prob * trade.contracts
            trade.win = trade.pnl_dollars > 0

        return BacktestDayResult(
            date=date_str,
            settlement_high_f=settlement_high_f,
            final_predicted_high_f=final_predicted_high,
            prediction_correct=prediction_correct,
            lead_time_hours=lead_time_hours,
            trades=trades,
        )

    def _proxy_settlement_from_rows(self, rows: List[Dict[str, str]]) -> Optional[float]:
        observed_values: List[float] = []
        for row in rows:
            value = row.get("observed_high_f")
            if value is None or str(value).strip() == "":
                continue
            try:
                observed_values.append(float(value))
            except (TypeError, ValueError):
                continue
        if not observed_values:
            return None
        return max(observed_values)

    def run(
        self,
        *,
        days: int = 30,
        min_confidence_threshold: float = 0.25,
        min_edge_threshold: float = 0.05,
        max_trades_per_day: int = 5,
        base_size: float = 1.0,
        starting_balance_dollars: float = 20.0,
    ) -> BacktestSummary:
        city_dir = self.logs_dir / self.city.code.upper()
        if not city_dir.exists():
            return BacktestSummary(
                city_code=self.city.code,
                days_requested=days,
                days_tested=0,
                prediction_accuracy=0.0,
                avg_peak_prediction_lead_hours=0.0,
                total_trades=0,
                win_rate=0.0,
                total_pnl_dollars=0.0,
                roi=0.0,
                starting_balance_dollars=float(starting_balance_dollars),
                ending_balance_dollars=float(starting_balance_dollars),
                return_pct=0.0,
                synthetic_market_model=True,
                proxy_settlement_days=0,
                day_results=[],
            )

        csv_files = sorted(city_dir.glob("*.csv"))
        if not csv_files:
            return BacktestSummary(
                city_code=self.city.code,
                days_requested=days,
                days_tested=0,
                prediction_accuracy=0.0,
                avg_peak_prediction_lead_hours=0.0,
                total_trades=0,
                win_rate=0.0,
                total_pnl_dollars=0.0,
                roi=0.0,
                starting_balance_dollars=float(starting_balance_dollars),
                ending_balance_dollars=float(starting_balance_dollars),
                return_pct=0.0,
                synthetic_market_model=True,
                proxy_settlement_days=0,
                day_results=[],
            )

        selected = csv_files[-max(1, days):]
        results: List[BacktestDayResult] = []
        proxy_days = 0
        for path in selected:
            date_str = path.stem
            rows = self._load_daily_rows(path)
            settlement = fetch_settlement(date_str, city=self.city, use_fallback=True)
            settlement_source = "official"
            settlement_high_f: Optional[float] = None
            if settlement is not None:
                settlement_high_f = float(settlement.settlement_high_f)
                settlement_source = str(settlement.source or "official")
            else:
                settlement_high_f = self._proxy_settlement_from_rows(rows)
                if settlement_high_f is None:
                    continue
                settlement_source = "proxy_observed_high"
                proxy_days += 1

            snapshots = self._hourly_snapshots(rows)
            day_result = self._simulate_day(
                date_str=date_str,
                snapshots=snapshots,
                settlement_high_f=float(settlement_high_f),
                min_confidence_threshold=min_confidence_threshold,
                min_edge_threshold=min_edge_threshold,
                max_trades_per_day=max_trades_per_day,
                base_size=base_size,
            )
            if day_result is not None:
                day_result.settlement_source = settlement_source
                results.append(day_result)

        if not results:
            return BacktestSummary(
                city_code=self.city.code,
                days_requested=days,
                days_tested=0,
                prediction_accuracy=0.0,
                avg_peak_prediction_lead_hours=0.0,
                total_trades=0,
                win_rate=0.0,
                total_pnl_dollars=0.0,
                roi=0.0,
                starting_balance_dollars=float(starting_balance_dollars),
                ending_balance_dollars=float(starting_balance_dollars),
                return_pct=0.0,
                synthetic_market_model=True,
                proxy_settlement_days=proxy_days,
                day_results=[],
            )

        starting_balance = max(0.01, float(starting_balance_dollars))
        balance = float(starting_balance)
        for day in results:
            settlement_i = int(round(day.settlement_high_f))
            adjusted_trades: List[BacktestTrade] = []
            for trade in day.trades:
                per_contract_cost = max(0.001, float(trade.market_prob))
                affordable = int(balance // per_contract_cost)
                contracts = max(0, min(int(trade.contracts), affordable))
                if contracts <= 0:
                    continue
                trade.contracts = contracts
                trade.cost_dollars = per_contract_cost * contracts
                payout = 1.0 if trade.predicted_high_f == settlement_i else 0.0
                trade.pnl_dollars = (payout - per_contract_cost) * contracts
                trade.win = trade.pnl_dollars > 0.0
                balance += trade.pnl_dollars
                adjusted_trades.append(trade)
            day.trades = adjusted_trades

        total_days = len(results)
        correct_days = sum(1 for r in results if r.prediction_correct)
        lead_values = [r.lead_time_hours for r in results if r.lead_time_hours is not None]
        trades = [t for r in results for t in r.trades]
        total_trades = len(trades)
        win_count = sum(1 for t in trades if t.win)
        total_pnl = sum(t.pnl_dollars for t in trades)
        ending_balance = balance

        return BacktestSummary(
            city_code=self.city.code,
            days_requested=days,
            days_tested=total_days,
            prediction_accuracy=(correct_days / total_days) if total_days else 0.0,
            avg_peak_prediction_lead_hours=mean(lead_values) if lead_values else 0.0,
            total_trades=total_trades,
            win_rate=(win_count / total_trades) if total_trades else 0.0,
            total_pnl_dollars=total_pnl,
            roi=(total_pnl / starting_balance) if starting_balance > 0 else 0.0,
            starting_balance_dollars=starting_balance,
            ending_balance_dollars=ending_balance,
            return_pct=((ending_balance - starting_balance) / starting_balance) if starting_balance > 0 else 0.0,
            synthetic_market_model=True,
            proxy_settlement_days=proxy_days,
            day_results=results,
        )
