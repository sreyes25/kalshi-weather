"""
Disciplined entry decision engine for weather-market trading.

This module turns raw BUY signals into a single high-quality trade decision
by combining probability quality, pricing edge, feasibility, timing, and risk.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from kalshi_weather.core.models import (
    BracketType,
    DailyObservation,
    PeakPrediction,
    TradeDecisionSnapshot,
    TradingSignal,
    TrajectoryAssessment,
)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_linear_trend_per_hour(points: List[Tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    xs = [x for x, _ in points]
    ys = [y for _, y in points]
    if xs[-1] - xs[0] <= 0.0:
        return 0.0
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom <= 1e-9:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom


@dataclass
class RankedSignal:
    signal: TradingSignal
    model_prob: float
    model_trend_pp: float
    market_lag_pp: float
    score: float


class DecisionEngine:
    """
    Converts model/market inputs into disciplined entry decisions.
    """

    def __init__(
        self,
        *,
        min_confidence_threshold: float,
        min_edge_threshold: float,
        max_risk_fraction: float,
        target_profit_fraction: float,
        max_trades_per_day: int,
        min_model_trend_pp: float,
        min_market_lag_pp: float,
        fractional_kelly_fraction: float = 0.50,
        kelly_max_fraction_per_trade: float = 0.20,
        kelly_fee_rate: float = 0.10,
        dynamic_edge_floor_enabled: bool = True,
        dynamic_edge_std_bump_per_f: float = 0.005,
        dynamic_edge_boundary_bump_max: float = 0.010,
        dynamic_edge_max_extra: float = 0.020,
    ):
        self.min_confidence_threshold = min_confidence_threshold
        self.min_edge_threshold = min_edge_threshold
        self.max_risk_fraction = max_risk_fraction
        self.target_profit_fraction = target_profit_fraction
        self.max_trades_per_day = max_trades_per_day
        self.min_model_trend_pp = min_model_trend_pp
        self.min_market_lag_pp = min_market_lag_pp
        self.fractional_kelly_fraction = _clamp(float(fractional_kelly_fraction), 0.0, 1.0)
        self.kelly_max_fraction_per_trade = max(0.0, float(kelly_max_fraction_per_trade))
        self.kelly_fee_rate = _clamp(float(kelly_fee_rate), 0.0, 0.99)
        self.dynamic_edge_floor_enabled = bool(dynamic_edge_floor_enabled)
        self.dynamic_edge_std_bump_per_f = max(0.0, float(dynamic_edge_std_bump_per_f))
        self.dynamic_edge_boundary_bump_max = max(0.0, float(dynamic_edge_boundary_bump_max))
        self.dynamic_edge_max_extra = max(0.0, float(dynamic_edge_max_extra))

    def estimate_peak_prediction(
        self,
        *,
        adjusted_mean_f: float,
        adjusted_std_f: float,
        observation: Optional[DailyObservation],
        trajectory_assessment: Optional[TrajectoryAssessment],
        now_local: datetime,
    ) -> PeakPrediction:
        observed_high = float(observation.observed_high_f) if observation is not None else adjusted_mean_f
        recent = observation.readings[-12:] if observation and observation.readings else []
        current_temp = float(recent[-1].reported_temp_f) if recent else observed_high

        trend_points: List[Tuple[float, float]] = []
        if recent:
            t0 = recent[0].timestamp
            for row in recent:
                x = (row.timestamp - t0).total_seconds() / 3600.0
                trend_points.append((x, float(row.reported_temp_f)))
        trend_fph = _safe_linear_trend_per_hour(trend_points)

        day_hour = now_local.hour + (now_local.minute / 60.0)
        hours_remaining = max(0.0, min(8.0, 19.5 - day_hour))
        trend_contrib = max(0.0, trend_fph) * min(2.0, hours_remaining) * 0.60

        trajectory_contrib = 0.0
        if trajectory_assessment is not None:
            trajectory_contrib = max(0.0, float(trajectory_assessment.expected_remaining_warming_f))
        else:
            trajectory_contrib = max(0.0, adjusted_mean_f - max(current_temp, observed_high)) * 0.60

        slowing_penalty = 0.0
        if len(trend_points) >= 6:
            mid = len(trend_points) // 2
            early_slope = _safe_linear_trend_per_hour(trend_points[:mid])
            late_slope = _safe_linear_trend_per_hour(trend_points[mid:])
            if late_slope < early_slope:
                slowing_penalty = max(0.0, early_slope - late_slope) * 0.35

        expected_remaining = max(0.0, trajectory_contrib + trend_contrib - slowing_penalty)
        if day_hour >= 18.0:
            expected_remaining = min(expected_remaining, 1.4)
        elif day_hour >= 16.0:
            expected_remaining = min(expected_remaining, 2.2)
        else:
            expected_remaining = min(expected_remaining, 4.2)

        predicted_high = max(
            observed_high,
            current_temp,
            (0.55 * adjusted_mean_f) + (0.45 * (current_temp + expected_remaining)),
        )
        max_feasible_temp = max(
            observed_high,
            current_temp + expected_remaining + 0.75 * max(0.8, adjusted_std_f),
        )

        std_conf = _clamp(1.0 - (adjusted_std_f - 1.2) / 4.5, 0.20, 0.98)
        time_conf = _clamp((day_hour - 11.0) / 8.0, 0.15, 1.0)
        traj_conf = 0.55
        if trajectory_assessment is not None:
            traj_conf = _clamp(
                0.50 * (1.0 - float(trajectory_assessment.prob_exceed_observed_high))
                + 0.50 * float(trajectory_assessment.lock_confidence),
                0.05,
                0.99,
            )
        confidence = _clamp((0.42 * std_conf) + (0.33 * traj_conf) + (0.25 * time_conf), 0.05, 0.99)

        reasoning = (
            f"trend={trend_fph:+.2f}F/hr, expected_warm={expected_remaining:.2f}F, "
            f"hours_left={hours_remaining:.1f}"
        )
        return PeakPrediction(
            predicted_high_f=predicted_high,
            confidence=confidence,
            expected_remaining_warming_f=expected_remaining,
            trend_f_per_hour=trend_fph,
            max_feasible_temp_f=max_feasible_temp,
            reasoning=reasoning,
        )

    def _is_bracket_feasible(
        self,
        signal: TradingSignal,
        *,
        peak_prediction: PeakPrediction,
        observation: Optional[DailyObservation],
    ) -> bool:
        bracket = signal.bracket
        observed_high = float(observation.observed_high_f) if observation is not None else None
        max_temp = peak_prediction.max_feasible_temp_f

        if observed_high is not None:
            obs_floor = int(math.floor(observed_high))
            if bracket.bracket_type == BracketType.BETWEEN and bracket.upper_bound is not None:
                if obs_floor > int(bracket.upper_bound):
                    return False
            elif bracket.bracket_type == BracketType.LESS_THAN and bracket.upper_bound is not None:
                if obs_floor >= int(bracket.upper_bound):
                    return False

        if max_temp is None:
            return True
        if bracket.bracket_type == BracketType.BETWEEN and bracket.lower_bound is not None:
            return float(bracket.lower_bound) <= float(max_temp) + 1e-9
        if bracket.bracket_type == BracketType.GREATER_THAN and bracket.lower_bound is not None:
            return (float(bracket.lower_bound) + 1.0) <= float(max_temp) + 1e-9
        return True

    def choose_trade(
        self,
        *,
        signals: List[TradingSignal],
        model_probabilities: Dict[str, float],
        previous_model_probabilities: Dict[str, float],
        previous_yes_ask_by_ticker: Dict[str, int],
        observation: Optional[DailyObservation],
        trajectory_assessment: Optional[TrajectoryAssessment],
        adjusted_mean_f: float,
        adjusted_std_f: float,
        now_local: datetime,
        trades_today: int,
        open_position_count: int,
        max_active_positions: int,
        min_entry_price_cents: int,
        max_entry_price_cents: int,
        max_spread_cents: int,
        min_volume: int,
    ) -> tuple[TradeDecisionSnapshot, Optional[TradingSignal], PeakPrediction]:
        peak_prediction = self.estimate_peak_prediction(
            adjusted_mean_f=adjusted_mean_f,
            adjusted_std_f=adjusted_std_f,
            observation=observation,
            trajectory_assessment=trajectory_assessment,
            now_local=now_local,
        )

        if trades_today >= max(0, self.max_trades_per_day):
            return (
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason=(
                        f"daily trade cap hit ({trades_today}/{max(0, self.max_trades_per_day)}); "
                        "no-overtrading rule active"
                    ),
                    max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
                ),
                None,
                peak_prediction,
            )

        if open_position_count >= max(0, max_active_positions):
            return (
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason=f"active positions {open_position_count} >= max {max_active_positions}",
                    max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
                ),
                None,
                peak_prediction,
            )

        buy_signals = [s for s in signals if s.direction == "BUY"]
        if not buy_signals:
            return (
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason="no BUY signals after model/viability processing",
                    max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
                ),
                None,
                peak_prediction,
            )

        candidates: List[RankedSignal] = []
        failure_reasons: List[str] = []

        for signal in buy_signals:
            ticker = signal.bracket.ticker
            model_prob = float(model_probabilities.get(ticker, signal.model_prob))
            ask_c = int(signal.bracket.yes_ask)
            spread_c = int(signal.bracket.yes_ask) - int(signal.bracket.yes_bid)
            vol = int(signal.bracket.volume)

            if model_prob < self.min_confidence_threshold:
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: model_prob {model_prob:.1%} < confidence floor {self.min_confidence_threshold:.1%}"
                )
                continue
            dynamic_edge_floor = float(self.min_edge_threshold)
            if self.dynamic_edge_floor_enabled:
                # Raise minimum edge in noisier regimes and for near-coinflip bins.
                std_excess = max(0.0, float(adjusted_std_f) - 1.5)
                std_bump = std_excess * self.dynamic_edge_std_bump_per_f
                boundary_factor = _clamp(1.0 - (abs(model_prob - 0.5) / 0.25), 0.0, 1.0)
                boundary_bump = boundary_factor * self.dynamic_edge_boundary_bump_max
                extra_edge = min(self.dynamic_edge_max_extra, std_bump + boundary_bump)
                dynamic_edge_floor += extra_edge
            if signal.edge < dynamic_edge_floor:
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: edge {signal.edge:.1%} < edge floor {dynamic_edge_floor:.1%}"
                )
                continue
            if not (min_entry_price_cents <= ask_c <= max_entry_price_cents):
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: ask {ask_c}c outside [{min_entry_price_cents},{max_entry_price_cents}]"
                )
                continue
            if spread_c > max_spread_cents:
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: spread {spread_c}c > {max_spread_cents}c"
                )
                continue
            if vol < min_volume:
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: volume {vol} < {min_volume}"
                )
                continue
            if not self._is_bracket_feasible(
                signal,
                peak_prediction=peak_prediction,
                observation=observation,
            ):
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: infeasible vs max_possible {peak_prediction.max_feasible_temp_f:.1f}F"
                )
                continue

            has_timing_baseline = (
                ticker not in previous_model_probabilities
                or ticker not in previous_yes_ask_by_ticker
            )
            prev_model_prob = float(previous_model_probabilities.get(ticker, model_prob))
            prev_ask_c = int(previous_yes_ask_by_ticker.get(ticker, ask_c))
            model_trend_pp = (model_prob - prev_model_prob) * 100.0
            market_move_pp = (float(ask_c) - float(prev_ask_c))
            market_lag_pp = model_trend_pp - market_move_pp

            timing_ok = has_timing_baseline or (
                model_trend_pp >= self.min_model_trend_pp
                and market_lag_pp >= self.min_market_lag_pp
            )
            if not timing_ok:
                failure_reasons.append(
                    f"{signal.bracket.subtitle}: timing fail (model_trend={model_trend_pp:+.1f}pp, lag={market_lag_pp:+.1f}pp)"
                )
                continue

            score = (
                (2.2 * signal.edge)
                + (0.8 * signal.confidence)
                + (max(0.0, market_lag_pp) / 100.0)
                + (0.6 * peak_prediction.confidence)
            )
            candidates.append(
                RankedSignal(
                    signal=signal,
                    model_prob=model_prob,
                    model_trend_pp=model_trend_pp,
                    market_lag_pp=market_lag_pp,
                    score=score,
                )
            )

        if not candidates:
            reason = failure_reasons[0] if failure_reasons else "all BUY signals filtered out"
            return (
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason=reason,
                    max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
                ),
                None,
                peak_prediction,
            )

        candidates.sort(key=lambda r: r.score, reverse=True)
        top = candidates[0]
        signal = top.signal
        entry_price = int(signal.bracket.yes_ask)
        target_exit = min(
            99,
            max(
                entry_price + 1,
                int(math.ceil(entry_price * (1.0 + max(0.01, self.target_profit_fraction)))),
            ),
        )
        snapshot = TradeDecisionSnapshot(
            status="TRADE",
            reason="all confidence/edge/feasibility/timing gates passed",
            ticker=signal.bracket.ticker,
            bracket_subtitle=signal.bracket.subtitle,
            yes_price_cents=entry_price,
            model_prob=top.model_prob,
            market_prob=signal.market_prob,
            edge=signal.edge,
            confidence=signal.confidence,
            target_exit_price_cents=target_exit,
            max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
            timing_lag_pp=top.market_lag_pp,
            model_trend_pp=top.model_trend_pp,
        )
        return snapshot, signal, peak_prediction

    def size_contracts(
        self,
        *,
        snapshot: TradeDecisionSnapshot,
        entry_price_cents: int,
        max_affordable_contracts: int,
        min_contracts: int,
    ) -> int:
        if max_affordable_contracts <= 0:
            return 0

        if snapshot.model_prob is not None:
            p = _clamp(float(snapshot.model_prob), 1e-4, 1.0 - 1e-4)
            q = 1.0 - p
            price = _clamp(float(entry_price_cents) / 100.0, 0.01, 0.99)
            # Net odds after fee haircut on winnings.
            b = ((1.0 - price) * (1.0 - self.kelly_fee_rate)) / price
            if b <= 1e-9:
                return 0
            full_kelly = ((b * p) - q) / b
            if full_kelly <= 0.0:
                return 0
            kelly_fraction = max(0.0, full_kelly) * self.fractional_kelly_fraction
            if snapshot.confidence is not None:
                kelly_fraction *= _clamp(float(snapshot.confidence), 0.25, 1.0)
            risk_fraction = _clamp(kelly_fraction, 0.0, self.kelly_max_fraction_per_trade)
            if self.max_risk_fraction <= 1e-9:
                return 0
            size_factor = _clamp(risk_fraction / self.max_risk_fraction, 0.0, 1.0)
        elif snapshot.edge is not None and snapshot.confidence is not None:
            edge_multiplier = max(0.0, snapshot.edge / max(1e-6, self.min_edge_threshold))
            raw_size = snapshot.confidence * edge_multiplier
            size_factor = _clamp(raw_size / 3.0, 0.15, 1.0)
        else:
            return 0

        sized = int(math.floor(float(max_affordable_contracts) * size_factor))
        sized = max(min_contracts, sized)
        sized = min(max_affordable_contracts, sized)
        return max(0, sized)
