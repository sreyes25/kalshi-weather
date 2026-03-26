"""LLM-friendly daily audit log snapshots for end-of-day review."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from kalshi_weather.core.models import MarketAnalysis

logger = logging.getLogger(__name__)


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


@dataclass
class DailyLLMLogWriter:
    """
    Persist one JSON snapshot per target day for external model review.

    The file updates intraday and is frozen when finalization criteria are met.
    """

    city_code: str
    timezone: ZoneInfo
    base_dir: Path = Path("logs/llm_daily")
    earliest_finalize_hour_local: int = 16
    lock_confidence_threshold: float = 0.90
    max_exceed_prob: float = 0.10
    top_brackets: int = 8

    def _resolve_file_path(self, target_date: str) -> Path:
        city_dir = self.base_dir / self.city_code.upper()
        city_dir.mkdir(parents=True, exist_ok=True)
        return city_dir / f"{target_date}.json"

    def _read_existing(self, path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {}

    def _write_payload(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)

    def _should_finalize(self, analysis: MarketAnalysis) -> tuple[bool, Optional[str]]:
        now_local = (
            analysis.analyzed_at.astimezone(self.timezone)
            if analysis.analyzed_at.tzinfo is not None
            else analysis.analyzed_at.replace(tzinfo=self.timezone)
        )
        now_day = now_local.date().isoformat()
        if now_day > analysis.target_date:
            return True, "market_day_rolled_over"

        obs = analysis.observation
        traj = analysis.trajectory_assessment
        if obs is None or traj is None:
            return False, None
        if now_day != analysis.target_date:
            return False, None
        if now_local.hour < max(0, min(23, int(self.earliest_finalize_hour_local))):
            return False, None
        if float(traj.lock_confidence) < float(self.lock_confidence_threshold):
            return False, None
        if float(traj.prob_exceed_observed_high) > float(self.max_exceed_prob):
            return False, None
        return True, "lock_confident_high_set"

    def _build_top_brackets(self, analysis: MarketAnalysis) -> List[Dict[str, Any]]:
        if not analysis.model_probabilities or not analysis.brackets:
            return []
        by_ticker = {b.ticker: b for b in analysis.brackets}
        rows: List[Dict[str, Any]] = []
        for ticker, model_prob in analysis.model_probabilities.items():
            bracket = by_ticker.get(ticker)
            if bracket is None:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "subtitle": bracket.subtitle,
                    "model_prob": float(model_prob),
                    "market_prob": float(bracket.implied_prob),
                    "edge": float(model_prob) - float(bracket.implied_prob),
                }
            )
        rows.sort(key=lambda r: r["model_prob"], reverse=True)
        return rows[: max(1, int(self.top_brackets))]

    def _build_sources(self, analysis: MarketAnalysis) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for fc in analysis.forecasts:
            changed_at = analysis.source_last_changed_at.get(fc.source)
            delta = analysis.source_last_change_delta.get(fc.source)
            rows.append(
                {
                    "source": fc.source,
                    "forecast_temp_f": float(fc.forecast_temp_f),
                    "std_dev_f": float(fc.std_dev),
                    "fetched_at": _iso_or_none(fc.fetched_at),
                    "last_changed_at": _iso_or_none(changed_at),
                    "last_delta": float(delta) if delta is not None else None,
                }
            )
        return rows

    def _build_payload(self, analysis: MarketAnalysis, existing: dict) -> dict:
        obs = analysis.observation
        traj = analysis.trajectory_assessment
        peak = analysis.peak_prediction
        decision = analysis.decision_snapshot
        top_brackets = self._build_top_brackets(analysis)
        should_finalize, finalize_reason = self._should_finalize(analysis)
        was_finalized = bool(existing.get("finalized"))
        finalized = was_finalized or should_finalize

        finalized_at = existing.get("finalized_at")
        finalized_reason = existing.get("finalized_reason")
        if should_finalize and not was_finalized:
            finalized_at = analysis.analyzed_at.isoformat()
            finalized_reason = finalize_reason

        current_temp_f = None
        if obs is not None and obs.readings:
            current_temp_f = float(obs.readings[-1].reported_temp_f)

        previous_count = int(existing.get("update_count", 0) or 0)
        payload = {
            "schema_version": 1,
            "city_code": self.city_code.upper(),
            "target_date": analysis.target_date,
            "updated_at": analysis.analyzed_at.isoformat(),
            "update_count": previous_count + 1,
            "finalized": finalized,
            "finalized_at": finalized_at,
            "finalized_reason": finalized_reason,
            "summary": {
                "raw_forecast_mean_f": float(analysis.raw_forecast_mean) if analysis.raw_forecast_mean is not None else None,
                "final_model_mean_f": float(analysis.adjusted_forecast_mean or analysis.forecast_mean),
                "final_model_std_f": float(analysis.adjusted_forecast_std or analysis.forecast_std),
                "forecast_only_mean_f": float(analysis.raw_forecast_mean) if analysis.raw_forecast_mean is not None else None,
                "tomorrow_prediction_date": analysis.tomorrow_date,
                "tomorrow_prediction_mean_f": float(analysis.tomorrow_forecast_mean) if analysis.tomorrow_forecast_mean is not None else None,
            },
            "sources": self._build_sources(analysis),
            "observation": {
                "station_id": obs.station_id if obs is not None else None,
                "observed_high_f": float(obs.observed_high_f) if obs is not None else None,
                "reported_series_high_f": float(obs.reported_series_high_f) if (obs is not None and obs.reported_series_high_f is not None) else None,
                "reported_max_6h_f": float(obs.reported_max_6h_f) if (obs is not None and obs.reported_max_6h_f is not None) else None,
                "possible_actual_high_low_f": float(obs.possible_actual_high_low) if obs is not None else None,
                "possible_actual_high_high_f": float(obs.possible_actual_high_high) if obs is not None else None,
                "current_temp_f": current_temp_f,
                "reading_count": len(obs.readings) if obs is not None else 0,
            },
            "trajectory": {
                "prob_high_already_set": float(traj.prob_high_already_set) if traj is not None else None,
                "prob_exceed_observed_high": float(traj.prob_exceed_observed_high) if traj is not None else None,
                "expected_remaining_warming_f": float(traj.expected_remaining_warming_f) if traj is not None else None,
                "lock_confidence": float(traj.lock_confidence) if traj is not None else None,
                "trend_f_per_hour": float(traj.trend_f_per_hour) if traj is not None else None,
                "reasoning": traj.reasoning if traj is not None else None,
            },
            "peak_nowcast": {
                "predicted_high_f": float(peak.predicted_high_f) if peak is not None else None,
                "confidence": float(peak.confidence) if peak is not None else None,
                "max_feasible_temp_f": float(peak.max_feasible_temp_f) if (peak is not None and peak.max_feasible_temp_f is not None) else None,
                "trend_f_per_hour": float(peak.trend_f_per_hour) if peak is not None else None,
                "expected_remaining_warming_f": float(peak.expected_remaining_warming_f) if peak is not None else None,
                "reasoning": peak.reasoning if peak is not None else None,
            },
            "decision": {
                "status": decision.status if decision is not None else None,
                "reason": decision.reason if decision is not None else None,
                "ticker": decision.ticker if decision is not None else None,
                "bracket_subtitle": decision.bracket_subtitle if decision is not None else None,
                "model_prob": float(decision.model_prob) if (decision is not None and decision.model_prob is not None) else None,
                "market_prob": float(decision.market_prob) if (decision is not None and decision.market_prob is not None) else None,
                "edge": float(decision.edge) if (decision is not None and decision.edge is not None) else None,
                "confidence": float(decision.confidence) if (decision is not None and decision.confidence is not None) else None,
            },
            "top_brackets": top_brackets,
            "signals_top5": [
                {
                    "ticker": sig.bracket.ticker,
                    "subtitle": sig.bracket.subtitle,
                    "direction": sig.direction,
                    "model_prob": float(sig.model_prob),
                    "market_prob": float(sig.market_prob),
                    "edge": float(sig.edge),
                    "confidence": float(sig.confidence),
                    "reasoning": sig.reasoning,
                }
                for sig in analysis.signals[:5]
            ],
            "open_positions": [
                {
                    "ticker": p.ticker,
                    "side": p.side,
                    "contracts": int(p.contracts),
                    "average_entry_price_cents": p.average_entry_price_cents,
                }
                for p in analysis.account_open_positions
            ],
            "auto_events_top10": analysis.auto_trader_events[:10],
        }
        return payload

    def append_snapshot(self, analysis: MarketAnalysis) -> Path:
        """
        Update the per-day JSON snapshot and freeze it once finalized.
        """
        output_path = self._resolve_file_path(analysis.target_date)
        existing = self._read_existing(output_path)
        if bool(existing.get("finalized")):
            return output_path

        payload = self._build_payload(analysis, existing)
        self._write_payload(output_path, payload)
        logger.debug("Wrote daily LLM log snapshot to %s", output_path)
        return output_path
