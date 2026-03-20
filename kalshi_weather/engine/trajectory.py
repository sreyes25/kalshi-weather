"""
Intraday trajectory and lock assessment engine.

Provides deterministic heuristics to estimate whether the daily high has
already occurred and how much additional warming is plausible.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Iterable, List, Optional, Protocol
from zoneinfo import ZoneInfo

from kalshi_weather.core import StationReading, TrajectoryAssessment
class _ForecastLike(Protocol):
    mean_temp_f: float
    std_dev: float


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _normal_cdf(x: float, mean: float, std_dev: float) -> float:
    if std_dev <= 0:
        return 1.0 if x >= mean else 0.0
    z = (x - mean) / (std_dev * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def _safe_trend_f_per_hour(readings: Iterable[StationReading]) -> float:
    series = sorted(readings, key=lambda r: r.timestamp)
    if len(series) < 2:
        return 0.0

    first = series[0]
    last = series[-1]
    hours = (last.timestamp - first.timestamp).total_seconds() / 3600.0
    if hours <= 0.0:
        return 0.0

    return (last.reported_temp_f - first.reported_temp_f) / hours


class TrajectoryEngine:
    """Heuristic intraday trajectory model for final-high advisory estimates."""

    def __init__(self, timezone: Optional[ZoneInfo] = None):
        self.timezone = timezone or ZoneInfo("America/New_York")

    def assess(
        self,
        current_time: datetime,
        observed_high_f: float,
        recent_readings: Optional[List[StationReading]],
        combined_forecast: _ForecastLike,
    ) -> TrajectoryAssessment:
        if current_time.tzinfo is None:
            now_local = current_time.replace(tzinfo=self.timezone)
        else:
            now_local = current_time.astimezone(self.timezone)

        readings = sorted((recent_readings or []), key=lambda r: r.timestamp)
        latest_temp = readings[-1].reported_temp_f if readings else observed_high_f
        trend_f_per_hour = _safe_trend_f_per_hour(readings)

        hour = now_local.hour + now_local.minute / 60.0
        if hour < 13.0:
            base_lock = 0.15
            time_upside_mult = 1.0
        elif hour < 16.0:
            base_lock = 0.45
            time_upside_mult = 0.75
        elif hour < 18.0:
            base_lock = 0.72
            time_upside_mult = 0.45
        else:
            base_lock = 0.86
            time_upside_mult = 0.20

        trend_lock_boost = 0.0
        trend_upside_mult = 1.0
        if trend_f_per_hour <= -0.6:
            trend_lock_boost = 0.22
            trend_upside_mult = 0.30
        elif trend_f_per_hour <= -0.2:
            trend_lock_boost = 0.14
            trend_upside_mult = 0.50
        elif trend_f_per_hour < 0.2:
            trend_lock_boost = 0.05
            trend_upside_mult = 0.75
        elif trend_f_per_hour >= 0.6:
            trend_lock_boost = -0.12
            trend_upside_mult = 1.40
        elif trend_f_per_hour >= 0.2:
            trend_lock_boost = -0.06
            trend_upside_mult = 1.20

        temp_gap = observed_high_f - latest_temp
        below_high = latest_temp < (observed_high_f - 0.15)

        gap_lock_boost = 0.0
        if below_high and trend_f_per_hour <= 0.0:
            gap_lock_boost += 0.16
            time_upside_mult *= 0.55
        elif below_high:
            gap_lock_boost += 0.06

        # Base upside from forecast tail strictly above observed high.
        tail_threshold = observed_high_f + 0.5
        base_upside = 1.0 - _normal_cdf(
            tail_threshold,
            combined_forecast.mean_temp_f,
            max(0.8, combined_forecast.std_dev),
        )

        prob_exceed = base_upside * time_upside_mult * trend_upside_mult

        near_high_and_rising = abs(latest_temp - observed_high_f) <= 0.8 and trend_f_per_hour > 0.15
        if near_high_and_rising:
            floor = 0.08 if hour < 18.0 else 0.04
            prob_exceed = max(prob_exceed, floor)

        prob_exceed = _clamp(prob_exceed, 0.001, 0.95)

        lock_confidence = _clamp(base_lock + trend_lock_boost + gap_lock_boost, 0.02, 0.99)
        prob_high_already_set = _clamp(
            0.52 * lock_confidence + 0.48 * (1.0 - prob_exceed),
            0.01,
            0.995,
        )

        positive_tail = max(0.0, combined_forecast.mean_temp_f + 0.6 * combined_forecast.std_dev - observed_high_f)
        mean_upside = max(0.0, combined_forecast.mean_temp_f - observed_high_f)
        expected_remaining_warming = prob_exceed * (0.6 * mean_upside + 0.4 * positive_tail)
        if trend_f_per_hour < 0:
            expected_remaining_warming *= 0.65
        elif trend_f_per_hour > 0.4:
            expected_remaining_warming *= 1.15
        expected_remaining_warming = _clamp(expected_remaining_warming, 0.0, 6.0)

        if below_high and trend_f_per_hour <= 0.0 and hour >= 16.0:
            reasoning = (
                "Observed high likely already set; temperature is below the high and "
                "flat-to-falling late in the day."
            )
        elif near_high_and_rising:
            reasoning = (
                "Current temperature is near the observed high and still rising, so "
                "some late upside remains plausible."
            )
        elif hour < 13.0:
            reasoning = "Morning regime: forecasts still dominate and lock confidence is low."
        else:
            reasoning = (
                "Intraday lock behavior reflects time-of-day and recent temperature trend "
                "relative to the observed high."
            )

        if not readings:
            reasoning += " Limited recent station history reduced trend certainty."

        return TrajectoryAssessment(
            prob_high_already_set=prob_high_already_set,
            prob_exceed_observed_high=prob_exceed,
            expected_remaining_warming_f=expected_remaining_warming,
            lock_confidence=lock_confidence,
            trend_f_per_hour=trend_f_per_hour,
            reasoning=reasoning,
        )
