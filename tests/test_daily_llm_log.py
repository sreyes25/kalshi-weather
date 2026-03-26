import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from kalshi_weather.core.models import (
    BracketType,
    DailyObservation,
    MarketAnalysis,
    MarketBracket,
    PeakPrediction,
    StationReading,
    StationType,
    TemperatureForecast,
    TradeDecisionSnapshot,
    TradingSignal,
    TrajectoryAssessment,
)
from kalshi_weather.utils.daily_llm_log import DailyLLMLogWriter


NYC_TZ = ZoneInfo("America/New_York")


def _make_analysis(analyzed_at: datetime, target_date: str) -> MarketAnalysis:
    forecast = TemperatureForecast(
        source="NWS",
        target_date=target_date,
        forecast_temp_f=72.0,
        low_f=69.5,
        high_f=74.5,
        std_dev=2.5,
        model_run_time=None,
        fetched_at=analyzed_at,
    )
    reading = StationReading(
        station_id="KNYC",
        timestamp=analyzed_at,
        station_type=StationType.FIVE_MINUTE,
        reported_temp_f=58.0,
        reported_temp_c=None,
        possible_actual_f_low=57.6,
        possible_actual_f_high=58.4,
    )
    obs = DailyObservation(
        station_id="KNYC",
        date=target_date,
        observed_high_f=60.0,
        possible_actual_high_low=59.5,
        possible_actual_high_high=60.5,
        reported_series_high_f=60.0,
        reported_max_6h_f=60.0,
        readings=[reading],
        last_updated=analyzed_at,
    )
    bracket_a = MarketBracket(
        ticker="A",
        event_ticker="E",
        subtitle="71° to 72°",
        bracket_type=BracketType.BETWEEN,
        lower_bound=71.0,
        upper_bound=72.0,
        yes_bid=25,
        yes_ask=27,
        last_price=26,
        volume=100,
        implied_prob=0.26,
    )
    bracket_b = MarketBracket(
        ticker="B",
        event_ticker="E",
        subtitle="73° or above",
        bracket_type=BracketType.GREATER_THAN,
        lower_bound=72.0,
        upper_bound=None,
        yes_bid=35,
        yes_ask=37,
        last_price=36,
        volume=120,
        implied_prob=0.36,
    )
    signal = TradingSignal(
        bracket=bracket_b,
        direction="BUY",
        model_prob=0.41,
        market_prob=0.36,
        edge=0.05,
        confidence=0.72,
        reasoning="test",
    )
    traj = TrajectoryAssessment(
        prob_high_already_set=0.91,
        prob_exceed_observed_high=0.08,
        expected_remaining_warming_f=0.6,
        lock_confidence=0.93,
        trend_f_per_hour=-0.5,
        reasoning="lock confident",
    )
    peak = PeakPrediction(
        predicted_high_f=71.8,
        confidence=0.62,
        expected_remaining_warming_f=0.6,
        trend_f_per_hour=-0.5,
        max_feasible_temp_f=73.2,
        reasoning="test peak",
    )
    decision = TradeDecisionSnapshot(
        status="SKIP",
        reason="test decision",
        ticker=None,
        bracket_subtitle=None,
    )
    return MarketAnalysis(
        city="New York City",
        target_date=target_date,
        forecasts=[forecast],
        observation=obs,
        brackets=[bracket_a, bracket_b],
        signals=[signal],
        forecast_mean=71.3,
        forecast_std=2.9,
        analyzed_at=analyzed_at,
        raw_forecast_mean=74.1,
        raw_forecast_std=4.0,
        adjusted_forecast_mean=71.3,
        adjusted_forecast_std=2.9,
        source_last_changed_at={"NWS": analyzed_at},
        model_probabilities={"A": 0.27, "B": 0.41},
        account_open_positions=[],
        auto_trader_events=["no trade"],
        trajectory_assessment=traj,
        peak_prediction=peak,
        decision_snapshot=decision,
    )


def test_daily_llm_log_finalizes_and_freezes(tmp_path: Path):
    writer = DailyLLMLogWriter(
        city_code="NYC",
        timezone=NYC_TZ,
        base_dir=tmp_path,
        earliest_finalize_hour_local=16,
        lock_confidence_threshold=0.90,
        max_exceed_prob=0.10,
    )
    analysis = _make_analysis(
        analyzed_at=datetime(2026, 3, 26, 17, 10, tzinfo=NYC_TZ),
        target_date="2026-03-26",
    )
    path = writer.append_snapshot(analysis)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["finalized"] is True
    assert payload["finalized_reason"] == "lock_confident_high_set"
    assert payload["update_count"] == 1
    assert payload["top_brackets"][0]["ticker"] == "B"

    # Once finalized, subsequent writes should not mutate the snapshot.
    newer = replace(analysis, analyzed_at=datetime(2026, 3, 26, 17, 20, tzinfo=NYC_TZ), forecast_mean=75.0)
    writer.append_snapshot(newer)
    payload_after = json.loads(path.read_text(encoding="utf-8"))
    assert payload_after["update_count"] == 1
    assert payload_after["summary"]["final_model_mean_f"] == payload["summary"]["final_model_mean_f"]


def test_daily_llm_log_updates_intraday_until_finalize(tmp_path: Path):
    writer = DailyLLMLogWriter(
        city_code="NYC",
        timezone=NYC_TZ,
        base_dir=tmp_path,
        earliest_finalize_hour_local=16,
        lock_confidence_threshold=0.95,
        max_exceed_prob=0.05,
    )
    analysis = _make_analysis(
        analyzed_at=datetime(2026, 3, 26, 13, 0, tzinfo=NYC_TZ),
        target_date="2026-03-26",
    )
    # Make this snapshot non-finalizable.
    analysis.trajectory_assessment.lock_confidence = 0.70
    analysis.trajectory_assessment.prob_exceed_observed_high = 0.20

    path = writer.append_snapshot(analysis)
    payload1 = json.loads(path.read_text(encoding="utf-8"))
    assert payload1["finalized"] is False
    assert payload1["update_count"] == 1

    newer = replace(analysis, analyzed_at=datetime(2026, 3, 26, 13, 10, tzinfo=NYC_TZ))
    writer.append_snapshot(newer)
    payload2 = json.loads(path.read_text(encoding="utf-8"))
    assert payload2["finalized"] is False
    assert payload2["update_count"] == 2
