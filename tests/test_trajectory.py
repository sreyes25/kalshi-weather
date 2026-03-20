from datetime import datetime
from zoneinfo import ZoneInfo

from kalshi_weather.core import (
    BracketType,
    DailyObservation,
    MarketBracket,
    StationReading,
    StationType,
)
from kalshi_weather.engine.probability import CombinedForecast, BracketProbabilityCalculator, adjust_forecast_with_observations
from kalshi_weather.engine.trajectory import TrajectoryEngine

NYC_TZ = ZoneInfo("America/New_York")


def make_combined_forecast(mean: float = 75.0, std_dev: float = 2.5) -> CombinedForecast:
    z_10 = 1.28155
    return CombinedForecast(
        target_date="2026-06-20",
        mean_temp_f=mean,
        std_dev=std_dev,
        low_f=mean - z_10 * std_dev,
        high_f=mean + z_10 * std_dev,
        source_count=2,
        sources_used=["NWS", "Open-Meteo Best Match"],
        weights_used={"NWS": 0.5, "Open-Meteo Best Match": 0.5},
    )


def make_reading(hour: int, minute: int, temp_f: float) -> StationReading:
    return StationReading(
        station_id="KNYC",
        timestamp=datetime(2026, 6, 20, hour, minute, tzinfo=NYC_TZ),
        station_type=StationType.FIVE_MINUTE,
        reported_temp_f=temp_f,
        reported_temp_c=None,
        possible_actual_f_low=temp_f - 0.3,
        possible_actual_f_high=temp_f + 0.3,
    )


def make_observation(observed_high: float, readings: list[StationReading]):
    return DailyObservation(
        station_id="KNYC",
        date="2026-06-20",
        observed_high_f=observed_high,
        possible_actual_high_low=observed_high - 0.5,
        possible_actual_high_high=observed_high + 0.5,
        readings=readings,
        last_updated=readings[-1].timestamp if readings else datetime(2026, 6, 20, 12, 0, tzinfo=NYC_TZ),
    )


def test_morning_rising_temp_has_low_lock_confidence():
    engine = TrajectoryEngine(NYC_TZ)
    readings = [
        make_reading(9, 0, 64.0),
        make_reading(9, 30, 65.1),
        make_reading(10, 0, 66.0),
    ]
    assessment = engine.assess(
        current_time=datetime(2026, 6, 20, 10, 5, tzinfo=NYC_TZ),
        observed_high_f=66.0,
        recent_readings=readings,
        combined_forecast=make_combined_forecast(mean=75.0, std_dev=3.0),
    )

    assert assessment.lock_confidence < 0.35
    assert assessment.prob_exceed_observed_high > 0.30


def test_3pm_near_high_and_rising_has_moderate_lock_confidence():
    engine = TrajectoryEngine(NYC_TZ)
    readings = [
        make_reading(14, 0, 74.0),
        make_reading(14, 30, 74.6),
        make_reading(15, 0, 75.0),
    ]
    assessment = engine.assess(
        current_time=datetime(2026, 6, 20, 15, 0, tzinfo=NYC_TZ),
        observed_high_f=75.0,
        recent_readings=readings,
        combined_forecast=make_combined_forecast(mean=76.0, std_dev=2.0),
    )

    assert 0.25 <= assessment.lock_confidence <= 0.70
    assert assessment.prob_exceed_observed_high >= 0.08


def test_6pm_below_high_and_falling_has_very_high_lock_confidence():
    engine = TrajectoryEngine(NYC_TZ)
    readings = [
        make_reading(17, 0, 76.0),
        make_reading(17, 30, 75.4),
        make_reading(18, 0, 74.9),
    ]
    assessment = engine.assess(
        current_time=datetime(2026, 6, 20, 18, 5, tzinfo=NYC_TZ),
        observed_high_f=77.2,
        recent_readings=readings,
        combined_forecast=make_combined_forecast(mean=77.5, std_dev=1.8),
    )

    assert assessment.lock_confidence >= 0.80
    assert assessment.prob_exceed_observed_high <= 0.08
    assert assessment.prob_high_already_set >= 0.85


def test_missing_observation_history_degrades_gracefully():
    engine = TrajectoryEngine(NYC_TZ)
    assessment = engine.assess(
        current_time=datetime(2026, 6, 20, 17, 0, tzinfo=NYC_TZ),
        observed_high_f=74.0,
        recent_readings=[],
        combined_forecast=make_combined_forecast(mean=75.0, std_dev=2.2),
    )

    assert assessment.trend_f_per_hour == 0.0
    assert 0.0 <= assessment.lock_confidence <= 1.0
    assert "Limited recent station history" in assessment.reasoning


def test_late_day_lock_reduces_upper_bracket_probability():
    readings = [
        make_reading(17, 10, 74.8),
        make_reading(17, 30, 74.1),
        make_reading(18, 0, 73.7),
    ]
    observation = make_observation(observed_high=75.6, readings=readings)
    adjusted = adjust_forecast_with_observations(
        combined_forecast=make_combined_forecast(mean=77.0, std_dev=2.8),
        observation=observation,
        timezone=NYC_TZ,
        current_time=datetime(2026, 6, 20, 18, 5, tzinfo=NYC_TZ),
    )

    assert adjusted.lock_mode_active
    assert adjusted.trajectory_assessment is not None

    high_bracket = MarketBracket(
        ticker="T-78-80",
        event_ticker="TEST",
        subtitle="78 to 80",
        bracket_type=BracketType.BETWEEN,
        lower_bound=78.0,
        upper_bound=80.0,
        yes_bid=10,
        yes_ask=12,
        last_price=11,
        volume=100,
        implied_prob=0.11,
    )
    high_bracket_prob = BracketProbabilityCalculator().calculate_from_adjusted_forecast(
        adjusted_forecast=adjusted,
        brackets=[high_bracket],
    )[0].model_prob

    assert high_bracket_prob < 0.12
