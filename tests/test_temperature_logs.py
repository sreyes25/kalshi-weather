from datetime import datetime
from pathlib import Path

from kalshi_weather.core.models import DailyObservation, MarketAnalysis
from kalshi_weather.utils.temperature_logs import DailyTemperatureLogger


def _build_analysis(analyzed_at: datetime, observed_high: float) -> MarketAnalysis:
    observation = DailyObservation(
        station_id="KNYC",
        date="2026-03-21",
        observed_high_f=observed_high,
        possible_actual_high_low=observed_high - 0.3,
        possible_actual_high_high=observed_high + 0.3,
    )

    return MarketAnalysis(
        city="New York City",
        target_date="2026-03-21",
        forecasts=[],
        observation=observation,
        brackets=[],
        signals=[],
        forecast_mean=62.0,
        forecast_std=2.5,
        analyzed_at=analyzed_at,
        raw_forecast_mean=61.2,
        raw_forecast_std=2.7,
        adjusted_forecast_mean=62.4,
        adjusted_forecast_std=2.2,
    )


def test_daily_temperature_logger_creates_and_appends(tmp_path: Path):
    logger = DailyTemperatureLogger(city_code="NYC", base_dir=tmp_path)
    first = _build_analysis(datetime(2026, 3, 21, 10, 0, 0), observed_high=60.0)
    second = _build_analysis(datetime(2026, 3, 21, 10, 5, 0), observed_high=60.4)

    first_path = logger.append_snapshot(first)
    second_path = logger.append_snapshot(second)

    assert first_path == second_path
    assert first_path.name == "2026-03-21.csv"
    assert first_path.parent.name == "NYC"

    lines = first_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    assert lines[0].startswith("timestamp,city,target_date,raw_forecast_mean,final_model_mean")
    assert "2026-03-21T10:00:00" in lines[1]
    assert "2026-03-21T10:05:00" in lines[2]
    assert "60.000" in lines[1]
    assert "60.400" in lines[2]
