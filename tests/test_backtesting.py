from datetime import datetime
from pathlib import Path

from kalshi_weather.config import NYC
from kalshi_weather.data.historical import SettlementRecord
from kalshi_weather.engine.backtesting import BacktestingEngine


def test_backtest_handles_missing_logs(tmp_path: Path):
    engine = BacktestingEngine(city=NYC, logs_dir=tmp_path)
    summary = engine.run(days=5)
    assert summary.days_tested == 0
    assert summary.total_trades == 0
    assert summary.roi == 0.0
    assert summary.starting_balance_dollars == 20.0
    assert summary.ending_balance_dollars == 20.0


def test_backtest_runs_with_sample_log(tmp_path: Path, monkeypatch):
    city_dir = tmp_path / "NYC"
    city_dir.mkdir(parents=True, exist_ok=True)
    csv_path = city_dir / "2026-03-20.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,city,target_date,raw_forecast_mean,final_model_mean,observed_high_f,forecast_std,final_model_std",
                "2026-03-20T12:05:00,New York City,2026-03-20,56.0,56.4,54.0,2.6,2.3",
                "2026-03-20T13:10:00,New York City,2026-03-20,56.2,56.8,55.0,2.4,2.1",
                "2026-03-20T14:20:00,New York City,2026-03-20,56.4,57.1,56.0,2.3,2.0",
            ]
        ),
        encoding="utf-8",
    )

    def _fake_settlement(date: str, city, use_fallback: bool = True):
        return SettlementRecord(
            date=date,
            city_code=city.code,
            settlement_high_f=57.0,
            settlement_low_f=45.0,
            source="test",
            station_name="KNYC",
            fetched_at=datetime.now(),
        )

    monkeypatch.setattr("kalshi_weather.engine.backtesting.fetch_settlement", _fake_settlement)

    engine = BacktestingEngine(city=NYC, logs_dir=tmp_path)
    summary = engine.run(
        days=3,
        min_confidence_threshold=0.2,
        min_edge_threshold=0.01,
        starting_balance_dollars=20.0,
    )
    assert summary.days_tested == 1
    assert summary.day_results[0].date == "2026-03-20"
    assert summary.total_trades >= 0
    assert summary.starting_balance_dollars == 20.0
