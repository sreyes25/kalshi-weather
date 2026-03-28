import pytest
import json
from unittest.mock import patch
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from kalshi_weather.cli.bot import (
    WeatherBot,
    _apply_tomorrow_midnight_carryover_floor,
    _evening_to_midnight_reference_temp_f,
    _project_midnight_carryover_floor_f,
)
from kalshi_weather.core.models import (
    TemperatureForecast,
    MarketBracket,
    BracketType,
    TradingSignal,
    DailyObservation,
    StationReading,
    StationType,
)

@pytest.fixture
def mock_bot_deps(tmp_path):
    state_path = tmp_path / "source_change_state_fixture.json"
    events_path = tmp_path / "source_change_events_fixture.jsonl"
    with patch('kalshi_weather.cli.bot.CombinedWeatherSource') as mock_weather, \
         patch('kalshi_weather.cli.bot.NWSStationParser') as mock_station, \
         patch('kalshi_weather.cli.bot.HighTempContract') as mock_contract, \
         patch('kalshi_weather.cli.bot.Dashboard') as mock_dashboard, \
         patch('kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH', str(state_path)), \
         patch('kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH', str(events_path)):
        
        # Setup mock returns
        bot = WeatherBot(city_code="NYC")
        
        # Mock weather source (not used directly if bot uses contract?) 
        # Wait, bot.py initializes: self.weather_source = OpenMeteoWeatherSource()
        # But performs analysis using: self.contract.fetch_forecasts(...) ?
        # In bot.py:
        # forecasts = self.contract.fetch_forecasts(target_date)
        # So it relies on contract wrapper.
        
        yield bot, mock_contract.return_value, mock_station.return_value

def test_perform_analysis_flow(mock_bot_deps):
    bot, mock_contract, mock_station = mock_bot_deps
    
    # Mock data
    mock_contract.fetch_forecasts.return_value = [
        TemperatureForecast("Test", "2024-01-01", 50.0, 48.0, 52.0, 1.0, datetime.now(), datetime.now())
    ]
    mock_contract.fetch_brackets.return_value = [
        MarketBracket("TICKER", "EVENT", "Subtitle", BracketType.BETWEEN, 49, 51, 10, 20, 15, 100, 0.15)
    ]
    mock_station.get_daily_summary.return_value = None
    
    # Run analysis
    analysis = bot.perform_analysis()
    
    # Assertions
    assert analysis.city == "New York City" # Default dummy config name
    assert len(analysis.forecasts) == 1
    assert len(analysis.brackets) == 1
    assert analysis.forecast_mean == 50.0
    
    # Verify calls
    assert mock_contract.fetch_forecasts.call_count >= 1
    mock_station.get_daily_summary.assert_called_once()
    mock_contract.fetch_brackets.assert_called_once()

def test_bot_run_structure():
    """Test that run loop exists (lightly)."""
    # This is hard to test without mocking the while loop or Live context.
    # Just verifying imports and instantiation worked in test_perform_analysis_flow.
    pass


def test_perform_analysis_resyncs_signal_probs_and_edges(mock_bot_deps):
    bot, mock_contract, mock_station = mock_bot_deps
    now = datetime.now()
    forecast = TemperatureForecast("Test", "2024-01-01", 50.0, 48.0, 52.0, 1.0, now, now)
    bracket = MarketBracket(
        "TICKER",
        "EVENT",
        "49° to 51°",
        BracketType.BETWEEN,
        49,
        51,
        10,
        20,
        15,
        100,
        0.15,
    )
    fake_signal = TradingSignal(
        bracket=bracket,
        direction="BUY",
        model_prob=0.99,
        market_prob=0.01,
        edge=0.98,
        confidence=0.99,
        reasoning="fake stale edge",
    )

    mock_contract.fetch_forecasts.return_value = [forecast]
    mock_contract.fetch_brackets.return_value = [bracket]
    mock_station.get_daily_summary.return_value = None
    bot.edge_detector.analyze = lambda **kwargs: [fake_signal]

    analysis = bot.perform_analysis()
    assert len(analysis.signals) == 1
    synced = analysis.signals[0]
    expected_model_prob = analysis.model_probabilities[bracket.ticker]
    assert synced.model_prob == pytest.approx(expected_model_prob, abs=1e-6)
    assert synced.market_prob == pytest.approx(bracket.implied_prob, abs=1e-6)
    assert synced.edge == pytest.approx(expected_model_prob - bracket.implied_prob, abs=1e-6)
    assert synced.edge != pytest.approx(fake_signal.edge, abs=1e-6)


def test_source_change_timestamps_persist_across_restarts(tmp_path):
    state_file = tmp_path / "source_change_state.json"
    events_file = tmp_path / "source_change_events.jsonl"
    market_tz = ZoneInfo("America/New_York")
    now1 = datetime(2026, 3, 26, 2, 56, 0, tzinfo=market_tz)
    now2 = datetime(2026, 3, 26, 3, 5, 0, tzinfo=market_tz)
    forecasts = [
        TemperatureForecast("NWS", "2026-03-26", 72.0, 70.0, 74.0, 2.5, now1, now1)
    ]

    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_file)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_file)):
        bot1 = WeatherBot(city_code="NYC")
        first_changed, first_delta = bot1._track_source_forecast_changes(
            forecasts=forecasts,
            target_date="2026-03-26",
            now_market=now1,
        )
        assert first_changed["NWS"] == now1
        assert first_delta["NWS"] is None

    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_file)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_file)):
        bot2 = WeatherBot(city_code="NYC")
        second_changed, second_delta = bot2._track_source_forecast_changes(
            forecasts=forecasts,
            target_date="2026-03-26",
            now_market=now2,
        )
        assert second_changed["NWS"] == now1
        assert second_delta["NWS"] is None


def test_source_change_delta_and_event_log_recorded(tmp_path):
    state_file = tmp_path / "source_change_state.json"
    events_file = tmp_path / "source_change_events.jsonl"
    market_tz = ZoneInfo("America/New_York")
    now1 = datetime(2026, 3, 26, 3, 0, 0, tzinfo=market_tz)
    now2 = datetime(2026, 3, 26, 3, 10, 0, tzinfo=market_tz)

    first_forecasts = [
        TemperatureForecast("NWS", "2026-03-26", 72.0, 70.0, 74.0, 2.5, now1, now1)
    ]
    second_forecasts = [
        TemperatureForecast("NWS", "2026-03-26", 73.3, 71.0, 75.0, 2.5, now2, now2)
    ]

    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_file)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_file)):
        bot = WeatherBot(city_code="NYC")
        bot._track_source_forecast_changes(
            forecasts=first_forecasts,
            target_date="2026-03-26",
            now_market=now1,
        )
        changed_at, deltas = bot._track_source_forecast_changes(
            forecasts=second_forecasts,
            target_date="2026-03-26",
            now_market=now2,
        )

    assert changed_at["NWS"] == now2
    assert deltas["NWS"] == pytest.approx(1.3)

    rows = [
        json.loads(line)
        for line in events_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(rows) >= 2
    last = rows[-1]
    assert last["source"] == "NWS"
    assert last["change_type"] == "update"
    assert last["delta"] == pytest.approx(1.3)
    assert last["previous_temp"] == pytest.approx(72.0)
    assert last["new_temp"] == pytest.approx(73.3)


def test_source_change_state_resets_on_new_day(tmp_path):
    state_file = tmp_path / "source_change_state.json"
    events_file = tmp_path / "source_change_events.jsonl"
    market_tz = ZoneInfo("America/New_York")
    today = datetime.now(market_tz).strftime("%Y-%m-%d")
    stale_day = (datetime.now(market_tz) - timedelta(days=1)).strftime("%Y-%m-%d")
    stale_payload = {
        "version": 1,
        "city_code": "NYC",
        "day_anchor": stale_day,
        "entries": [
            {
                "target_date": stale_day,
                "source": "NWS",
                "temp_rounded": 65.0,
                "changed_at": datetime.now(market_tz).isoformat(),
            }
        ],
    }
    state_file.write_text(json.dumps(stale_payload), encoding="utf-8")

    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_file)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_file)):
        bot = WeatherBot(city_code="NYC")
        assert bot._source_last_changed_at_by_key == {}
        assert bot._source_last_forecast_temp_by_key == {}
        assert bot._source_last_delta_by_key == {}

    saved_payload = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved_payload["day_anchor"] == today
    assert saved_payload["entries"] == []


def test_project_midnight_carryover_floor_late_day_cooling():
    tz = ZoneInfo("America/New_York")
    observation = DailyObservation(
        station_id="KNYC",
        date="2026-07-10",
        observed_high_f=84.0,
        possible_actual_high_low=83.0,
        possible_actual_high_high=85.0,
        readings=[
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 20, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=84.0,
                reported_temp_c=None,
                possible_actual_f_low=83.5,
                possible_actual_f_high=84.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 21, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=82.0,
                reported_temp_c=None,
                possible_actual_f_low=81.5,
                possible_actual_f_high=82.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 22, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=80.0,
                reported_temp_c=None,
                possible_actual_f_low=79.5,
                possible_actual_f_high=80.5,
            ),
        ],
        last_updated=datetime(2026, 7, 10, 22, 5, tzinfo=tz),
    )
    now_local = datetime(2026, 7, 10, 22, 5, tzinfo=tz)

    carryover_floor = _project_midnight_carryover_floor_f(
        observation=observation,
        tomorrow_date="2026-07-11",
        now_local=now_local,
    )
    assert carryover_floor == pytest.approx(76.0)

    adjusted = _apply_tomorrow_midnight_carryover_floor(
        tomorrow_mean_f=72.0,
        tomorrow_date="2026-07-11",
        now_local=now_local,
        observation=observation,
    )
    assert adjusted == pytest.approx(76.0)


def test_project_midnight_carryover_floor_not_applied_too_early():
    tz = ZoneInfo("America/New_York")
    observation = DailyObservation(
        station_id="KNYC",
        date="2026-07-10",
        observed_high_f=84.0,
        possible_actual_high_low=83.0,
        possible_actual_high_high=85.0,
        readings=[
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 14, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=84.0,
                reported_temp_c=None,
                possible_actual_f_low=83.5,
                possible_actual_f_high=84.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 15, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=83.0,
                reported_temp_c=None,
                possible_actual_f_low=82.5,
                possible_actual_f_high=83.5,
            ),
        ],
        last_updated=datetime(2026, 7, 10, 15, 0, tzinfo=tz),
    )
    now_local = datetime(2026, 7, 10, 15, 5, tzinfo=tz)

    carryover_floor = _project_midnight_carryover_floor_f(
        observation=observation,
        tomorrow_date="2026-07-11",
        now_local=now_local,
    )
    assert carryover_floor is None

    adjusted = _apply_tomorrow_midnight_carryover_floor(
        tomorrow_mean_f=72.0,
        tomorrow_date="2026-07-11",
        now_local=now_local,
        observation=observation,
    )
    assert adjusted == pytest.approx(72.0)


def test_night_high_prediction_only_triggers_when_tomorrow_below_evening_context():
    tz = ZoneInfo("America/New_York")
    observation = DailyObservation(
        station_id="KNYC",
        date="2026-07-10",
        observed_high_f=84.0,
        possible_actual_high_low=83.0,
        possible_actual_high_high=85.0,
        readings=[
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 20, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=84.0,
                reported_temp_c=None,
                possible_actual_f_low=83.5,
                possible_actual_f_high=84.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 21, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=82.0,
                reported_temp_c=None,
                possible_actual_f_low=81.5,
                possible_actual_f_high=82.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 22, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=80.0,
                reported_temp_c=None,
                possible_actual_f_low=79.5,
                possible_actual_f_high=80.5,
            ),
        ],
        last_updated=datetime(2026, 7, 10, 22, 5, tzinfo=tz),
    )
    now_local = datetime(2026, 7, 10, 22, 5, tzinfo=tz)

    evening_reference = _evening_to_midnight_reference_temp_f(
        observation=observation,
        tomorrow_date="2026-07-11",
        now_local=now_local,
    )
    assert evening_reference == pytest.approx(84.0)

    unchanged = _apply_tomorrow_midnight_carryover_floor(
        tomorrow_mean_f=85.0,
        tomorrow_date="2026-07-11",
        now_local=now_local,
        observation=observation,
    )
    assert unchanged == pytest.approx(85.0)


def test_night_high_prediction_not_applied_while_evening_still_warming():
    tz = ZoneInfo("America/New_York")
    observation = DailyObservation(
        station_id="KNYC",
        date="2026-07-10",
        observed_high_f=74.0,
        possible_actual_high_low=73.0,
        possible_actual_high_high=75.0,
        readings=[
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 19, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=70.0,
                reported_temp_c=None,
                possible_actual_f_low=69.5,
                possible_actual_f_high=70.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 20, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=72.0,
                reported_temp_c=None,
                possible_actual_f_low=71.5,
                possible_actual_f_high=72.5,
            ),
            StationReading(
                station_id="KNYC",
                timestamp=datetime(2026, 7, 10, 21, 0, tzinfo=tz),
                station_type=StationType.HOURLY,
                reported_temp_f=74.0,
                reported_temp_c=None,
                possible_actual_f_low=73.5,
                possible_actual_f_high=74.5,
            ),
        ],
        last_updated=datetime(2026, 7, 10, 21, 5, tzinfo=tz),
    )
    now_local = datetime(2026, 7, 10, 21, 5, tzinfo=tz)

    carryover_floor = _project_midnight_carryover_floor_f(
        observation=observation,
        tomorrow_date="2026-07-11",
        now_local=now_local,
    )
    assert carryover_floor is None

    unchanged = _apply_tomorrow_midnight_carryover_floor(
        tomorrow_mean_f=60.0,
        tomorrow_date="2026-07-11",
        now_local=now_local,
        observation=observation,
    )
    assert unchanged == pytest.approx(60.0)
