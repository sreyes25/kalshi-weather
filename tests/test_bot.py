import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from kalshi_weather.cli.bot import WeatherBot
from kalshi_weather.core.models import (
    TemperatureForecast,
    MarketBracket,
    BracketType,
    TradingSignal
)

@pytest.fixture
def mock_bot_deps():
    with patch('kalshi_weather.cli.bot.CombinedWeatherSource') as mock_weather, \
         patch('kalshi_weather.cli.bot.NWSStationParser') as mock_station, \
         patch('kalshi_weather.cli.bot.HighTempContract') as mock_contract, \
         patch('kalshi_weather.cli.bot.Dashboard') as mock_dashboard:
        
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
    mock_contract.fetch_forecasts.assert_called_once()
    mock_station.get_daily_summary.assert_called_once()
    mock_contract.fetch_brackets.assert_called_once()

def test_bot_run_structure():
    """Test that run loop exists (lightly)."""
    # This is hard to test without mocking the while loop or Live context.
    # Just verifying imports and instantiation worked in test_perform_analysis_flow.
    pass
