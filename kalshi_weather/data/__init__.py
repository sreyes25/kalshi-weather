"""Data fetching modules for weather forecasts, observations, and markets."""

from kalshi_weather.data.weather import (
    OpenMeteoSource,
    NWSForecastSource,
    CombinedWeatherSource,
    fetch_all_forecasts,
)

from kalshi_weather.data.stations import (
    NWSStationParser,
    get_station_observations,
    get_daily_observation,
    get_recent_observation_history,
    celsius_to_fahrenheit,
    calculate_temp_bounds,
    determine_station_type,
)

from kalshi_weather.data.dsm import (
    DSMParser,
    get_dsm_observation,
)

from kalshi_weather.data.markets import (
    KalshiMarketClient,
    fetch_brackets_for_date,
    get_market_summary,
    parse_bracket_subtitle,
    calculate_implied_probability,
    format_date_for_ticker,
)
from kalshi_weather.data.markets_ws import (
    KalshiQuoteWebsocketClient,
    LiveQuoteUpdate,
)

from kalshi_weather.data.historical import (
    SettlementRecord,
    fetch_settlement,
    fetch_settlement_range,
    get_yesterday_settlement,
)

__all__ = [
    # Weather
    "OpenMeteoSource",
    "NWSForecastSource",
    "CombinedWeatherSource",
    "fetch_all_forecasts",
    # Stations
    "NWSStationParser",
    "get_station_observations",
    "get_daily_observation",
    "get_recent_observation_history",
    "celsius_to_fahrenheit",
    "calculate_temp_bounds",
    "determine_station_type",
    # DSM
    "DSMParser",
    "get_dsm_observation",
    # Markets
    "KalshiMarketClient",
    "fetch_brackets_for_date",
    "get_market_summary",
    "parse_bracket_subtitle",
    "calculate_implied_probability",
    "format_date_for_ticker",
    "KalshiQuoteWebsocketClient",
    "LiveQuoteUpdate",
    # Historical
    "SettlementRecord",
    "fetch_settlement",
    "fetch_settlement_range",
    "get_yesterday_settlement",
]
