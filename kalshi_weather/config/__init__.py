"""Configuration module for Kalshi Weather Bot."""

from kalshi_weather.config.cities import (
    CityConfig,
    NYC,
    CITIES,
    DEFAULT_CITY,
    get_city,
    list_cities,
)

from kalshi_weather.config.settings import (
    # API Endpoints
    OPEN_METEO_FORECAST_URL,
    OPEN_METEO_GFS_URL,
    OPEN_METEO_ENSEMBLE_URL,
    NWS_API_BASE,
    NWS_STATIONS_URL,
    NWS_CLIMATE_URL,
    KALSHI_API_BASE,
    KALSHI_MARKETS_URL,
    # API Settings
    API_TIMEOUT,
    NWS_USER_AGENT,
    MAX_RETRIES,
    RETRY_DELAY,
    # Trading Parameters
    MIN_EDGE_THRESHOLD,
    MAX_EDGE_THRESHOLD,
    KALSHI_FEE_RATE,
    # Forecast Parameters
    MIN_STD_DEV,
    DEFAULT_STD_DEV,
    # Display Settings
    DEFAULT_REFRESH_INTERVAL,
    # Logging
    LOG_LEVEL,
    LOG_FORMAT,
)

__all__ = [
    # Cities
    "CityConfig",
    "NYC",
    "CITIES",
    "DEFAULT_CITY",
    "get_city",
    "list_cities",
    # API Endpoints
    "OPEN_METEO_FORECAST_URL",
    "OPEN_METEO_GFS_URL",
    "OPEN_METEO_ENSEMBLE_URL",
    "NWS_API_BASE",
    "NWS_STATIONS_URL",
    "NWS_CLIMATE_URL",
    "KALSHI_API_BASE",
    "KALSHI_MARKETS_URL",
    # API Settings
    "API_TIMEOUT",
    "NWS_USER_AGENT",
    "MAX_RETRIES",
    "RETRY_DELAY",
    # Trading Parameters
    "MIN_EDGE_THRESHOLD",
    "MAX_EDGE_THRESHOLD",
    "KALSHI_FEE_RATE",
    # Forecast Parameters
    "MIN_STD_DEV",
    "DEFAULT_STD_DEV",
    # Display Settings
    "DEFAULT_REFRESH_INTERVAL",
    # Logging
    "LOG_LEVEL",
    "LOG_FORMAT",
]
