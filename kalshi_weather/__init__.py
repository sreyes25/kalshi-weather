"""
Kalshi Weather Bot - Weather forecast analysis for Kalshi temperature markets.

A tool for fetching weather forecasts, parsing NWS observations, and analyzing
Kalshi temperature market brackets to identify trading edges.
"""

__version__ = "0.1.0"

from kalshi_weather.core import (
    # Enums
    BracketType,
    StationType,
    ContractType,
    # Data classes
    TemperatureForecast,
    StationReading,
    DailyObservation,
    MarketBracket,
    TradingSignal,
    MarketAnalysis,
)

from kalshi_weather.config import (
    CityConfig,
    NYC,
    get_city,
    list_cities,
)

from kalshi_weather.contracts import (
    HighTempContract,
)

__all__ = [
    # Version
    "__version__",
    # Enums
    "BracketType",
    "StationType",
    "ContractType",
    # Data classes
    "TemperatureForecast",
    "StationReading",
    "DailyObservation",
    "MarketBracket",
    "TradingSignal",
    "MarketAnalysis",
    # Config
    "CityConfig",
    "NYC",
    "get_city",
    "list_cities",
    # Contracts
    "HighTempContract",
]
