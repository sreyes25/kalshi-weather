"""
City configurations for Kalshi Weather Bot.

Add new cities here to extend support.
"""

from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class CityConfig:
    """Configuration for a supported city."""
    name: str              # Full city name
    code: str              # Short code (e.g., "NYC")
    station_id: str        # NWS station ID (e.g., "KNYC")
    lat: float             # Latitude
    lon: float             # Longitude
    timezone: str          # Timezone string
    # Kalshi series tickers for different contract types
    high_temp_ticker: str = ""   # e.g., "KXHIGHNY"
    low_temp_ticker: str = ""    # e.g., "KXLOWNY"
    # NWS Weather Forecast Office for climate reports
    wfo: str = ""                # e.g., "okx" for NYC


# =============================================================================
# CITY DEFINITIONS
# =============================================================================

NYC = CityConfig(
    name="New York City",
    code="NYC",
    station_id="KNYC",
    lat=40.7829,
    lon=-73.9654,
    timezone="America/New_York",
    high_temp_ticker="KXHIGHNY",
    low_temp_ticker="KXLOWNY",
    wfo="okx",
)

# =============================================================================
# CITY REGISTRY
# =============================================================================

CITIES: Dict[str, CityConfig] = {
    "NYC": NYC,
}

DEFAULT_CITY = NYC


def get_city(code: str) -> CityConfig:
    """
    Get city configuration by code.

    Args:
        code: City code (e.g., "NYC")

    Returns:
        CityConfig for the requested city

    Raises:
        KeyError: If city code is not found
    """
    code = code.upper()
    if code not in CITIES:
        available = ", ".join(CITIES.keys())
        raise KeyError(f"City '{code}' not found. Available: {available}")
    return CITIES[code]


def list_cities() -> list[str]:
    """Return list of available city codes."""
    return list(CITIES.keys())
