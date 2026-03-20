"""
Global settings and constants for Kalshi Weather Bot.

API endpoints, timeouts, and trading parameters.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# =============================================================================
# API ENDPOINTS
# =============================================================================

# Open-Meteo APIs (free, no auth required)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_GFS_URL = "https://api.open-meteo.com/v1/gfs"
OPEN_METEO_ENSEMBLE_URL = "https://api.open-meteo.com/v1/ensemble"

# NWS APIs (free, no auth required)
NWS_API_BASE = "https://api.weather.gov"
NWS_STATIONS_URL = "https://api.weather.gov/stations/{station_id}/observations"
NWS_CLIMATE_URL = "https://www.weather.gov/wrh/climate"

# Kalshi API
KALSHI_API_BASE = os.getenv("KALSHI_API_BASE", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_MARKETS_URL = f"{KALSHI_API_BASE}/markets"


# =============================================================================
# API SETTINGS
# =============================================================================

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "10"))  # seconds
NWS_USER_AGENT = os.getenv(
    "NWS_USER_AGENT",
    "KalshiWeatherBot/1.0 (github.com/kalshi-weather-bot)"
)
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = float(os.getenv("RETRY_DELAY", "1.0"))  # seconds


# =============================================================================
# TRADING PARAMETERS
# =============================================================================

MIN_EDGE_THRESHOLD = float(os.getenv("MIN_EDGE_THRESHOLD", "0.08"))  # 8%
MAX_EDGE_THRESHOLD = float(os.getenv("MAX_EDGE_THRESHOLD", "0.40"))  # 40%
KALSHI_FEE_RATE = 0.10  # 10% fee on winnings (fixed by Kalshi)


# =============================================================================
# FORECAST PARAMETERS
# =============================================================================

# Minimum standard deviation floor (prevent overconfidence)
MIN_STD_DEV = float(os.getenv("MIN_STD_DEV", "1.5"))  # °F

# Default std dev when not provided by model
DEFAULT_STD_DEV = float(os.getenv("DEFAULT_STD_DEV", "2.5"))  # °F


# =============================================================================
# DISPLAY SETTINGS
# =============================================================================

DEFAULT_REFRESH_INTERVAL = int(os.getenv("DEFAULT_REFRESH_INTERVAL", "60"))  # seconds


# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
