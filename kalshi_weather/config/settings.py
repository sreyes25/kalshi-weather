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
CRITICAL_REEVAL_START_MINUTE = int(os.getenv("CRITICAL_REEVAL_START_MINUTE", "51"))
CRITICAL_REEVAL_END_MINUTE = int(os.getenv("CRITICAL_REEVAL_END_MINUTE", "54"))
CRITICAL_REEVAL_REFRESH_INTERVAL = int(os.getenv("CRITICAL_REEVAL_REFRESH_INTERVAL", "5"))

# =============================================================================
# AUTO-TRADING SAFETY FLAGS
# =============================================================================

AUTO_SELL_ENABLED = os.getenv("AUTO_SELL_ENABLED", "false").lower() == "true"
AUTO_SELL_DRY_RUN = os.getenv("AUTO_SELL_DRY_RUN", "true").lower() == "true"
AUTO_SELL_ON_WRONG_POSITION = os.getenv("AUTO_SELL_ON_WRONG_POSITION", "true").lower() == "true"
AUTO_SELL_PLACE_TARGETS = os.getenv("AUTO_SELL_PLACE_TARGETS", "false").lower() == "true"
AUTO_SELL_MAX_CONTRACTS = int(os.getenv("AUTO_SELL_MAX_CONTRACTS", "250"))
POSITION_EXIT_FEE_RATE = float(os.getenv("POSITION_EXIT_FEE_RATE", "0.02"))
AUTO_SELL_MIN_PROFIT_CENTS = float(os.getenv("AUTO_SELL_MIN_PROFIT_CENTS", "1.0"))
AUTO_SELL_START_HOUR_LOCAL = int(os.getenv("AUTO_SELL_START_HOUR_LOCAL", "15"))
AUTO_SELL_START_MINUTE_LOCAL = int(os.getenv("AUTO_SELL_START_MINUTE_LOCAL", "0"))
AUTO_SELL_MIN_NON_PRIMARY_CYCLES = int(os.getenv("AUTO_SELL_MIN_NON_PRIMARY_CYCLES", "1"))
# Ambiguity buffer: require a clear model lead over this position before auto-sell.
# Helps avoid churn when mean is near a bracket boundary (e.g. 56.5 between adjacent bins).
AUTO_SELL_MIN_PRIMARY_GAP_PP = float(os.getenv("AUTO_SELL_MIN_PRIMARY_GAP_PP", "6.0"))
AUTO_SELL_FORCE_EXIT_HOUR_LOCAL = int(os.getenv("AUTO_SELL_FORCE_EXIT_HOUR_LOCAL", "16"))
AUTO_SELL_FORCE_EXIT_MINUTE_LOCAL = int(os.getenv("AUTO_SELL_FORCE_EXIT_MINUTE_LOCAL", "0"))
AUTO_SELL_MIN_HOLD_MINUTES = int(os.getenv("AUTO_SELL_MIN_HOLD_MINUTES", "30"))
AUTO_SELL_MAX_DRAWDOWN_FRACTION = float(os.getenv("AUTO_SELL_MAX_DRAWDOWN_FRACTION", "0.50"))


# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
