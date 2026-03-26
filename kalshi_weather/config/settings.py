"""
Global settings and constants for Kalshi Weather Bot.

API endpoints, timeouts, and trading parameters.
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def _optional_positive_float_env(name: str, default: float) -> float | None:
    """
    Parse a float env var where non-positive values disable the feature.

    Returns None when the configured value is <= 0.
    """
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return float(default)
    if parsed <= 0.0:
        return None
    return parsed


def _float_env_alias(names: tuple[str, ...], default: float) -> float:
    """
    Parse the first present/valid float env var from a list of aliases.
    """
    for name in names:
        raw = os.getenv(name)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return float(default)


# =============================================================================
# API ENDPOINTS
# =============================================================================

# Open-Meteo APIs (free, no auth required)
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_GFS_URL = "https://api.open-meteo.com/v1/gfs"
OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"

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
NEARBY_STATION_IDS = [
    s.strip().upper() for s in os.getenv("NEARBY_STATION_IDS", "").split(",") if s.strip()
]
NEARBY_STATION_INFLUENCE = float(os.getenv("NEARBY_STATION_INFLUENCE", "0.55"))
NEARBY_STATION_MAX_DELTA_F = float(os.getenv("NEARBY_STATION_MAX_DELTA_F", "3.0"))
WIND_ADVECTION_NUDGE_ENABLED = os.getenv("WIND_ADVECTION_NUDGE_ENABLED", "true").lower() == "true"
WIND_ADVECTION_MIN_SPEED_MPH = float(os.getenv("WIND_ADVECTION_MIN_SPEED_MPH", "6.0"))
WIND_ADVECTION_ONSHORE_NUDGE_F = float(os.getenv("WIND_ADVECTION_ONSHORE_NUDGE_F", "-0.20"))
WIND_ADVECTION_OFFSHORE_NUDGE_F = float(os.getenv("WIND_ADVECTION_OFFSHORE_NUDGE_F", "0.15"))
WIND_ADVECTION_NUDGE_CAP_F = float(os.getenv("WIND_ADVECTION_NUDGE_CAP_F", "0.30"))
OPEN_METEO_EXTRA_MODELS = [
    s.strip() for s in os.getenv("OPEN_METEO_EXTRA_MODELS", "").split(",") if s.strip()
]
OPEN_METEO_EXTRA_MODEL_STD_DEV = float(os.getenv("OPEN_METEO_EXTRA_MODEL_STD_DEV", "2.3"))
REQUIRED_FORECAST_SOURCES = [
    s.strip()
    for s in os.getenv(
        "REQUIRED_FORECAST_SOURCES",
        "Open-Meteo Best Match,GFS+HRRR,NWS",
    ).split(",")
    if s.strip()
]
STRICT_FORECAST_SOURCE_VALIDATION = (
    os.getenv("STRICT_FORECAST_SOURCE_VALIDATION", "true").lower() == "true"
)
ACCUWEATHER_API_BASE = os.getenv("ACCUWEATHER_API_BASE", "https://dataservice.accuweather.com").strip()
ACCUWEATHER_API_KEY = os.getenv("ACCUWEATHER_API_KEY", "").strip()
ACCUWEATHER_LOCATION_KEY = os.getenv("ACCUWEATHER_LOCATION_KEY", "").strip()
ACCUWEATHER_ENABLED = os.getenv("ACCUWEATHER_ENABLED", "false").lower() == "true"
ACCUWEATHER_FORECAST_DAYS = int(os.getenv("ACCUWEATHER_FORECAST_DAYS", "5"))
ACCUWEATHER_LANGUAGE = os.getenv("ACCUWEATHER_LANGUAGE", "en-us").strip()
ACCUWEATHER_STD_DEV = float(os.getenv("ACCUWEATHER_STD_DEV", "2.2"))
ACCUWEATHER_WEIGHT = float(os.getenv("ACCUWEATHER_WEIGHT", "4.0"))


# =============================================================================
# TRADING PARAMETERS
# =============================================================================

# Keep thresholds alias-compatible with legacy AUTO_BUY_* naming to prevent drift.
MIN_CONFIDENCE_THRESHOLD = _float_env_alias(
    ("MIN_CONFIDENCE_THRESHOLD", "AUTO_BUY_MIN_CONFIDENCE"),
    0.60,
)
MIN_EDGE_THRESHOLD = _float_env_alias(("MIN_EDGE_THRESHOLD", "AUTO_BUY_MIN_EDGE"), 0.08)
MAX_EDGE_THRESHOLD = float(os.getenv("MAX_EDGE_THRESHOLD", "0.40"))  # 40%
MAX_RISK = float(os.getenv("MAX_RISK", "0.20"))  # 20% of capital
TARGET_PROFIT = float(os.getenv("TARGET_PROFIT", "0.15"))  # +15% take-profit target
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "5"))
PAPER_TRADING_START_BALANCE_DOLLARS = float(
    os.getenv("PAPER_TRADING_START_BALANCE_DOLLARS", "20.0")
)
KALSHI_FEE_RATE = 0.10  # 10% fee on winnings (fixed by Kalshi)
FRACTIONAL_KELLY_FRACTION = float(os.getenv("FRACTIONAL_KELLY_FRACTION", "0.50"))
KELLY_MAX_FRACTION_PER_TRADE = float(os.getenv("KELLY_MAX_FRACTION_PER_TRADE", str(MAX_RISK)))
KELLY_FEE_RATE = float(os.getenv("KELLY_FEE_RATE", str(KALSHI_FEE_RATE)))

# Timing / entry quality controls (percentage points)
TIMING_MIN_MODEL_TREND_PP = float(os.getenv("TIMING_MIN_MODEL_TREND_PP", "0.8"))
TIMING_MIN_MARKET_LAG_PP = float(os.getenv("TIMING_MIN_MARKET_LAG_PP", "0.6"))


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
EVENT_DRIVEN_SCHEDULER_ENABLED = os.getenv("EVENT_DRIVEN_SCHEDULER_ENABLED", "true").lower() == "true"
FORECAST_POLL_INTERVAL_SECONDS = int(os.getenv("FORECAST_POLL_INTERVAL_SECONDS", "300"))
METAR_POLL_INTERVAL_SECONDS = int(os.getenv("METAR_POLL_INTERVAL_SECONDS", "300"))
HRRR_REFRESH_MINUTE_LOCAL = int(os.getenv("HRRR_REFRESH_MINUTE_LOCAL", "15"))
CRITICAL_REEVAL_START_MINUTE = int(os.getenv("CRITICAL_REEVAL_START_MINUTE", "51"))
CRITICAL_REEVAL_END_MINUTE = int(os.getenv("CRITICAL_REEVAL_END_MINUTE", "54"))
CRITICAL_REEVAL_REFRESH_INTERVAL = int(os.getenv("CRITICAL_REEVAL_REFRESH_INTERVAL", "5"))
MARKET_TARGET_DAY_OFFSET = int(os.getenv("MARKET_TARGET_DAY_OFFSET", "0"))
MARKET_TARGET_STRICT = os.getenv("MARKET_TARGET_STRICT", "false").lower() == "true"
BOT_MANAGE_ONLY_TAGGED_POSITIONS = os.getenv("BOT_MANAGE_ONLY_TAGGED_POSITIONS", "true").lower() == "true"
BOT_CLIENT_TAG = os.getenv("BOT_CLIENT_TAG", "kwbot").strip()
TRADE_JOURNAL_DB_PATH = os.getenv("TRADE_JOURNAL_DB_PATH", "logs/trade_journal.sqlite").strip()
SOURCE_CHANGE_STATE_PATH = os.getenv("SOURCE_CHANGE_STATE_PATH", "logs/source_change_state.json").strip()
SOURCE_CHANGE_EVENTS_PATH = os.getenv("SOURCE_CHANGE_EVENTS_PATH", "logs/source_change_events.jsonl").strip()
LLM_DAILY_LOG_ENABLED = os.getenv("LLM_DAILY_LOG_ENABLED", "true").lower() == "true"
LLM_DAILY_LOG_DIR = os.getenv("LLM_DAILY_LOG_DIR", "logs/llm_daily").strip()
LLM_DAILY_LOG_EARLIEST_HOUR_LOCAL = int(os.getenv("LLM_DAILY_LOG_EARLIEST_HOUR_LOCAL", "16"))
LLM_DAILY_LOG_LOCK_CONFIDENCE_THRESHOLD = float(
    os.getenv("LLM_DAILY_LOG_LOCK_CONFIDENCE_THRESHOLD", "0.90")
)
LLM_DAILY_LOG_MAX_EXCEED_PROB = float(os.getenv("LLM_DAILY_LOG_MAX_EXCEED_PROB", "0.10"))
LLM_DAILY_LOG_TOP_BRACKETS = int(os.getenv("LLM_DAILY_LOG_TOP_BRACKETS", "8"))

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
AUTO_SELL_ALLOW_PRIMARY_SCALP_TARGETS = os.getenv("AUTO_SELL_ALLOW_PRIMARY_SCALP_TARGETS", "true").lower() == "true"
AUTO_SELL_PRIMARY_SCALP_MAX_HOLD_MINUTES = int(os.getenv("AUTO_SELL_PRIMARY_SCALP_MAX_HOLD_MINUTES", "60"))
AUTO_SELL_INCLUDE_EVENT_POSITIONS = os.getenv("AUTO_SELL_INCLUDE_EVENT_POSITIONS", "true").lower() == "true"
AUTO_PRIMARY_PROFIT_LOCK_WARN_PROB = float(os.getenv("AUTO_PRIMARY_PROFIT_LOCK_WARN_PROB", "0.97"))
AUTO_PRIMARY_PROFIT_LOCK_TRIGGER_PROB = float(os.getenv("AUTO_PRIMARY_PROFIT_LOCK_TRIGGER_PROB", "0.99"))
AUTO_PRIMARY_FINAL_WINDOW_CLOSE_HOUR_LOCAL = int(os.getenv("AUTO_PRIMARY_FINAL_WINDOW_CLOSE_HOUR_LOCAL", "19"))
AUTO_PRIMARY_FINAL_WINDOW_CLOSE_MINUTE_LOCAL = int(os.getenv("AUTO_PRIMARY_FINAL_WINDOW_CLOSE_MINUTE_LOCAL", "51"))
AUTO_PRIMARY_RISK_BUFFER_FLOOR = float(os.getenv("AUTO_PRIMARY_RISK_BUFFER_FLOOR", "0.8"))
AUTO_PRIMARY_EDGE_EXCEED_WARN_PROB = float(os.getenv("AUTO_PRIMARY_EDGE_EXCEED_WARN_PROB", "0.10"))
AUTO_PRIMARY_EDGE_EXCEED_TRIGGER_PROB = float(os.getenv("AUTO_PRIMARY_EDGE_EXCEED_TRIGGER_PROB", "0.20"))
AUTO_PRIMARY_RISK_PERSISTENCE_CYCLES = int(os.getenv("AUTO_PRIMARY_RISK_PERSISTENCE_CYCLES", "2"))

# Auto-buy / scalp controls
AUTO_BUY_ENABLED = os.getenv("AUTO_BUY_ENABLED", "false").lower() == "true"
AUTO_BUY_DRY_RUN = os.getenv("AUTO_BUY_DRY_RUN", "true").lower() == "true"
AUTO_BUY_MAX_BUDGET_DOLLARS = float(os.getenv("AUTO_BUY_MAX_BUDGET_DOLLARS", "10.0"))
AUTO_BUY_MIN_ORDER_DOLLARS = float(os.getenv("AUTO_BUY_MIN_ORDER_DOLLARS", "1.0"))
AUTO_BUY_MAX_ORDER_DOLLARS = float(os.getenv("AUTO_BUY_MAX_ORDER_DOLLARS", "7.0"))
AUTO_BUY_MAX_ACTIVE_POSITIONS = int(os.getenv("AUTO_BUY_MAX_ACTIVE_POSITIONS", "1"))
AUTO_BUY_MAX_CONTRACTS_PER_ORDER = int(os.getenv("AUTO_BUY_MAX_CONTRACTS_PER_ORDER", "2"))
AUTO_BUY_MIN_EDGE = _float_env_alias(("AUTO_BUY_MIN_EDGE", "MIN_EDGE_THRESHOLD"), MIN_EDGE_THRESHOLD)
AUTO_BUY_MIN_CONFIDENCE = _float_env_alias(
    ("AUTO_BUY_MIN_CONFIDENCE", "MIN_CONFIDENCE_THRESHOLD"),
    MIN_CONFIDENCE_THRESHOLD,
)
AUTO_BUY_MIN_ENTRY_PRICE_CENTS = int(os.getenv("AUTO_BUY_MIN_ENTRY_PRICE_CENTS", "8"))
AUTO_BUY_MAX_ENTRY_PRICE_CENTS = int(os.getenv("AUTO_BUY_MAX_ENTRY_PRICE_CENTS", "45"))
AUTO_BUY_MAX_SPREAD_CENTS = int(os.getenv("AUTO_BUY_MAX_SPREAD_CENTS", "7"))
AUTO_BUY_MIN_VOLUME = int(os.getenv("AUTO_BUY_MIN_VOLUME", "25"))
AUTO_BUY_DYNAMIC_EDGE_FLOOR_ENABLED = os.getenv(
    "AUTO_BUY_DYNAMIC_EDGE_FLOOR_ENABLED", "true"
).lower() == "true"
AUTO_BUY_DYNAMIC_EDGE_STD_BUMP_PER_F = float(
    os.getenv("AUTO_BUY_DYNAMIC_EDGE_STD_BUMP_PER_F", "0.005")
)
AUTO_BUY_DYNAMIC_EDGE_BOUNDARY_BUMP_MAX = float(
    os.getenv("AUTO_BUY_DYNAMIC_EDGE_BOUNDARY_BUMP_MAX", "0.010")
)
AUTO_BUY_DYNAMIC_EDGE_MAX_EXTRA = float(
    os.getenv("AUTO_BUY_DYNAMIC_EDGE_MAX_EXTRA", "0.020")
)
AUTO_BUY_TARGET_PROFIT_CENTS = int(os.getenv("AUTO_BUY_TARGET_PROFIT_CENTS", "4"))
AUTO_BUY_FEE_BUFFER_CENTS = int(os.getenv("AUTO_BUY_FEE_BUFFER_CENTS", "2"))
AUTO_BUY_MAKER_FIRST_ENABLED = os.getenv("AUTO_BUY_MAKER_FIRST_ENABLED", "true").lower() == "true"
AUTO_BUY_MAKER_PRICE_OFFSET_CENTS = int(os.getenv("AUTO_BUY_MAKER_PRICE_OFFSET_CENTS", "1"))
AUTO_BUY_AFTER_REPORT_ONLY = os.getenv("AUTO_BUY_AFTER_REPORT_ONLY", "true").lower() == "true"
AUTO_BUY_REPORT_MINUTE_LOCAL = int(os.getenv("AUTO_BUY_REPORT_MINUTE_LOCAL", "51"))
AUTO_BUY_REPORT_GATE_END_HOUR_LOCAL = int(os.getenv("AUTO_BUY_REPORT_GATE_END_HOUR_LOCAL", "19"))
AUTO_BUY_POST_REPORT_WINDOW_MINUTES = int(os.getenv("AUTO_BUY_POST_REPORT_WINDOW_MINUTES", "3"))
AUTO_BUY_POST_REPORT_REQUIRE_UPTREND = os.getenv("AUTO_BUY_POST_REPORT_REQUIRE_UPTREND", "true").lower() == "true"
AUTO_BUY_POST_REPORT_MIN_ASK_DELTA_CENTS = int(os.getenv("AUTO_BUY_POST_REPORT_MIN_ASK_DELTA_CENTS", "1"))
AUTO_BUY_ALLOW_OFF_PRIMARY_WHEN_NO_PRIMARY_EDGE = (
    os.getenv("AUTO_BUY_ALLOW_OFF_PRIMARY_WHEN_NO_PRIMARY_EDGE", "false").lower() == "true"
)
AUTO_BUY_SPECULATIVE_ENABLED = os.getenv("AUTO_BUY_SPECULATIVE_ENABLED", "true").lower() == "true"
AUTO_BUY_SPECULATIVE_WINDOW_MINUTES = int(os.getenv("AUTO_BUY_SPECULATIVE_WINDOW_MINUTES", "2"))
AUTO_BUY_SPEC_MIN_PROB_EXCEED = float(os.getenv("AUTO_BUY_SPEC_MIN_PROB_EXCEED", "0.20"))
AUTO_BUY_SPEC_MIN_TEMP_TREND_FPH = float(os.getenv("AUTO_BUY_SPEC_MIN_TEMP_TREND_FPH", "0.20"))
AUTO_BUY_SPEC_MIN_DEW_TREND_FPH = float(os.getenv("AUTO_BUY_SPEC_MIN_DEW_TREND_FPH", "0.10"))
AUTO_BUY_SPEC_MIN_NOWCAST_SCORE = float(os.getenv("AUTO_BUY_SPEC_MIN_NOWCAST_SCORE", "10.0"))
AUTO_BUY_SPEC_MIN_ORDER_DOLLARS = float(os.getenv("AUTO_BUY_SPEC_MIN_ORDER_DOLLARS", "3.0"))
AUTO_BUY_SPEC_MAX_ORDER_DOLLARS = float(os.getenv("AUTO_BUY_SPEC_MAX_ORDER_DOLLARS", "4.0"))
AUTO_EXIT_STOP_LOSS_DOLLARS = _optional_positive_float_env("AUTO_EXIT_STOP_LOSS_DOLLARS", 3.0)
AUTO_EXIT_TAKE_PROFIT_DOLLARS = _optional_positive_float_env("AUTO_EXIT_TAKE_PROFIT_DOLLARS", 3.0)
AUTO_EXIT_TARGET_PROFIT_FRACTION = float(
    os.getenv("AUTO_EXIT_TARGET_PROFIT_FRACTION", str(TARGET_PROFIT))
)
AUTO_EXIT_CONFIDENCE_DROP_PP = float(os.getenv("AUTO_EXIT_CONFIDENCE_DROP_PP", "8.0"))
AUTO_EXIT_MIN_MODEL_PROB = float(os.getenv("AUTO_EXIT_MIN_MODEL_PROB", "0.40"))

# Local emergency kill switch (file-based).
# Create file to trigger: `touch <KILL_SWITCH_FILE>`
# Remove file to clear: `rm <KILL_SWITCH_FILE>`
KILL_SWITCH_ENABLED = os.getenv("KILL_SWITCH_ENABLED", "true").lower() == "true"
KILL_SWITCH_FILE = os.getenv("KILL_SWITCH_FILE", "/tmp/kalshi_weather_bot.kill")
KILL_SWITCH_MODE = os.getenv("KILL_SWITCH_MODE", "halt").strip().lower()

# Remote kill switch via SMS (Twilio polling)
REMOTE_KILL_ENABLED = os.getenv("REMOTE_KILL_ENABLED", "false").lower() == "true"
REMOTE_KILL_PROVIDER = os.getenv("REMOTE_KILL_PROVIDER", "twilio").strip().lower()
REMOTE_KILL_POLL_SECONDS = int(os.getenv("REMOTE_KILL_POLL_SECONDS", "20"))
REMOTE_KILL_ALLOWED_FROM = os.getenv("REMOTE_KILL_ALLOWED_FROM", "").strip()
REMOTE_KILL_COMMAND_PREFIX = os.getenv("REMOTE_KILL_COMMAND_PREFIX", "KWBOT").strip()
REMOTE_KILL_PIN = os.getenv("REMOTE_KILL_PIN", "").strip()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_TO_NUMBER = os.getenv("TWILIO_TO_NUMBER", "").strip()
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "").strip()
WHATSAPP_CLOUD_API_VERSION = os.getenv("WHATSAPP_CLOUD_API_VERSION", "v21.0").strip()
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
WHATSAPP_TO_NUMBER = os.getenv("WHATSAPP_TO_NUMBER", "").strip()

# Outbound notification alerts (manual intervention prompts)
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"
ALERTS_PROVIDER = os.getenv("ALERTS_PROVIDER", "twilio").strip().lower()
ALERTS_MIN_INTERVAL_SECONDS = int(os.getenv("ALERTS_MIN_INTERVAL_SECONDS", "20"))
ALERT_TO_NUMBER = os.getenv("ALERT_TO_NUMBER", "").strip()
ALERT_POSITION_LOSS_FRACTION = float(os.getenv("ALERT_POSITION_LOSS_FRACTION", "0.35"))
ALERT_BRACKET_CHANGE_ENABLED = os.getenv("ALERT_BRACKET_CHANGE_ENABLED", "true").lower() == "true"
ALERT_LLM_PROMPT_ENABLED = os.getenv("ALERT_LLM_PROMPT_ENABLED", "true").lower() == "true"
ALERT_TOP_BRACKETS = int(os.getenv("ALERT_TOP_BRACKETS", "3"))


# =============================================================================
# LOGGING
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
