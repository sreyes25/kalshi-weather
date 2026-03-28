"""
Main Bot Runner.

Orchestrates the data fetching, analysis, and display loop.
"""

import json
import time
import math
import logging
import re
import os
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from rich.live import Live

from kalshi_weather.config.settings import (
    MIN_CONFIDENCE_THRESHOLD,
    MIN_EDGE_THRESHOLD,
    MAX_RISK,
    TARGET_PROFIT,
    MAX_TRADES_PER_DAY,
    PAPER_TRADING_START_BALANCE_DOLLARS,
    FRACTIONAL_KELLY_FRACTION,
    KELLY_MAX_FRACTION_PER_TRADE,
    KELLY_FEE_RATE,
    TIMING_MIN_MODEL_TREND_PP,
    TIMING_MIN_MARKET_LAG_PP,
    DEFAULT_REFRESH_INTERVAL,
    EVENT_DRIVEN_SCHEDULER_ENABLED,
    FORECAST_POLL_INTERVAL_SECONDS,
    METAR_POLL_INTERVAL_SECONDS,
    HRRR_REFRESH_MINUTE_LOCAL,
    CRITICAL_REEVAL_START_MINUTE,
    CRITICAL_REEVAL_END_MINUTE,
    CRITICAL_REEVAL_REFRESH_INTERVAL,
    MARKET_TARGET_DAY_OFFSET,
    MARKET_TARGET_STRICT,
    BOT_MANAGE_ONLY_TAGGED_POSITIONS,
    BOT_CLIENT_TAG,
    TRADE_JOURNAL_DB_PATH,
    SOURCE_CHANGE_STATE_PATH,
    SOURCE_CHANGE_EVENTS_PATH,
    LLM_DAILY_LOG_ENABLED,
    LLM_DAILY_LOG_DIR,
    LLM_DAILY_LOG_EARLIEST_HOUR_LOCAL,
    LLM_DAILY_LOG_LOCK_CONFIDENCE_THRESHOLD,
    LLM_DAILY_LOG_MAX_EXCEED_PROB,
    LLM_DAILY_LOG_TOP_BRACKETS,
    POSITION_EXIT_FEE_RATE,
    AUTO_SELL_ENABLED,
    AUTO_SELL_DRY_RUN,
    AUTO_SELL_ON_WRONG_POSITION,
    AUTO_SELL_PLACE_TARGETS,
    AUTO_SELL_MAX_CONTRACTS,
    AUTO_SELL_MIN_PROFIT_CENTS,
    AUTO_SELL_START_HOUR_LOCAL,
    AUTO_SELL_START_MINUTE_LOCAL,
    AUTO_SELL_MIN_NON_PRIMARY_CYCLES,
    AUTO_SELL_MIN_PRIMARY_GAP_PP,
    AUTO_SELL_FORCE_EXIT_HOUR_LOCAL,
    AUTO_SELL_FORCE_EXIT_MINUTE_LOCAL,
    AUTO_SELL_MIN_HOLD_MINUTES,
    AUTO_SELL_MAX_DRAWDOWN_FRACTION,
    AUTO_SELL_ALLOW_PRIMARY_SCALP_TARGETS,
    AUTO_SELL_PRIMARY_SCALP_MAX_HOLD_MINUTES,
    AUTO_SELL_INCLUDE_EVENT_POSITIONS,
    AUTO_PRIMARY_PROFIT_LOCK_WARN_PROB,
    AUTO_PRIMARY_PROFIT_LOCK_TRIGGER_PROB,
    AUTO_PRIMARY_FINAL_WINDOW_CLOSE_HOUR_LOCAL,
    AUTO_PRIMARY_FINAL_WINDOW_CLOSE_MINUTE_LOCAL,
    AUTO_PRIMARY_RISK_BUFFER_FLOOR,
    AUTO_PRIMARY_EDGE_EXCEED_WARN_PROB,
    AUTO_PRIMARY_EDGE_EXCEED_TRIGGER_PROB,
    AUTO_PRIMARY_RISK_PERSISTENCE_CYCLES,
    AUTO_BUY_ENABLED,
    AUTO_BUY_DRY_RUN,
    AUTO_BUY_MAX_BUDGET_DOLLARS,
    AUTO_BUY_MIN_ORDER_DOLLARS,
    AUTO_BUY_MAX_ORDER_DOLLARS,
    AUTO_BUY_MAX_ACTIVE_POSITIONS,
    AUTO_BUY_MAX_CONTRACTS_PER_ORDER,
    AUTO_BUY_MIN_ENTRY_PRICE_CENTS,
    AUTO_BUY_MAX_ENTRY_PRICE_CENTS,
    AUTO_BUY_MAX_SPREAD_CENTS,
    AUTO_BUY_MIN_VOLUME,
    AUTO_BUY_DYNAMIC_EDGE_FLOOR_ENABLED,
    AUTO_BUY_DYNAMIC_EDGE_STD_BUMP_PER_F,
    AUTO_BUY_DYNAMIC_EDGE_BOUNDARY_BUMP_MAX,
    AUTO_BUY_DYNAMIC_EDGE_MAX_EXTRA,
    AUTO_BUY_TARGET_PROFIT_CENTS,
    AUTO_BUY_FEE_BUFFER_CENTS,
    AUTO_BUY_MAKER_FIRST_ENABLED,
    AUTO_BUY_MAKER_PRICE_OFFSET_CENTS,
    AUTO_BUY_AFTER_REPORT_ONLY,
    AUTO_BUY_REPORT_MINUTE_LOCAL,
    AUTO_BUY_REPORT_GATE_END_HOUR_LOCAL,
    AUTO_BUY_POST_REPORT_WINDOW_MINUTES,
    AUTO_BUY_POST_REPORT_REQUIRE_UPTREND,
    AUTO_BUY_POST_REPORT_MIN_ASK_DELTA_CENTS,
    AUTO_BUY_ALLOW_OFF_PRIMARY_WHEN_NO_PRIMARY_EDGE,
    AUTO_BUY_SPECULATIVE_ENABLED,
    AUTO_BUY_SPECULATIVE_WINDOW_MINUTES,
    AUTO_BUY_SPEC_MIN_PROB_EXCEED,
    AUTO_BUY_SPEC_MIN_TEMP_TREND_FPH,
    AUTO_BUY_SPEC_MIN_DEW_TREND_FPH,
    AUTO_BUY_SPEC_MIN_NOWCAST_SCORE,
    AUTO_BUY_SPEC_MIN_ORDER_DOLLARS,
    AUTO_BUY_SPEC_MAX_ORDER_DOLLARS,
    AUTO_EXIT_STOP_LOSS_DOLLARS,
    AUTO_EXIT_TAKE_PROFIT_DOLLARS,
    AUTO_EXIT_TARGET_PROFIT_FRACTION,
    AUTO_EXIT_CONFIDENCE_DROP_PP,
    AUTO_EXIT_MIN_MODEL_PROB,
    KILL_SWITCH_ENABLED,
    KILL_SWITCH_FILE,
    KILL_SWITCH_MODE,
    REMOTE_KILL_ENABLED,
    REMOTE_KILL_PROVIDER,
    REMOTE_KILL_POLL_SECONDS,
    REMOTE_KILL_ALLOWED_FROM,
    REMOTE_KILL_COMMAND_PREFIX,
    REMOTE_KILL_PIN,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_TO_NUMBER,
    TWILIO_FROM_NUMBER,
    WHATSAPP_CLOUD_API_VERSION,
    WHATSAPP_PHONE_NUMBER_ID,
    WHATSAPP_ACCESS_TOKEN,
    WHATSAPP_TO_NUMBER,
    ALERTS_ENABLED,
    ALERTS_PROVIDER,
    ALERTS_MIN_INTERVAL_SECONDS,
    ALERT_TO_NUMBER,
    ALERT_POSITION_LOSS_FRACTION,
    ALERT_BRACKET_CHANGE_ENABLED,
    ALERT_LLM_PROMPT_ENABLED,
    ALERT_TOP_BRACKETS,
    ALERT_SOURCE_CHANGE_ENABLED,
    ALERT_SOURCE_CHANGE_MIN_DELTA_F,
    ALERT_OBS_DIVERGENCE_ENABLED,
    ALERT_OBS_DIVERGENCE_TEMP_F,
    ALERT_OBS_DIVERGENCE_EXCEED_PROB,
    ALERT_OBS_DIVERGENCE_LOCK_CONFIDENCE,
    ALERT_HOURLY_UPDATE_ENABLED,
    ALERT_HOURLY_UPDATE_MINUTE_LOCAL,
    API_TIMEOUT,
    NEARBY_STATION_IDS,
    NEARBY_STATION_INFLUENCE,
    NEARBY_STATION_MAX_DELTA_F,
    REQUIRED_FORECAST_SOURCES,
    STRICT_FORECAST_SOURCE_VALIDATION,
)
from kalshi_weather.config import NYC
from kalshi_weather.contracts import HighTempContract
from kalshi_weather.core.models import BracketType, DailyObservation, MarketAnalysis, OpenPosition, PeakPrediction, TradeDecisionSnapshot
from kalshi_weather.data.weather import CombinedWeatherSource
from kalshi_weather.data.stations import NWSStationParser
# from kalshi_weather.data.markets import KalshiMarketSource # Assuming this exists or using Contract class
from kalshi_weather.engine.edge_detector import EdgeDetector
from kalshi_weather.engine.calibration import ForecastCalibrator
from kalshi_weather.engine.decision_engine import DecisionEngine
from kalshi_weather.engine.position_manager import evaluate_open_positions
from kalshi_weather.engine.auto_trader import execute_auto_sells
from kalshi_weather.cli.display import Dashboard
from kalshi_weather.utils.daily_llm_log import DailyLLMLogWriter
from kalshi_weather.utils.temperature_logs import DailyTemperatureLogger
from kalshi_weather.utils.trade_journal import TradeJournal

logger = logging.getLogger(__name__)


def _distance_to_losing_edge_f(bracket, observed_high_f: float | None) -> float | None:
    if observed_high_f is None or bracket is None:
        return None
    if bracket.bracket_type == BracketType.BETWEEN:
        return None if bracket.upper_bound is None else float(bracket.upper_bound) - float(observed_high_f)
    if bracket.bracket_type == BracketType.LESS_THAN:
        return None if bracket.upper_bound is None else float(bracket.upper_bound) - float(observed_high_f)
    if bracket.bracket_type == BracketType.GREATER_THAN:
        if bracket.lower_bound is None:
            return None
        if observed_high_f > float(bracket.lower_bound):
            return float("inf")
        return float(bracket.lower_bound) + 1.0 - float(observed_high_f)
    return None


def _normal_cdf(x: float, mean: float, std_dev: float) -> float:
    if std_dev <= 0:
        return 1.0 if x >= mean else 0.0
    z = (x - mean) / (std_dev * math.sqrt(2.0))
    return 0.5 * (1.0 + math.erf(z))


def _remaining_edge_exceed_prob(
    *,
    distance_to_edge_f: float | None,
    remaining_mean_f: float,
    remaining_std_f: float,
) -> float:
    if distance_to_edge_f is None:
        return 0.0
    if math.isinf(distance_to_edge_f):
        return 0.0
    if distance_to_edge_f <= 0.0:
        return 1.0
    return max(0.0, min(1.0, 1.0 - _normal_cdf(distance_to_edge_f, remaining_mean_f, max(0.3, remaining_std_f))))


def _build_client_order_id(prefix: str, ticker: str, side: str, price_cents: int, count: int) -> str:
    ticker_key = re.sub(r"[^A-Za-z0-9]", "", ticker)[-16:]
    return f"{prefix}-{ticker_key}-{side.lower()}-{price_cents}-{count}"[:64]


def _position_key(ticker: str, side: str) -> str:
    return f"{ticker}|{side.upper()}"


def _paper_mark_price_cents(position: OpenPosition) -> Optional[int]:
    side = str(position.side or "").upper()

    def _as_cents(value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        try:
            cents = int(value)
        except (TypeError, ValueError):
            return None
        return max(0, min(100, cents))

    if side == "YES":
        for candidate in (position.yes_bid, position.last_price, position.yes_ask):
            cents = _as_cents(candidate)
            if cents is not None:
                return cents
        return None
    if side == "NO":
        for candidate in (position.yes_ask, position.last_price, position.yes_bid):
            cents = _as_cents(candidate)
            if cents is not None:
                return max(0, min(100, 100 - cents))
        return None
    return _as_cents(position.last_price)


def _yes_bracket_still_viable(bracket, observed_high_f: float | None) -> bool:
    """
    Return False when a YES bracket is already impossible given observed high.
    """
    if observed_high_f is None:
        return True
    # Settlement brackets are integer-based; once we have seen 62, outcomes <=61 are dead.
    observed_floor = int(math.floor(observed_high_f))
    if bracket.bracket_type == BracketType.BETWEEN:
        if bracket.upper_bound is None:
            return True
        return observed_floor <= int(bracket.upper_bound)
    if bracket.bracket_type == BracketType.LESS_THAN:
        if bracket.upper_bound is None:
            return True
        # LESS_THAN wins only when final high is strictly below threshold.
        return observed_floor < int(bracket.upper_bound)
    if bracket.bracket_type == BracketType.GREATER_THAN:
        return True
    return True


def _buy_viability_high_f(observation) -> float | None:
    """
    Conservative high used for buy viability filtering.

    If the estimated actual-high ceiling is already above a bracket,
    treat that bracket as non-viable for new YES entries.
    """
    if observation is None:
        return None
    high = observation.observed_high_f
    if observation.possible_actual_high_high is not None:
        high = max(high, observation.possible_actual_high_high)
    return high


def _safe_linear_trend_per_hour(points: list[tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    if xs[-1] - xs[0] <= 0.0:
        return 0.0
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom <= 1e-9:
        return 0.0
    return sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom


def _nowcast_score(temp_trend: float, dew_trend: float, rh_trend: float) -> float:
    score = (12.0 * temp_trend) + (8.0 * dew_trend) + (0.6 * rh_trend)
    return max(-100.0, min(100.0, score))


def _observation_age_minutes(observation: DailyObservation | None, now_local: datetime) -> float | None:
    if observation is None or not observation.readings:
        return None
    latest = observation.readings[-1].timestamp
    if latest.tzinfo is None:
        latest_local = latest.replace(tzinfo=now_local.tzinfo)
    else:
        latest_local = latest.astimezone(now_local.tzinfo)
    return max(0.0, (now_local - latest_local).total_seconds() / 60.0)


def _project_midnight_carryover_floor_f(
    *,
    observation: DailyObservation | None,
    tomorrow_date: str | None,
    now_local: datetime,
) -> float | None:
    """
    Estimate a conservative midnight carryover floor for tomorrow's high.

    Late-day warmth can roll into just after midnight and become tomorrow's high.
    We project local midnight temperature from recent in-day trend, then use that
    as a floor candidate for tomorrow's modeled mean.
    """
    if observation is None or not observation.readings or not tomorrow_date:
        return None
    if now_local.tzinfo is None:
        return None

    try:
        obs_date = datetime.strptime(observation.date, "%Y-%m-%d").date()
        next_date = datetime.strptime(tomorrow_date, "%Y-%m-%d").date()
    except ValueError:
        return None

    if next_date != (obs_date + timedelta(days=1)):
        return None

    latest = max(observation.readings, key=lambda r: r.timestamp)
    latest_local = latest.timestamp.astimezone(now_local.tzinfo)
    if latest_local.date() != obs_date:
        return None
    # Night-high logic should only engage in the evening/night window.
    if latest_local.hour < 18:
        return None

    midnight_local = datetime.combine(next_date, datetime.min.time(), tzinfo=now_local.tzinfo)
    hours_to_midnight = (midnight_local - latest_local).total_seconds() / 3600.0
    # Only apply late in the day where midnight carryover is meaningful.
    if hours_to_midnight <= 0.0 or hours_to_midnight > 8.0:
        return None

    window_start = latest_local - timedelta(hours=3)
    trend_points: list[tuple[float, float]] = []
    for reading in observation.readings:
        ts_local = reading.timestamp.astimezone(now_local.tzinfo)
        if ts_local.date() != obs_date:
            continue
        if ts_local < window_start or ts_local > latest_local:
            continue
        x_hours = (ts_local - window_start).total_seconds() / 3600.0
        trend_points.append((x_hours, float(reading.reported_temp_f)))

    trend_f_per_hour = _safe_linear_trend_per_hour(trend_points)
    # This adjustment is specifically for a post-peak cooling regime.
    # If temperatures are still rising meaningfully, do not apply it.
    if trend_f_per_hour > 0.25:
        return None
    trend_f_per_hour = max(-8.0, min(0.0, trend_f_per_hour))
    latest_temp_f = float(latest.reported_temp_f)
    projected_midnight_f = latest_temp_f + trend_f_per_hour * hours_to_midnight

    # Cooling-only midnight projection; never exceed the latest observed temp.
    carryover_floor_f = min(latest_temp_f, projected_midnight_f)
    carryover_floor_f = max(latest_temp_f - 12.0, carryover_floor_f)
    return round(carryover_floor_f, 1)


def _evening_to_midnight_reference_temp_f(
    *,
    observation: DailyObservation | None,
    tomorrow_date: str | None,
    now_local: datetime,
) -> float | None:
    """
    Return a recent evening-to-midnight temperature reference for tomorrow checks.
    """
    if observation is None or not observation.readings or not tomorrow_date:
        return None
    if now_local.tzinfo is None:
        return None

    try:
        obs_date = datetime.strptime(observation.date, "%Y-%m-%d").date()
        next_date = datetime.strptime(tomorrow_date, "%Y-%m-%d").date()
    except ValueError:
        return None

    if next_date != (obs_date + timedelta(days=1)):
        return None

    latest = max(observation.readings, key=lambda r: r.timestamp)
    latest_local = latest.timestamp.astimezone(now_local.tzinfo)
    if latest_local.date() != obs_date:
        return None
    if latest_local.hour < 18:
        return None

    midnight_local = datetime.combine(next_date, datetime.min.time(), tzinfo=now_local.tzinfo)
    hours_to_midnight = (midnight_local - latest_local).total_seconds() / 3600.0
    if hours_to_midnight <= 0.0 or hours_to_midnight > 8.0:
        return None

    evening_candidates: list[float] = []
    for reading in observation.readings:
        ts_local = reading.timestamp.astimezone(now_local.tzinfo)
        if ts_local.date() != obs_date:
            continue
        if ts_local.hour < 18:
            continue
        if ts_local > latest_local:
            continue
        evening_candidates.append(float(reading.reported_temp_f))

    if not evening_candidates:
        return None
    return max(evening_candidates)


def _apply_tomorrow_midnight_carryover_floor(
    *,
    tomorrow_mean_f: float | None,
    tomorrow_date: str | None,
    now_local: datetime,
    observation: DailyObservation | None,
) -> float | None:
    """
    Raise tomorrow's forecast mean when late-day carryover implies a higher floor.
    """
    if tomorrow_mean_f is None:
        return _project_midnight_carryover_floor_f(
            observation=observation,
            tomorrow_date=tomorrow_date,
            now_local=now_local,
        )
    evening_reference_f = _evening_to_midnight_reference_temp_f(
        observation=observation,
        tomorrow_date=tomorrow_date,
        now_local=now_local,
    )
    if evening_reference_f is None:
        return float(tomorrow_mean_f)
    # Only trigger night-high prediction when tomorrow mean is below
    # observed evening-to-midnight temperature context.
    if float(tomorrow_mean_f) >= float(evening_reference_f):
        return float(tomorrow_mean_f)

    carryover_floor_f = _project_midnight_carryover_floor_f(
        observation=observation,
        tomorrow_date=tomorrow_date,
        now_local=now_local,
    )
    if carryover_floor_f is None:
        return float(tomorrow_mean_f)
    return max(float(tomorrow_mean_f), float(carryover_floor_f))


class WeatherBot:
    """
    Main bot controller.
    """

    def __init__(self, city_code: str = "NYC", refresh_interval: int = DEFAULT_REFRESH_INTERVAL):
        self.city_code = city_code
        self.refresh_interval = refresh_interval
        self.dashboard = Dashboard()
        self.edge_detector = EdgeDetector()
        self.decision_engine = DecisionEngine(
            min_confidence_threshold=MIN_CONFIDENCE_THRESHOLD,
            min_edge_threshold=MIN_EDGE_THRESHOLD,
            max_risk_fraction=MAX_RISK,
            target_profit_fraction=TARGET_PROFIT,
            max_trades_per_day=MAX_TRADES_PER_DAY,
            min_model_trend_pp=TIMING_MIN_MODEL_TREND_PP,
            min_market_lag_pp=TIMING_MIN_MARKET_LAG_PP,
            fractional_kelly_fraction=FRACTIONAL_KELLY_FRACTION,
            kelly_max_fraction_per_trade=KELLY_MAX_FRACTION_PER_TRADE,
            kelly_fee_rate=KELLY_FEE_RATE,
            dynamic_edge_floor_enabled=AUTO_BUY_DYNAMIC_EDGE_FLOOR_ENABLED,
            dynamic_edge_std_bump_per_f=AUTO_BUY_DYNAMIC_EDGE_STD_BUMP_PER_F,
            dynamic_edge_boundary_bump_max=AUTO_BUY_DYNAMIC_EDGE_BOUNDARY_BUMP_MAX,
            dynamic_edge_max_extra=AUTO_BUY_DYNAMIC_EDGE_MAX_EXTRA,
        )
        
        from kalshi_weather.config import get_city
        self.city_config = get_city(city_code)
        self.calibrator = ForecastCalibrator(city=self.city_config)
        self._market_tz = ZoneInfo(self.city_config.timezone)

        # Initialize Data Sources
        self.weather_source = CombinedWeatherSource(city=self.city_config)
        self.station_source = NWSStationParser(city=self.city_config)
        self.contract = HighTempContract(self.city_config) 
        self.temperature_logger = DailyTemperatureLogger(city_code=city_code)
        self._previous_model_probabilities: dict[str, float] = {}
        self._previous_open_position_qty: dict[str, int] = {}
        self._previous_all_open_position_qty: dict[str, int] = {}
        self._non_primary_streaks: dict[str, int] = {}
        self._position_first_seen_at: dict[str, datetime] = {}
        self._primary_flip_risk_streaks: dict[str, int] = {}
        self._kill_switch_seen_active = False
        self._last_remote_kill_poll: Optional[datetime] = None
        self._last_remote_kill_sid: Optional[str] = None
        self._remote_kill_warned = False
        self._previous_yes_ask_by_ticker: dict[str, int] = {}
        self._loss_alert_active_by_position_key: dict[str, bool] = {}
        self._source_change_alert_last_notified_at_by_key: dict[tuple[str, str], datetime] = {}
        self._obs_divergence_alert_active_by_target_date: dict[str, bool] = {}
        self._last_primary_ticker_by_target_date: dict[str, str] = {}
        self._last_hourly_update_key: Optional[str] = None
        self._last_notification_sent_at: Optional[datetime] = None
        self._notification_config_warned = False
        self._journal = TradeJournal(TRADE_JOURNAL_DB_PATH)
        self._paper_start_event_id = self._journal.latest_order_event_id()
        self._managed_position_keys = self._journal.active_managed_keys()
        self._event_driven_scheduler_enabled = EVENT_DRIVEN_SCHEDULER_ENABLED
        self._cached_forecasts_by_date: dict[str, list] = {}
        self._cached_observations_by_date: dict[str, Optional[DailyObservation]] = {}
        self._source_last_forecast_temp_by_key: dict[tuple[str, str], float] = {}
        self._source_last_changed_at_by_key: dict[tuple[str, str], datetime] = {}
        self._source_last_delta_by_key: dict[tuple[str, str], Optional[float]] = {}
        self._source_change_state_path = Path(SOURCE_CHANGE_STATE_PATH)
        self._source_change_events_path = Path(SOURCE_CHANGE_EVENTS_PATH)
        self._next_forecast_refresh_at: Optional[datetime] = None
        self._next_hrrr_refresh_at: Optional[datetime] = None
        self._next_metar_refresh_at: Optional[datetime] = None
        self._scheduler_day_anchor: Optional[str] = None
        # Runtime-only guard: only sell quantities that can be attributed to this
        # bot process fills, never arbitrary account quantity on the same ticker.
        self._managed_qty_limits: dict[str, int] = {}
        self._pending_buy_qty: dict[str, int] = {}
        self._managed_qty_bootstrapped = False
        self._last_bankroll_unlock_attempt_at: Optional[datetime] = None
        self._bankroll_fallback_warned = False
        self._daily_llm_log_writer: Optional[DailyLLMLogWriter] = None
        if LLM_DAILY_LOG_ENABLED:
            self._daily_llm_log_writer = DailyLLMLogWriter(
                city_code=self.city_config.code,
                timezone=self._market_tz,
                base_dir=Path(LLM_DAILY_LOG_DIR),
                earliest_finalize_hour_local=LLM_DAILY_LOG_EARLIEST_HOUR_LOCAL,
                lock_confidence_threshold=LLM_DAILY_LOG_LOCK_CONFIDENCE_THRESHOLD,
                max_exceed_prob=LLM_DAILY_LOG_MAX_EXCEED_PROB,
                top_brackets=LLM_DAILY_LOG_TOP_BRACKETS,
            )
        self._load_source_change_state()

    def run(self):
        """Start the main loop."""
        
        with Live(self.dashboard.layout, refresh_per_second=4, screen=True) as live:
            while True:
                try:
                    remote_event = self._poll_remote_kill_command()
                    if remote_event:
                        logger.warning(remote_event)
                    if self._kill_switch_active():
                        mode = self._kill_switch_mode()
                        if mode == "halt":
                            logger.warning(
                                "KILL SWITCH ACTIVE (%s) - stopping bot loop. Remove file to resume: %s",
                                mode,
                                KILL_SWITCH_FILE,
                            )
                            break
                        if not self._kill_switch_seen_active:
                            logger.warning(
                                "KILL SWITCH ACTIVE (%s) - auto-trading paused while dashboard keeps running.",
                                mode,
                            )
                    elif self._kill_switch_seen_active:
                        logger.warning("Kill switch cleared; auto-trading re-enabled.")
                    self._kill_switch_seen_active = self._kill_switch_active()

                    analysis = self.perform_analysis()
                    self.temperature_logger.append_snapshot(analysis)
                    if self._daily_llm_log_writer is not None:
                        try:
                            self._daily_llm_log_writer.append_snapshot(analysis)
                        except Exception as exc:
                            logger.warning("Daily LLM log update skipped: %s", exc)
                    self.dashboard.update(analysis)
                    try:
                        self._process_notifications(analysis)
                    except Exception as exc:
                        logger.warning("Notification step skipped: %s", exc)
                    
                    time.sleep(self._next_sleep_seconds())
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.exception("Error in main loop")
                    # TODO: Show error in dashboard footer
                    time.sleep(10) # Retry delay

    def _in_critical_reeval_window(self, now: datetime) -> bool:
        """Return True when current minute is in the hourly critical re-eval window."""
        return CRITICAL_REEVAL_START_MINUTE <= now.minute <= CRITICAL_REEVAL_END_MINUTE

    def _seconds_until_next_critical_window(self, now: datetime) -> int:
        """
        Seconds until next critical window start (minute 51 by default).
        """
        next_window = now.replace(
            minute=CRITICAL_REEVAL_START_MINUTE,
            second=0,
            microsecond=0,
        )
        if now >= next_window:
            next_window = next_window + timedelta(hours=1)
        return max(1, int((next_window - now).total_seconds()))

    def _next_sleep_seconds(self) -> int:
        """
        Dynamic cadence:
        - Inside critical window: refresh quickly.
        - Outside critical window: sleep until either normal refresh or next window.
        """
        now = datetime.now(self._market_tz)
        if self._in_critical_reeval_window(now):
            base_sleep = max(1, min(self.refresh_interval, CRITICAL_REEVAL_REFRESH_INTERVAL))
        else:
            base_sleep = max(1, min(self.refresh_interval, self._seconds_until_next_critical_window(now)))

        if not self._event_driven_scheduler_enabled:
            return base_sleep

        due_in: list[int] = []
        for next_at in (
            self._next_metar_refresh_at,
            self._next_forecast_refresh_at,
            self._next_hrrr_refresh_at,
        ):
            if next_at is None:
                continue
            seconds_left = int((next_at - now).total_seconds())
            due_in.append(max(1, seconds_left))
        if not due_in:
            return base_sleep
        return max(1, min(base_sleep, min(due_in)))

    def _next_hrrr_refresh_after(self, now_local: datetime) -> datetime:
        """
        Return next hourly model refresh time anchored to configured minute.
        """
        minute = max(0, min(59, int(HRRR_REFRESH_MINUTE_LOCAL)))
        candidate = now_local.replace(minute=minute, second=5, microsecond=0)
        if now_local >= candidate:
            candidate = candidate + timedelta(hours=1)
        return candidate

    def _load_source_change_state(self) -> None:
        """
        Restore per-source forecast state from disk for today's market day.
        """
        now_market_day = datetime.now(self._market_tz).strftime("%Y-%m-%d")
        self._scheduler_day_anchor = now_market_day
        path = self._source_change_state_path
        if not str(path):
            return
        if not path.exists():
            return

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Source-change state load failed (%s): %s", path, exc)
            return

        if not isinstance(payload, dict):
            return

        stored_city = str(payload.get("city_code", "")).strip().upper()
        if stored_city and stored_city != self.city_config.code.upper():
            return

        stored_day = str(payload.get("day_anchor", "")).strip()
        if stored_day != now_market_day:
            # Daily reset policy: start clean on each new market day.
            self._save_source_change_state(day_anchor=now_market_day)
            return

        loaded = 0
        for row in payload.get("entries", []):
            if not isinstance(row, dict):
                continue
            target_date = str(row.get("target_date", "")).strip()
            source = str(row.get("source", "")).strip()
            changed_raw = row.get("changed_at")
            temp_raw = row.get("temp_rounded")
            delta_raw = row.get("last_delta")
            if not target_date or not source or changed_raw is None or temp_raw is None:
                continue
            try:
                changed_at = datetime.fromisoformat(str(changed_raw))
                if changed_at.tzinfo is None:
                    changed_at = changed_at.replace(tzinfo=self._market_tz)
                changed_at = changed_at.astimezone(self._market_tz)
                temp_rounded = round(float(temp_raw), 1)
                delta_value = (
                    round(float(delta_raw), 1)
                    if delta_raw is not None
                    else None
                )
            except (TypeError, ValueError):
                continue
            key = (target_date, source)
            self._source_last_forecast_temp_by_key[key] = temp_rounded
            self._source_last_changed_at_by_key[key] = changed_at
            self._source_last_delta_by_key[key] = delta_value
            loaded += 1

        if loaded:
            logger.info("Loaded %d source-change timestamps from %s", loaded, path)

    def _save_source_change_state(self, *, day_anchor: Optional[str] = None) -> None:
        """
        Persist per-source forecast temperatures + last change metadata.
        """
        path = self._source_change_state_path
        if not str(path):
            return

        anchor = day_anchor or self._scheduler_day_anchor or datetime.now(self._market_tz).strftime("%Y-%m-%d")
        entries: list[dict[str, object]] = []
        for key in sorted(self._source_last_changed_at_by_key.keys()):
            changed_at = self._source_last_changed_at_by_key.get(key)
            temp_rounded = self._source_last_forecast_temp_by_key.get(key)
            if changed_at is None or temp_rounded is None:
                continue
            target_date, source = key
            if changed_at.tzinfo is None:
                changed_at = changed_at.replace(tzinfo=self._market_tz)
            entries.append(
                {
                    "target_date": target_date,
                    "source": source,
                    "temp_rounded": round(float(temp_rounded), 1),
                    "last_delta": self._source_last_delta_by_key.get(key),
                    "changed_at": changed_at.astimezone(self._market_tz).isoformat(),
                }
            )

        payload = {
            "version": 2,
            "city_code": self.city_config.code,
            "day_anchor": anchor,
            "entries": entries,
            "saved_at": datetime.now(self._market_tz).isoformat(),
        }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(path)
        except OSError as exc:
            logger.warning("Source-change state save failed (%s): %s", path, exc)

    def _append_source_change_event(
        self,
        *,
        target_date: str,
        source: str,
        previous_temp: Optional[float],
        new_temp: float,
        delta: Optional[float],
        changed_at: datetime,
        change_type: str,
    ) -> None:
        """Append one source-change event to JSONL audit log."""
        path = self._source_change_events_path
        if not str(path):
            return
        row = {
            "city_code": self.city_config.code.upper(),
            "target_date": target_date,
            "source": source,
            "change_type": change_type,
            "previous_temp": round(float(previous_temp), 1) if previous_temp is not None else None,
            "new_temp": round(float(new_temp), 1),
            "delta": round(float(delta), 1) if delta is not None else None,
            "changed_at": changed_at.astimezone(self._market_tz).isoformat(),
            "recorded_at": datetime.now(self._market_tz).isoformat(),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(row, sort_keys=True))
                fp.write("\n")
        except OSError as exc:
            logger.warning("Source-change events log append failed (%s): %s", path, exc)

    def _track_source_forecast_changes(
        self,
        *,
        forecasts,
        target_date: str,
        now_market: datetime,
    ) -> tuple[dict[str, datetime], dict[str, Optional[float]]]:
        """
        Track per-source forecast value changes and return:
        - latest change timestamps for the current target date
        - signed delta at last change for each source
        """
        source_last_changed_at: dict[str, datetime] = {}
        source_last_change_delta: dict[str, Optional[float]] = {}
        for forecast in forecasts:
            key = (target_date, forecast.source)
            temp_rounded = round(float(forecast.forecast_temp_f), 1)
            prev_temp = self._source_last_forecast_temp_by_key.get(key)
            if prev_temp is None or temp_rounded != prev_temp:
                self._source_last_changed_at_by_key[key] = now_market
                delta = None if prev_temp is None else round(float(temp_rounded) - float(prev_temp), 1)
                self._source_last_delta_by_key[key] = delta
                self._append_source_change_event(
                    target_date=target_date,
                    source=forecast.source,
                    previous_temp=prev_temp,
                    new_temp=temp_rounded,
                    delta=delta,
                    changed_at=now_market,
                    change_type="init" if prev_temp is None else "update",
                )
            self._source_last_forecast_temp_by_key[key] = temp_rounded

            changed_at = self._source_last_changed_at_by_key.get(key)
            if changed_at is not None:
                source_last_changed_at[forecast.source] = changed_at
            source_last_change_delta[forecast.source] = self._source_last_delta_by_key.get(key)
        self._save_source_change_state()
        return source_last_changed_at, source_last_change_delta

    def _kill_switch_mode(self) -> str:
        mode = (KILL_SWITCH_MODE or "halt").strip().lower()
        if self._kill_switch_active():
            try:
                with open(KILL_SWITCH_FILE, "r", encoding="utf-8") as fp:
                    file_mode = fp.read().strip().lower()
                if file_mode in {"halt", "pause"}:
                    mode = file_mode
            except OSError:
                pass
        if mode not in {"halt", "pause"}:
            return "halt"
        return mode

    def _kill_switch_active(self) -> bool:
        if not KILL_SWITCH_ENABLED:
            return False
        return os.path.exists(KILL_SWITCH_FILE)

    def _trading_paused_by_kill_switch(self) -> bool:
        return self._kill_switch_active() and self._kill_switch_mode() == "pause"

    def _normalize_phone(self, value: str) -> str:
        return "".join(ch for ch in value if ch.isdigit() or ch == "+")

    def _parse_remote_command(self, body: str) -> Optional[tuple[str, Optional[str]]]:
        # Returns (action, mode) where action in {"set", "clear"} and mode in {"halt", "pause", None}
        parts = [p for p in body.strip().split() if p]
        if not parts:
            return None
        prefix = (REMOTE_KILL_COMMAND_PREFIX or "KWBOT").strip().upper()
        if parts[0].upper() != prefix:
            return None
        idx = 1
        if REMOTE_KILL_PIN:
            if len(parts) <= idx or parts[idx] != REMOTE_KILL_PIN:
                return None
            idx += 1
        if len(parts) <= idx:
            return None
        cmd = parts[idx].upper()
        if cmd in {"KILL", "HALT", "STOP"}:
            return ("set", "halt")
        if cmd in {"PAUSE"}:
            return ("set", "pause")
        if cmd in {"RESUME", "CLEAR", "RUN", "UNPAUSE"}:
            return ("clear", None)
        return None

    def _apply_remote_command(self, action: str, mode: Optional[str]) -> str:
        if action == "clear":
            try:
                if os.path.exists(KILL_SWITCH_FILE):
                    os.remove(KILL_SWITCH_FILE)
                return "REMOTE KILL: cleared (bot resumed)"
            except OSError as exc:
                return f"REMOTE KILL: failed to clear kill switch ({exc})"
        target_mode = mode if mode in {"halt", "pause"} else "halt"
        try:
            with open(KILL_SWITCH_FILE, "w", encoding="utf-8") as fp:
                fp.write(target_mode)
            return f"REMOTE KILL: set {target_mode}"
        except OSError as exc:
            return f"REMOTE KILL: failed to set {target_mode} ({exc})"

    def _poll_remote_kill_command(self) -> Optional[str]:
        if not REMOTE_KILL_ENABLED:
            return None
        if REMOTE_KILL_PROVIDER != "twilio":
            if not self._remote_kill_warned:
                self._remote_kill_warned = True
                return "REMOTE KILL: unsupported provider (set REMOTE_KILL_PROVIDER=twilio)"
            return None
        if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_TO_NUMBER and REMOTE_KILL_ALLOWED_FROM):
            if not self._remote_kill_warned:
                self._remote_kill_warned = True
                return "REMOTE KILL: missing Twilio env config; remote kill disabled"
            return None
        now = datetime.now(self._market_tz)
        if self._last_remote_kill_poll is not None:
            elapsed = (now - self._last_remote_kill_poll).total_seconds()
            if elapsed < max(5, REMOTE_KILL_POLL_SECONDS):
                return None
        self._last_remote_kill_poll = now
        try:
            url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
            response = requests.get(
                url,
                params={"To": TWILIO_TO_NUMBER, "PageSize": 8},
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            messages = payload.get("messages", [])
            allowed_from = self._normalize_phone(REMOTE_KILL_ALLOWED_FROM)
            for msg in messages:
                if msg.get("direction") != "inbound":
                    continue
                sid = msg.get("sid")
                if sid and sid == self._last_remote_kill_sid:
                    break
                from_number = self._normalize_phone(str(msg.get("from", "")))
                if from_number != allowed_from:
                    continue
                parsed = self._parse_remote_command(str(msg.get("body", "")))
                if parsed is None:
                    continue
                if sid:
                    self._last_remote_kill_sid = sid
                action, mode = parsed
                return self._apply_remote_command(action, mode)
        except requests.exceptions.RequestException as exc:
            logger.warning("REMOTE KILL poll failed: %s", exc)
        except (TypeError, ValueError) as exc:
            logger.warning("REMOTE KILL parse failed: %s", exc)
        return None

    def _alert_to_number(self) -> str:
        """
        Resolve destination number for outbound alerts.

        Order:
        - ALERT_TO_NUMBER (global override)
        - WHATSAPP_TO_NUMBER (provider-specific)
        - REMOTE_KILL_ALLOWED_FROM (legacy fallback)
        """
        return (ALERT_TO_NUMBER or WHATSAPP_TO_NUMBER or REMOTE_KILL_ALLOWED_FROM or "").strip()

    def _twilio_numbers(self) -> tuple[str, str]:
        """
        Return `(from_number, to_number)` for Twilio outbound alerts.
        """
        from_number = (TWILIO_FROM_NUMBER or TWILIO_TO_NUMBER or "").strip()
        to_number = self._alert_to_number()
        return from_number, to_number

    def _digits_only_phone(self, value: str) -> str:
        return "".join(ch for ch in str(value or "") if ch.isdigit())

    def _notifications_config_ready(self) -> bool:
        if not ALERTS_ENABLED:
            return False
        provider = ALERTS_PROVIDER.strip().lower()
        if provider == "twilio":
            from_number, to_number = self._twilio_numbers()
            if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and from_number and to_number:
                return True
            if not self._notification_config_warned:
                self._notification_config_warned = True
                logger.warning(
                    "Notifications disabled: missing Twilio config (sid/auth/from/to). "
                    "Set ALERT_TO_NUMBER and TWILIO_FROM_NUMBER (or rely on REMOTE_KILL_ALLOWED_FROM/TWILIO_TO_NUMBER)."
                )
            return False
        if provider in {"whatsapp", "whatsapp_cloud", "meta_whatsapp"}:
            to_number = self._digits_only_phone(self._alert_to_number())
            if WHATSAPP_PHONE_NUMBER_ID and WHATSAPP_ACCESS_TOKEN and to_number:
                return True
            if not self._notification_config_warned:
                self._notification_config_warned = True
                logger.warning(
                    "Notifications disabled: missing WhatsApp Cloud config "
                    "(WHATSAPP_PHONE_NUMBER_ID / WHATSAPP_ACCESS_TOKEN / ALERT_TO_NUMBER or WHATSAPP_TO_NUMBER)."
                )
            return False
        if not self._notification_config_warned:
            self._notification_config_warned = True
            logger.warning(
                "Notifications disabled: unsupported provider '%s' "
                "(supported: twilio, whatsapp_cloud)",
                ALERTS_PROVIDER,
            )
        return False

    def _send_alert_message(self, body: str, *, force: bool = False) -> bool:
        if not self._notifications_config_ready():
            return False
        text = str(body or "").strip()
        if not text:
            return False
        now = datetime.now(self._market_tz)
        if not force and self._last_notification_sent_at is not None:
            elapsed = (now - self._last_notification_sent_at).total_seconds()
            if elapsed < max(0, int(ALERTS_MIN_INTERVAL_SECONDS)):
                return False
        provider = ALERTS_PROVIDER.strip().lower()
        ok = False
        if provider == "twilio":
            ok = self._send_twilio_alert(text)
        elif provider in {"whatsapp", "whatsapp_cloud", "meta_whatsapp"}:
            ok = self._send_whatsapp_cloud_alert(text)
        if ok:
            self._last_notification_sent_at = now
        return ok

    def _send_twilio_alert(self, text: str) -> bool:
        from_number, to_number = self._twilio_numbers()
        try:
            url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
            response = requests.post(
                url,
                data={
                    "From": from_number,
                    "To": to_number,
                    "Body": text[:3000],
                },
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as exc:
            logger.warning("Notification send failed: %s", exc)
            return False

    def _send_whatsapp_cloud_alert(self, text: str) -> bool:
        to_number = self._digits_only_phone(self._alert_to_number())
        body = text[:3800]
        api_version = (WHATSAPP_CLOUD_API_VERSION or "v21.0").strip()
        url = f"https://graph.facebook.com/{api_version}/{WHATSAPP_PHONE_NUMBER_ID}/messages"
        try:
            response = requests.post(
                url,
                json={
                    "messaging_product": "whatsapp",
                    "to": to_number,
                    "type": "text",
                    "text": {"preview_url": False, "body": body},
                },
                headers={
                    "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
                    "Content-Type": "application/json",
                },
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as exc:
            logger.warning("WhatsApp Cloud notification failed: %s", exc)
            return False

    def _build_drawdown_alert_message(
        self,
        *,
        threshold: float,
        triggered_rows: list[tuple[str, str, int, float, float, float, float, str]],
    ) -> str:
        lines = [
            (
                f"KWBOT ALERT: position loss >= {threshold * 100:.0f}% "
                f"({datetime.now(self._market_tz).strftime('%Y-%m-%d %H:%M')})"
            ),
        ]
        for ticker, side, qty, entry_c, mark_c, drawdown, pnl_dollars, action in triggered_rows:
            lines.append(
                f"- {ticker} {side} qty={qty} entry={entry_c:.1f}c mark={mark_c:.1f}c "
                f"loss={drawdown:.1%} pnl={pnl_dollars:+.2f}$ action={action}"
            )
        lines.append("Action: SELL/REDUCE flagged position(s) now unless you have stronger contrary data.")
        return "\n".join(lines)

    def _detect_drawdown_alerts(
        self,
        analysis: MarketAnalysis,
        *,
        threshold: float,
    ) -> list[tuple[str, str, int, float, float, float, float, str]]:
        triggered: list[tuple[str, str, int, float, float, float, float, str]] = []
        active_keys: set[str] = set()
        for rec in analysis.open_positions:
            position = rec.position
            key = _position_key(position.ticker, position.side)
            active_keys.add(key)
            entry_c = position.average_entry_price_cents
            mark_c = rec.liquidation_net_cents
            if entry_c is None or mark_c is None or float(entry_c) <= 0.0:
                self._loss_alert_active_by_position_key[key] = False
                continue
            drawdown = max(0.0, (float(entry_c) - float(mark_c)) / float(entry_c))
            is_active = drawdown >= threshold
            was_active = self._loss_alert_active_by_position_key.get(key, False)
            self._loss_alert_active_by_position_key[key] = is_active
            if is_active and not was_active:
                pnl_dollars = (float(mark_c) - float(entry_c)) * max(0, int(position.contracts)) / 100.0
                triggered.append(
                    (
                        position.ticker,
                        position.side.upper(),
                        int(position.contracts),
                        float(entry_c),
                        float(mark_c),
                        float(drawdown),
                        float(pnl_dollars),
                        rec.action,
                    )
                )
        for key in list(self._loss_alert_active_by_position_key.keys()):
            if key not in active_keys:
                self._loss_alert_active_by_position_key.pop(key, None)
        return triggered

    def _top_bracket_rows(self, analysis: MarketAnalysis, limit: int) -> list[tuple[str, float, Optional[float], str]]:
        by_ticker = {b.ticker: b for b in analysis.brackets}
        rows: list[tuple[str, float, Optional[float], str]] = []
        for ticker, model_prob in analysis.model_probabilities.items():
            bracket = by_ticker.get(ticker)
            market_prob = bracket.implied_prob if bracket is not None else None
            subtitle = bracket.subtitle if bracket is not None else ticker
            rows.append((ticker, float(model_prob), float(market_prob) if market_prob is not None else None, subtitle))
        rows.sort(key=lambda row: row[1], reverse=True)
        return rows[: max(1, int(limit))]

    def _build_bracket_shift_summary_message(
        self,
        *,
        analysis: MarketAnalysis,
        previous_ticker: str,
        new_ticker: str,
    ) -> str:
        by_ticker = {b.ticker: b for b in analysis.brackets}
        previous_label = by_ticker.get(previous_ticker).subtitle if by_ticker.get(previous_ticker) else previous_ticker
        new_bracket = by_ticker.get(new_ticker)
        new_label = new_bracket.subtitle if new_bracket is not None else new_ticker
        top_rows = self._top_bracket_rows(analysis, limit=max(1, int(ALERT_TOP_BRACKETS)))
        top_text = " | ".join(
            (
                f"{subtitle}: model {model_prob:.1%}, mkt {market_prob:.1%}"
                if market_prob is not None
                else f"{subtitle}: model {model_prob:.1%}"
            )
            for _ticker, model_prob, market_prob, subtitle in top_rows
        )
        today_mean = (
            analysis.adjusted_forecast_mean
            if analysis.adjusted_forecast_mean is not None
            else analysis.forecast_mean
        )
        today_std = (
            analysis.adjusted_forecast_std
            if analysis.adjusted_forecast_std is not None
            else analysis.forecast_std
        )
        obs_high = analysis.observation.observed_high_f if analysis.observation is not None else None
        lock_conf = (
            analysis.trajectory_assessment.lock_confidence
            if analysis.trajectory_assessment is not None
            else None
        )
        lines = [
            f"KWBOT BRACKET SHIFT ({analysis.city})",
            f"{analysis.target_date}: {previous_label} -> {new_label}",
            f"Today model: {today_mean:.1f}F +/- {today_std:.1f}F",
        ]
        if obs_high is not None:
            lines.append(f"Observed high so far: {obs_high:.1f}F")
        if lock_conf is not None:
            lines.append(f"High-already-set confidence: {lock_conf:.1%}")
        if analysis.tomorrow_date and analysis.tomorrow_forecast_mean is not None:
            lines.append(
                f"Tomorrow ({analysis.tomorrow_date}) forecast mean: {analysis.tomorrow_forecast_mean:.1f}F"
            )
        if top_text:
            lines.append(f"Top brackets: {top_text}")
        return "\n".join(lines)

    def _build_llm_prompt_message(
        self,
        *,
        analysis: MarketAnalysis,
        previous_ticker: str,
        new_ticker: str,
    ) -> str:
        by_ticker = {b.ticker: b for b in analysis.brackets}
        previous_label = by_ticker.get(previous_ticker).subtitle if by_ticker.get(previous_ticker) else previous_ticker
        new_label = by_ticker.get(new_ticker).subtitle if by_ticker.get(new_ticker) else new_ticker
        forecasts = analysis.forecasts[:6]
        top_rows = self._top_bracket_rows(analysis, limit=max(2, int(ALERT_TOP_BRACKETS)))
        now_local = datetime.now(self._market_tz).strftime("%Y-%m-%d %H:%M %Z")

        lines: list[str] = [
            "LLM PROMPT INPUT (Kalshi weather bracket re-eval)",
            f"Timestamp: {now_local}",
            f"City: {analysis.city}",
            f"Target date: {analysis.target_date}",
            f"Tomorrow date: {analysis.tomorrow_date or 'n/a'}",
            f"Primary bracket shift: {previous_label} -> {new_label}",
            "",
            "Today forecast state:",
            (
                f"- adjusted_mean_f: {analysis.adjusted_forecast_mean:.2f}"
                if analysis.adjusted_forecast_mean is not None
                else f"- model_mean_f: {analysis.forecast_mean:.2f}"
            ),
            (
                f"- adjusted_std_f: {analysis.adjusted_forecast_std:.2f}"
                if analysis.adjusted_forecast_std is not None
                else f"- model_std_f: {analysis.forecast_std:.2f}"
            ),
            f"- tomorrow_mean_f: {analysis.tomorrow_forecast_mean if analysis.tomorrow_forecast_mean is not None else 'n/a'}",
        ]
        if analysis.observation is not None:
            lines.append(f"- observed_high_f: {analysis.observation.observed_high_f:.2f}")
            if analysis.observation.readings:
                lines.append(f"- current_temp_f: {analysis.observation.readings[-1].reported_temp_f:.2f}")
        if analysis.trajectory_assessment is not None:
            lines.append(f"- lock_confidence: {analysis.trajectory_assessment.lock_confidence:.3f}")
            lines.append(
                f"- prob_exceed_observed_high: {analysis.trajectory_assessment.prob_exceed_observed_high:.3f}"
            )
        lines.append("")
        lines.append("Forecast sources:")
        for fc in forecasts:
            lines.append(f"- {fc.source}: temp_f={fc.forecast_temp_f:.2f}, std_f={fc.std_dev:.2f}")
        lines.append("")
        lines.append("Top bracket probabilities (model vs market):")
        for _ticker, model_prob, market_prob, subtitle in top_rows:
            if market_prob is None:
                lines.append(f"- {subtitle}: model={model_prob:.4f}")
            else:
                edge_pp = (model_prob - market_prob) * 100.0
                lines.append(
                    f"- {subtitle}: model={model_prob:.4f}, market={market_prob:.4f}, edge_pp={edge_pp:+.2f}"
                )
        lines.append("")
        lines.append("Open positions (side-price basis):")
        if analysis.open_positions:
            for rec in analysis.open_positions[:6]:
                pos = rec.position
                entry = pos.average_entry_price_cents
                mark = rec.liquidation_net_cents
                drawdown = "n/a"
                if entry is not None and mark is not None and float(entry) > 0.0:
                    drawdown = f"{max(0.0, (float(entry) - float(mark)) / float(entry)):.3f}"
                lines.append(
                    f"- {pos.ticker} {pos.side} qty={pos.contracts} entry_c={entry} mark_c={mark} "
                    f"drawdown_frac={drawdown} action={rec.action}"
                )
        else:
            lines.append("- none")
        lines.append("")
        lines.append(
            "Task: infer the most likely settlement bracket, estimate confidence, "
            "and provide a hold/sell recommendation for each open position using a max loss tolerance of 35%."
        )
        return "\n".join(lines)

    def _maybe_alert_on_drawdown(self, analysis: MarketAnalysis) -> None:
        threshold = max(0.01, min(0.95, float(ALERT_POSITION_LOSS_FRACTION)))
        triggered = self._detect_drawdown_alerts(analysis, threshold=threshold)
        if not triggered:
            return
        message = self._build_drawdown_alert_message(threshold=threshold, triggered_rows=triggered)
        self._send_alert_message(message, force=True)

    def _build_source_change_alert_message(
        self,
        *,
        analysis: MarketAnalysis,
        changed_rows: list[tuple[str, float, float, datetime]],
    ) -> str:
        mean_f = (
            analysis.adjusted_forecast_mean
            if analysis.adjusted_forecast_mean is not None
            else analysis.forecast_mean
        )
        std_f = (
            analysis.adjusted_forecast_std
            if analysis.adjusted_forecast_std is not None
            else analysis.forecast_std
        )
        lines = [
            (
                f"KWBOT SOURCE UPDATE ({analysis.city}) "
                f"{analysis.target_date} {datetime.now(self._market_tz).strftime('%H:%M')}"
            ),
            f"Model now: {mean_f:.1f}F +/- {std_f:.1f}F",
        ]
        if analysis.observation is not None:
            lines.append(f"Observed high: {analysis.observation.observed_high_f:.1f}F")
            if analysis.observation.readings:
                lines.append(f"Current temp: {analysis.observation.readings[-1].reported_temp_f:.1f}F")
        for source, temp_f, delta_f, changed_at in changed_rows:
            changed_local = changed_at.astimezone(self._market_tz).strftime("%H:%M")
            lines.append(
                f"- {source}: {temp_f:.1f}F (delta {delta_f:+.1f}F @ {changed_local})"
            )
        lines.append("Manual check: source inputs moved; review bracket exposure.")
        return "\n".join(lines)

    def _maybe_alert_on_source_change(self, analysis: MarketAnalysis) -> None:
        if not ALERT_SOURCE_CHANGE_ENABLED:
            return
        min_delta = max(0.0, float(ALERT_SOURCE_CHANGE_MIN_DELTA_F))
        changed_rows: list[tuple[str, float, float, datetime]] = []
        for fc in analysis.forecasts:
            source = str(fc.source)
            delta = analysis.source_last_change_delta.get(source)
            changed_at = analysis.source_last_changed_at.get(source)
            if delta is None or changed_at is None:
                continue
            try:
                delta_f = float(delta)
            except (TypeError, ValueError):
                continue
            if abs(delta_f) < min_delta:
                continue
            key = (analysis.target_date, source)
            prior_notified = self._source_change_alert_last_notified_at_by_key.get(key)
            if prior_notified is not None and changed_at <= prior_notified:
                continue
            self._source_change_alert_last_notified_at_by_key[key] = changed_at
            changed_rows.append((source, float(fc.forecast_temp_f), delta_f, changed_at))

        if not changed_rows:
            return
        changed_rows.sort(key=lambda row: row[0].lower())
        message = self._build_source_change_alert_message(
            analysis=analysis,
            changed_rows=changed_rows,
        )
        self._send_alert_message(message, force=False)

    def _build_observation_divergence_alert_message(
        self,
        *,
        analysis: MarketAnalysis,
        direction: str,
        model_mean_f: float,
        model_std_f: float,
        current_temp_f: float,
        observed_high_f: float,
        gap_current_f: float,
        gap_high_f: float,
    ) -> str:
        traj = analysis.trajectory_assessment
        lines = [
            (
                f"KWBOT LIVE DIVERGENCE ({analysis.city}) "
                f"{analysis.target_date} {datetime.now(self._market_tz).strftime('%H:%M')}"
            ),
            f"Signal: {direction}",
            f"Model mean/std: {model_mean_f:.1f}F +/- {model_std_f:.1f}F",
            (
                f"Current temp: {current_temp_f:.1f}F (gap {gap_current_f:+.1f}F), "
                f"Observed high: {observed_high_f:.1f}F (gap {gap_high_f:+.1f}F)"
            ),
        ]
        if traj is not None:
            lines.append(
                (
                    "Trajectory: "
                    f"exceed_high={traj.prob_exceed_observed_high:.0%}, "
                    f"lock_conf={traj.lock_confidence:.0%}, "
                    f"trend={traj.trend_f_per_hour:+.2f}F/hr"
                )
            )
        if analysis.open_positions:
            lines.append("Open positions:")
            for rec in analysis.open_positions[:4]:
                pos = rec.position
                entry = pos.average_entry_price_cents
                mark = rec.liquidation_net_cents
                lines.append(
                    f"- {pos.ticker} {pos.side} qty={pos.contracts} entry={entry} mark={mark} action={rec.action}"
                )
        lines.append("Manual decision: re-check thesis; trim/exit risk if mismatch persists.")
        return "\n".join(lines)

    def _maybe_alert_on_observation_divergence(self, analysis: MarketAnalysis) -> None:
        if not ALERT_OBS_DIVERGENCE_ENABLED:
            return
        obs = analysis.observation
        traj = analysis.trajectory_assessment
        key = analysis.target_date
        if obs is None or traj is None:
            self._obs_divergence_alert_active_by_target_date[key] = False
            return

        model_mean = (
            analysis.adjusted_forecast_mean
            if analysis.adjusted_forecast_mean is not None
            else analysis.forecast_mean
        )
        model_std = (
            analysis.adjusted_forecast_std
            if analysis.adjusted_forecast_std is not None
            else analysis.forecast_std
        )
        current_temp = (
            float(obs.readings[-1].reported_temp_f)
            if obs.readings
            else float(obs.observed_high_f)
        )
        observed_high = float(obs.observed_high_f)
        gap_current = current_temp - float(model_mean)
        gap_high = observed_high - float(model_mean)

        threshold_f = max(0.5, float(ALERT_OBS_DIVERGENCE_TEMP_F))
        exceed_prob_threshold = max(0.0, min(1.0, float(ALERT_OBS_DIVERGENCE_EXCEED_PROB)))
        lock_conf_threshold = max(0.0, min(1.0, float(ALERT_OBS_DIVERGENCE_LOCK_CONFIDENCE)))

        hotter_than_model = (
            (gap_current >= threshold_f or gap_high >= threshold_f)
            and float(traj.prob_exceed_observed_high) >= exceed_prob_threshold
        )
        cooler_than_model = (
            (gap_current <= -threshold_f and gap_high <= -threshold_f)
            and float(traj.lock_confidence) >= lock_conf_threshold
            and float(traj.trend_f_per_hour) <= 0.15
        )
        triggered = hotter_than_model or cooler_than_model
        was_active = self._obs_divergence_alert_active_by_target_date.get(key, False)
        self._obs_divergence_alert_active_by_target_date[key] = triggered
        if not triggered or was_active:
            return

        direction = "live temperatures are running HOTTER than model"
        if cooler_than_model:
            direction = "live temperatures are running COOLER than model"
        message = self._build_observation_divergence_alert_message(
            analysis=analysis,
            direction=direction,
            model_mean_f=float(model_mean),
            model_std_f=float(model_std),
            current_temp_f=float(current_temp),
            observed_high_f=float(observed_high),
            gap_current_f=float(gap_current),
            gap_high_f=float(gap_high),
        )
        self._send_alert_message(message, force=True)

    def _build_hourly_update_message(
        self,
        *,
        analysis: MarketAnalysis,
        now_local: datetime,
    ) -> str:
        model_mean = (
            analysis.adjusted_forecast_mean
            if analysis.adjusted_forecast_mean is not None
            else analysis.forecast_mean
        )
        model_std = (
            analysis.adjusted_forecast_std
            if analysis.adjusted_forecast_std is not None
            else analysis.forecast_std
        )
        top_bracket_line = "n/a"
        if analysis.model_probabilities and analysis.brackets:
            top_ticker, top_prob = max(analysis.model_probabilities.items(), key=lambda item: item[1])
            bracket = next((b for b in analysis.brackets if b.ticker == top_ticker), None)
            if bracket is not None:
                top_bracket_line = (
                    f"{bracket.subtitle} model={float(top_prob):.1%} "
                    f"mkt={float(bracket.implied_prob):.1%}"
                )
            else:
                top_bracket_line = f"{top_ticker} model={float(top_prob):.1%}"

        current_temp = None
        observed_high = None
        if analysis.observation is not None:
            observed_high = float(analysis.observation.observed_high_f)
            if analysis.observation.readings:
                current_temp = float(analysis.observation.readings[-1].reported_temp_f)

        tomorrow_line = "n/a"
        if analysis.tomorrow_date and analysis.tomorrow_forecast_mean is not None:
            tomorrow_line = f"{analysis.tomorrow_date}: {float(analysis.tomorrow_forecast_mean):.1f}F"
        elif analysis.tomorrow_date:
            tomorrow_line = f"{analysis.tomorrow_date}: n/a"

        worst_drawdown = 0.0
        for rec in analysis.open_positions:
            pos = rec.position
            entry = pos.average_entry_price_cents
            mark = rec.liquidation_net_cents
            if entry is None or mark is None or float(entry) <= 0.0:
                continue
            dd = max(0.0, (float(entry) - float(mark)) / float(entry))
            worst_drawdown = max(worst_drawdown, dd)

        lines = [
            f"KWBOT HOURLY UPDATE {now_local.strftime('%Y-%m-%d %H:%M %Z')}",
            f"Target: {analysis.target_date}",
            f"Model: {float(model_mean):.1f}F +/- {float(model_std):.1f}F",
            (
                f"Current/High: {current_temp:.1f}F / {observed_high:.1f}F"
                if current_temp is not None and observed_high is not None
                else "Current/High: n/a"
            ),
            f"Top bracket: {top_bracket_line}",
            f"Tm prediction: {tomorrow_line}",
            (
                f"Open positions: {len(analysis.open_positions)} "
                f"(worst drawdown {worst_drawdown:.1%})"
            ),
        ]
        return "\n".join(lines)

    def _maybe_send_hourly_update(
        self,
        analysis: MarketAnalysis,
        *,
        now_local: Optional[datetime] = None,
    ) -> None:
        if not ALERT_HOURLY_UPDATE_ENABLED:
            return
        now_local = now_local or datetime.now(self._market_tz)
        minute_target = max(0, min(59, int(ALERT_HOURLY_UPDATE_MINUTE_LOCAL)))
        if now_local.minute < minute_target:
            return
        hour_key = now_local.strftime("%Y-%m-%d %H")
        if self._last_hourly_update_key == hour_key:
            return
        message = self._build_hourly_update_message(
            analysis=analysis,
            now_local=now_local,
        )
        if self._send_alert_message(message, force=True):
            self._last_hourly_update_key = hour_key

    def _maybe_alert_on_bracket_shift(self, analysis: MarketAnalysis) -> None:
        if not ALERT_BRACKET_CHANGE_ENABLED:
            return
        if not analysis.model_probabilities:
            return
        top_ticker, _top_prob = max(analysis.model_probabilities.items(), key=lambda item: item[1])
        previous_ticker = self._last_primary_ticker_by_target_date.get(analysis.target_date)
        self._last_primary_ticker_by_target_date[analysis.target_date] = top_ticker
        if previous_ticker is None:
            return
        if previous_ticker == top_ticker:
            return
        summary = self._build_bracket_shift_summary_message(
            analysis=analysis,
            previous_ticker=previous_ticker,
            new_ticker=top_ticker,
        )
        self._send_alert_message(summary, force=True)
        if ALERT_LLM_PROMPT_ENABLED:
            llm_prompt = self._build_llm_prompt_message(
                analysis=analysis,
                previous_ticker=previous_ticker,
                new_ticker=top_ticker,
            )
            self._send_alert_message(llm_prompt, force=True)

    def _process_notifications(self, analysis: MarketAnalysis) -> None:
        if not self._notifications_config_ready():
            return
        self._maybe_send_hourly_update(analysis)
        self._maybe_alert_on_source_change(analysis)
        self._maybe_alert_on_drawdown(analysis)
        self._maybe_alert_on_observation_divergence(analysis)
        self._maybe_alert_on_bracket_shift(analysis)

    def perform_analysis(self) -> MarketAnalysis:
        """Run one full analysis cycle."""
        now_market = datetime.now(self._market_tz)
        now_market_day = now_market.strftime("%Y-%m-%d")
        today = (now_market + timedelta(days=MARKET_TARGET_DAY_OFFSET)).strftime("%Y-%m-%d")
        configured_tomorrow = (now_market + timedelta(days=MARKET_TARGET_DAY_OFFSET + 1)).strftime("%Y-%m-%d")

        # Prefer the configured target day. If absent, optionally fall back.
        target_date = today
        brackets = self.contract.fetch_brackets(target_date)
        if not brackets and not MARKET_TARGET_STRICT:
            logger.warning(
                "No market brackets found for %s; falling back to %s",
                target_date,
                configured_tomorrow,
            )
            target_date = configured_tomorrow
            brackets = self.contract.fetch_brackets(target_date)
        elif not brackets and MARKET_TARGET_STRICT:
            logger.warning(
                "No market brackets found for strict target date %s; waiting for market to go live.",
                target_date,
            )
        next_day_date = (
            datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        current_yes_ask_by_ticker = {b.ticker: int(b.yes_ask) for b in brackets}

        if self._scheduler_day_anchor != now_market_day:
            self._cached_forecasts_by_date.clear()
            self._cached_observations_by_date.clear()
            self._source_last_forecast_temp_by_key.clear()
            self._source_last_changed_at_by_key.clear()
            self._source_last_delta_by_key.clear()
            self._next_forecast_refresh_at = None
            self._next_hrrr_refresh_at = None
            self._next_metar_refresh_at = None
            self._scheduler_day_anchor = now_market_day
            self._save_source_change_state(day_anchor=now_market_day)

        # 1. Fetch forecasts (event-driven poll interval + hourly HRRR anchor)
        should_refresh_forecasts = (
            (not self._event_driven_scheduler_enabled)
            or (target_date not in self._cached_forecasts_by_date)
            or (next_day_date not in self._cached_forecasts_by_date)
            or (self._next_forecast_refresh_at is None)
            or (now_market >= self._next_forecast_refresh_at)
            or (self._next_hrrr_refresh_at is None)
            or (now_market >= self._next_hrrr_refresh_at)
        )
        if should_refresh_forecasts:
            forecasts = self.contract.fetch_forecasts(target_date)
            tomorrow_forecasts = self.contract.fetch_forecasts(next_day_date)
            self._cached_forecasts_by_date[target_date] = list(forecasts)
            self._cached_forecasts_by_date[next_day_date] = list(tomorrow_forecasts)
            if self._event_driven_scheduler_enabled:
                self._next_forecast_refresh_at = now_market + timedelta(
                    seconds=max(30, FORECAST_POLL_INTERVAL_SECONDS)
                )
                self._next_hrrr_refresh_at = self._next_hrrr_refresh_after(now_market)
            # Update rolling forecast-error calibration store (best effort).
            try:
                self.calibrator.record_forecast_snapshot(forecasts, target_date=target_date)
                self.calibrator.refresh_settlement_errors()
            except Exception as exc:
                logger.warning("Forecast calibration update skipped: %s", exc)
        else:
            forecasts = list(self._cached_forecasts_by_date.get(target_date, []))
            tomorrow_forecasts = list(self._cached_forecasts_by_date.get(next_day_date, []))

        source_last_changed_at, source_last_change_delta = self._track_source_forecast_changes(
            forecasts=forecasts,
            target_date=target_date,
            now_market=now_market,
        )

        source_validation_error: Optional[str] = None
        if REQUIRED_FORECAST_SOURCES:
            loaded_sources = {f.source for f in forecasts}
            missing_sources = [s for s in REQUIRED_FORECAST_SOURCES if s not in loaded_sources]
            if missing_sources:
                source_validation_error = "missing forecast sources: " + ", ".join(missing_sources)
                logger.warning(
                    "Forecast source validation failed for %s: %s (loaded=%s)",
                    target_date,
                    ", ".join(missing_sources),
                    ", ".join(sorted(loaded_sources)) if loaded_sources else "none",
                )

        # 2. Fetch Observations (event-driven 5-minute refresh when enabled)
        # - `target_observation` is used for model conditioning only when target date
        #   is the current market day.
        # - `live_observation` drives dashboard/auto-buy trend checks.
        missing_target_obs = target_date not in self._cached_observations_by_date
        missing_live_obs = (target_date != now_market_day) and (now_market_day not in self._cached_observations_by_date)
        cached_live_observation = (
            self._cached_observations_by_date.get(now_market_day)
            if target_date != now_market_day
            else self._cached_observations_by_date.get(target_date)
        )
        live_obs_age_min = _observation_age_minutes(cached_live_observation, now_market)
        stale_live_obs = live_obs_age_min is not None and live_obs_age_min > 75.0
        if stale_live_obs:
            logger.warning(
                "Live observation appears stale (age=%.0fm); forcing immediate refresh.",
                float(live_obs_age_min),
            )
        should_refresh_observations = (
            (not self._event_driven_scheduler_enabled)
            or missing_target_obs
            or missing_live_obs
            or stale_live_obs
            or (self._next_metar_refresh_at is None)
            or (now_market >= self._next_metar_refresh_at)
        )
        if should_refresh_observations:
            target_observation = self.station_source.get_daily_summary(target_date)
            self._cached_observations_by_date[target_date] = target_observation
            if target_date != now_market_day:
                live_observation = self.station_source.get_daily_summary(now_market_day)
                self._cached_observations_by_date[now_market_day] = live_observation
            else:
                live_observation = target_observation
            if self._event_driven_scheduler_enabled:
                self._next_metar_refresh_at = now_market + timedelta(seconds=max(30, METAR_POLL_INTERVAL_SECONDS))
        else:
            target_observation = self._cached_observations_by_date.get(target_date)
            if target_date != now_market_day:
                live_observation = self._cached_observations_by_date.get(now_market_day)
            else:
                live_observation = target_observation
        live_obs_age_after_fetch = _observation_age_minutes(live_observation, now_market)
        if live_obs_age_after_fetch is not None and live_obs_age_after_fetch > 90.0:
            logger.warning(
                (
                    "Live observation remains stale after refresh (age=%.0fm, station=%s). "
                    "Likely upstream NWS/API lag or connectivity issue."
                ),
                float(live_obs_age_after_fetch),
                self.station_source.station_id,
            )
        same_day_target = target_date == now_market_day
        
        # 4. Run Edge Detection
        signals = self.edge_detector.analyze(
            forecasts=forecasts,
            observation=target_observation,
            brackets=brackets,
            min_edge=MIN_EDGE_THRESHOLD,
        )
        
        # 5. Compile Analysis
        # We need to calculate combined stats manually here if not returned by edge detector?
        # Edge detector does calculations internally but returns signals.
        # To display "Combined Mean" etc, we might need to expose that from Edge Detector 
        # or re-calculate/get it from the intermediate steps.
        # 'EdgeDetector' doesn't currently return the 'AdjustedForecast' object. 
        # I should probably update EdgeDetector to return a full Analysis object or tuple.
        # But for now, let's just re-combine to get the mean for display 
        # (It's cheap enough, or I can refactor EdgeDetector).
        
        from kalshi_weather.engine.probability import (
            BracketProbabilityCalculator,
            adjust_forecast_with_observations,
            combine_forecasts,
        )

        viability_high_f = _buy_viability_high_f(target_observation) if same_day_target else None
        combined = combine_forecasts(forecasts) if forecasts else None
        adjusted = None
        tomorrow_combined = combine_forecasts(tomorrow_forecasts) if tomorrow_forecasts else None
        raw_tomorrow_mean_f = tomorrow_combined.mean_temp_f if tomorrow_combined else None
        tomorrow_forecast_mean = _apply_tomorrow_midnight_carryover_floor(
            tomorrow_mean_f=raw_tomorrow_mean_f,
            tomorrow_date=next_day_date,
            now_local=now_market,
            observation=live_observation,
        )
        if (
            raw_tomorrow_mean_f is not None
            and tomorrow_forecast_mean is not None
            and tomorrow_forecast_mean > float(raw_tomorrow_mean_f)
        ):
            logger.info(
                (
                    "Tomorrow forecast carryover floor applied for %s: "
                    "raw=%.1fF adjusted=%.1fF"
                ),
                next_day_date,
                float(raw_tomorrow_mean_f),
                float(tomorrow_forecast_mean),
            )
        model_probabilities: dict[str, float] = {}
        if combined and brackets:
            adjusted = adjust_forecast_with_observations(combined, target_observation)
            bracket_probs = BracketProbabilityCalculator().calculate_all_probabilities(
                brackets,
                adjusted.mean_temp_f,
                adjusted.std_dev,
                lower_bound=adjusted.conditioning_cutoff_f,
                upper_bound=adjusted.conditioning_ceiling_f,
                viability_floor_f=viability_high_f,
            )
            probs_by_ticker = {bp.bracket.ticker: bp for bp in bracket_probs}
            synced_signals = []
            for sig in signals:
                bp = probs_by_ticker.get(sig.bracket.ticker)
                if bp is None:
                    synced_signals.append(sig)
                    continue
                synced_edge = float(bp.model_prob) - float(bp.market_prob)
                synced_signals.append(
                    replace(
                        sig,
                        model_prob=float(bp.model_prob),
                        market_prob=float(bp.market_prob),
                        edge=synced_edge,
                        confidence=self.edge_detector._calculate_confidence(abs(synced_edge), adjusted.std_dev),
                        reasoning=self.edge_detector._build_reasoning(
                            sig.bracket.subtitle,
                            synced_edge,
                            float(bp.model_prob),
                            float(bp.market_prob),
                            adjusted,
                        ),
                    )
                )
            signals = synced_signals
            model_probabilities = {bp.bracket.ticker: bp.model_prob for bp in bracket_probs}
        else:
            logger.warning(
                "Model probability update skipped (combined=%s, brackets=%s)",
                bool(combined),
                bool(brackets),
            )

        if brackets and viability_high_f is not None:
            non_viable_tickers = {
                b.ticker
                for b in brackets
                if not _yes_bracket_still_viable(b, viability_high_f)
            }
            if non_viable_tickers:
                signals = [
                    s for s in signals
                    if not (s.direction == "BUY" and s.bracket.ticker in non_viable_tickers)
                ]

        all_open_positions = self.contract.fetch_open_positions()
        # Backfill missing average-entry from local trade journal when API omits it.
        patched_positions: list[OpenPosition] = []
        for p in all_open_positions:
            if p.average_entry_price_cents is not None:
                patched_positions.append(p)
                continue
            fallback_entry = self._journal.estimate_open_entry_price_cents(
                ticker=p.ticker,
                side=p.side,
            )
            if fallback_entry is None:
                patched_positions.append(p)
            else:
                patched_positions.append(
                    replace(p, average_entry_price_cents=int(fallback_entry))
                )
        all_open_positions = patched_positions
        all_current_qty: dict[str, int] = {
            _position_key(p.ticker, p.side): int(max(0, p.contracts))
            for p in all_open_positions
        }
        lifecycle_events: list[str] = []

        # One-time startup recovery:
        # if we have bot-managed keys in journal but no in-memory qty limits
        # (common after restart), attribute current open qty back to bot management.
        if (
            BOT_MANAGE_ONLY_TAGGED_POSITIONS
            and not self._managed_qty_bootstrapped
            and self._managed_position_keys
        ):
            recovered = 0
            for key in list(self._managed_position_keys):
                qty = int(max(0, all_current_qty.get(key, 0)))
                if qty <= 0:
                    continue
                if self._managed_qty_limits.get(key, 0) <= 0:
                    self._managed_qty_limits[key] = qty
                    recovered += 1
            if recovered > 0:
                lifecycle_events.append(
                    f"MANAGED_RECOVERY restored {recovered} bot-tagged position(s) after restart"
                )
            self._managed_qty_bootstrapped = True

        # Promote pending bot buys to managed qty only when observed position qty
        # actually increases vs prior cycle (fill-confirmed attribution).
        for key, pending in list(self._pending_buy_qty.items()):
            if pending <= 0:
                self._pending_buy_qty.pop(key, None)
                continue
            cur_qty = all_current_qty.get(key, 0)
            prev_qty = self._previous_all_open_position_qty.get(key, 0)
            observed_increase = max(0, cur_qty - prev_qty)
            attributed_fill = min(pending, observed_increase)
            if attributed_fill <= 0:
                continue
            self._managed_qty_limits[key] = self._managed_qty_limits.get(key, 0) + int(attributed_fill)
            self._pending_buy_qty[key] = pending - int(attributed_fill)
            if self._pending_buy_qty[key] <= 0:
                self._pending_buy_qty.pop(key, None)
            ticker, side = key.split("|", 1)
            self._journal.set_managed_position(ticker=ticker, side=side, active=True)
            self._managed_position_keys.add(key)
            lifecycle_events.append(f"BOT_FILL {ticker} {side} qty={int(attributed_fill)}")

        # Reconcile managed limits downward to what's actually open now.
        for key, managed_qty in list(self._managed_qty_limits.items()):
            cur_qty = all_current_qty.get(key, 0)
            next_qty = min(int(managed_qty), int(cur_qty))
            if next_qty > 0:
                self._managed_qty_limits[key] = next_qty
                continue
            self._managed_qty_limits.pop(key, None)
            ticker, side = key.split("|", 1)
            self._journal.set_managed_position(ticker=ticker, side=side, active=False)
            self._managed_position_keys.discard(key)

        if BOT_MANAGE_ONLY_TAGGED_POSITIONS:
            open_positions: list[OpenPosition] = []
            for p in all_open_positions:
                key = _position_key(p.ticker, p.side)
                managed_qty = self._managed_qty_limits.get(key, 0)
                if managed_qty <= 0:
                    continue
                managed_contracts = min(int(p.contracts), int(managed_qty))
                if managed_contracts <= 0:
                    continue
                open_positions.append(replace(p, contracts=managed_contracts))
        else:
            open_positions = all_open_positions
        desynced_managed_keys: list[str] = []
        if BOT_MANAGE_ONLY_TAGGED_POSITIONS and self._managed_position_keys:
            for p in all_open_positions:
                key = _position_key(p.ticker, p.side)
                if key not in self._managed_position_keys:
                    continue
                if self._managed_qty_limits.get(key, 0) > 0:
                    continue
                desynced_managed_keys.append(key)
            if desynced_managed_keys:
                logger.warning(
                    "Managed-position qty desync detected for %d key(s): %s",
                    len(desynced_managed_keys),
                    ", ".join(desynced_managed_keys[:3]),
                )
        active_event_ticker = brackets[0].event_ticker if brackets else None
        def _is_active_event_position(position: OpenPosition) -> bool:
            if active_event_ticker is None:
                return False
            evt = str(position.event_ticker or "").strip()
            if evt == active_event_ticker:
                return True
            ticker = str(position.ticker or "").strip()
            if not ticker:
                return False
            return ticker == active_event_ticker or ticker.startswith(f"{active_event_ticker}-")

        sell_positions = (
            [p for p in open_positions if _is_active_event_position(p)]
            if active_event_ticker is not None
            else list(open_positions)
        )
        if (
            AUTO_SELL_INCLUDE_EVENT_POSITIONS
            and active_event_ticker is not None
        ):
            # Merge in visible account positions for the active event so auto-sell
            # can unwind risk even when positions are not journal-tagged.
            by_key: dict[str, OpenPosition] = {
                _position_key(p.ticker, p.side): p for p in sell_positions
            }
            for p in all_open_positions:
                if not _is_active_event_position(p):
                    continue
                by_key[_position_key(p.ticker, p.side)] = p
            sell_positions = list(by_key.values())
        account_event_positions = [p for p in all_open_positions if _is_active_event_position(p)]
        portfolio_totals = self.contract.fetch_portfolio_totals(
            event_ticker=active_event_ticker
        )
        now_local = datetime.now(self._market_tz)
        current_qty: dict[str, int] = {
            f"{p.ticker}|{p.side.upper()}": p.contracts for p in sell_positions
        }
        active_keys = set(current_qty.keys())
        for key in list(self._position_first_seen_at.keys()):
            if key not in active_keys:
                self._position_first_seen_at.pop(key, None)
                self._primary_flip_risk_streaks.pop(key, None)
        for key in active_keys:
            self._position_first_seen_at.setdefault(key, now_local)
        position_age_minutes: dict[str, float] = {}
        for key, ts in self._position_first_seen_at.items():
            age_min = max(0.0, (now_local - ts).total_seconds() / 60.0)
            position_age_minutes[key] = age_min
        for key, prev_qty in self._previous_open_position_qty.items():
            cur_qty = current_qty.get(key, 0)
            ticker, side = key.split("|", 1)
            if cur_qty <= 0 and prev_qty > 0:
                lifecycle_events.append(f"SOLD {ticker} {side} qty={prev_qty}")
                self._journal.set_managed_position(ticker=ticker, side=side, active=False)
                self._managed_position_keys.discard(_position_key(ticker, side))
                self._journal.log_order_event(
                    event_type="position_closed",
                    target_date=target_date,
                    ticker=ticker,
                    side=side,
                    qty=prev_qty,
                    status="closed",
                    reason="qty reached zero",
                )
            elif cur_qty < prev_qty:
                lifecycle_events.append(
                    f"REDUCED {ticker} {side} qty={prev_qty} -> {cur_qty}"
                )
                self._journal.log_order_event(
                    event_type="position_reduced",
                    target_date=target_date,
                    ticker=ticker,
                    side=side,
                    qty=cur_qty,
                    status="open",
                    reason=f"qty {prev_qty}->{cur_qty}",
                )
        managed_active_keys = {k for k, qty in self._managed_qty_limits.items() if qty > 0}
        self._journal.deactivate_missing(managed_active_keys)
        decision_mean_f = adjusted.mean_temp_f if adjusted else (combined.mean_temp_f if combined else 0.0)
        decision_std_f = adjusted.std_dev if adjusted else (combined.std_dev if combined else 2.5)
        peak_prediction = self.decision_engine.estimate_peak_prediction(
            adjusted_mean_f=decision_mean_f,
            adjusted_std_f=decision_std_f,
            observation=live_observation,
            trajectory_assessment=adjusted.trajectory_assessment if adjusted else None,
            now_local=now_local,
        )
        kill_switch_paused = self._trading_paused_by_kill_switch()
        decision_snapshot = None
        if desynced_managed_keys:
            auto_buy_events = [
                "SAFETY BLOCK: auto-buy disabled (managed qty desync after restart; verify open positions and restart cleanly)"
            ]
            decision_snapshot = TradeDecisionSnapshot(
                status="SKIP",
                reason="managed qty desync safety block",
                max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
            )
        elif kill_switch_paused:
            auto_buy_events = [f"KILL SWITCH ACTIVE: auto-buy disabled ({self._kill_switch_mode()})"]
            decision_snapshot = TradeDecisionSnapshot(
                status="SKIP",
                reason=f"kill switch active ({self._kill_switch_mode()})",
                max_feasible_temp_f=peak_prediction.max_feasible_temp_f,
            )
        else:
            primary_outcome_ticker = None
            if model_probabilities:
                primary_outcome_ticker = max(model_probabilities, key=model_probabilities.get)
            buy_positions = [
                p for p in open_positions
                if _is_active_event_position(p)
            ] if active_event_ticker else open_positions
            auto_buy_events, decision_snapshot, peak_prediction = self._run_auto_buys(
                signals=signals,
                model_probabilities=model_probabilities,
                open_positions=buy_positions,
                portfolio_totals=portfolio_totals,
                observation=live_observation,
                trajectory_assessment=adjusted.trajectory_assessment if adjusted else None,
                adjusted_mean_f=decision_mean_f,
                adjusted_std_f=decision_std_f,
                target_date=target_date,
                is_same_day_target=same_day_target,
                primary_outcome_ticker=primary_outcome_ticker,
                source_validation_error=(
                    source_validation_error if STRICT_FORECAST_SOURCE_VALIDATION else None
                ),
            )
            if source_validation_error and not STRICT_FORECAST_SOURCE_VALIDATION:
                auto_buy_events = [f"SOURCE WARNING: {source_validation_error}"] + auto_buy_events

        final_window_cutoff = now_local.replace(
            hour=AUTO_PRIMARY_FINAL_WINDOW_CLOSE_HOUR_LOCAL,
            minute=AUTO_PRIMARY_FINAL_WINDOW_CLOSE_MINUTE_LOCAL,
            second=0,
            microsecond=0,
        ).time()
        final_window_open = now_local.time() < final_window_cutoff
        risk_buffer_f = max(
            AUTO_PRIMARY_RISK_BUFFER_FLOOR,
            (
                (adjusted.trajectory_assessment.expected_remaining_warming_f + 0.5 * adjusted.std_dev)
                if adjusted and adjusted.trajectory_assessment
                else AUTO_PRIMARY_RISK_BUFFER_FLOOR
            ),
        )
        observed_high_f = target_observation.observed_high_f if target_observation else None
        nearby_station_ids = NEARBY_STATION_IDS or list(self.city_config.nearby_station_ids)
        nearby_nowcast = self.station_source.summarize_nearby_nowcast(nearby_station_ids)
        remaining_mean_f = (
            adjusted.trajectory_assessment.expected_remaining_warming_f
            if adjusted and adjusted.trajectory_assessment
            else 0.6
        )
        remaining_std_f = (
            max(0.6, adjusted.std_dev * 0.45)
            if adjusted
            else 1.0
        )
        if observed_high_f is not None and nearby_nowcast:
            nearby_max = nearby_nowcast.get("max_temp_f")
            if nearby_max is not None:
                delta = max(0.0, float(nearby_max) - float(observed_high_f))
                delta = min(delta, max(0.0, NEARBY_STATION_MAX_DELTA_F))
                remaining_mean_f += delta * max(0.0, NEARBY_STATION_INFLUENCE)
        primary_flip_risk_by_ticker = {}
        primary_edge_exceed_prob_by_ticker = {}
        for b in brackets:
            distance = _distance_to_losing_edge_f(b, observed_high_f)
            edge_exceed_prob = _remaining_edge_exceed_prob(
                distance_to_edge_f=distance,
                remaining_mean_f=remaining_mean_f,
                remaining_std_f=remaining_std_f,
            )
            primary_edge_exceed_prob_by_ticker[b.ticker] = edge_exceed_prob
            hard_risk = edge_exceed_prob >= AUTO_PRIMARY_EDGE_EXCEED_TRIGGER_PROB
            soft_risk = edge_exceed_prob >= AUTO_PRIMARY_EDGE_EXCEED_WARN_PROB
            distance_risk = distance is not None and distance <= risk_buffer_f
            risk_now = final_window_open and (hard_risk or (soft_risk and distance_risk))
            streak_key = f"{b.ticker}|YES"
            prior = self._primary_flip_risk_streaks.get(streak_key, 0)
            current = (prior + 1) if risk_now else 0
            self._primary_flip_risk_streaks[streak_key] = current
            primary_flip_risk_by_ticker[b.ticker] = hard_risk or (
                current >= max(1, AUTO_PRIMARY_RISK_PERSISTENCE_CYCLES)
            )

        position_recommendations = evaluate_open_positions(
            positions=sell_positions,
            model_probabilities=model_probabilities,
            previous_model_probabilities=self._previous_model_probabilities,
            exit_fee_rate=POSITION_EXIT_FEE_RATE,
            brackets_by_ticker={b.ticker: b for b in brackets},
            observed_high_f=observed_high_f,
            final_window_open=final_window_open,
            primary_profit_lock_warn_prob=AUTO_PRIMARY_PROFIT_LOCK_WARN_PROB,
            primary_profit_lock_trigger_prob=AUTO_PRIMARY_PROFIT_LOCK_TRIGGER_PROB,
            primary_risk_buffer_f=risk_buffer_f,
            primary_flip_risk_by_ticker=primary_flip_risk_by_ticker,
            primary_edge_exceed_prob_by_ticker=primary_edge_exceed_prob_by_ticker,
            stop_loss_dollars=AUTO_EXIT_STOP_LOSS_DOLLARS,
            take_profit_dollars=AUTO_EXIT_TAKE_PROFIT_DOLLARS,
            take_profit_fraction=AUTO_EXIT_TARGET_PROFIT_FRACTION,
            confidence_drop_trigger_pp=AUTO_EXIT_CONFIDENCE_DROP_PP,
            min_model_prob_after_drop=AUTO_EXIT_MIN_MODEL_PROB,
        )
        for rec in position_recommendations:
            key = f"{rec.position.ticker}|{rec.position.side.upper()}"
            if rec.position.side.upper() != "YES":
                self._non_primary_streaks[key] = 0
                continue
            if rec.is_primary_outcome_position:
                self._non_primary_streaks[key] = 0
            else:
                self._non_primary_streaks[key] = self._non_primary_streaks.get(key, 0) + 1
        auto_events = execute_auto_sells(
            recommendations=position_recommendations,
            contract=self.contract,
            enabled=AUTO_SELL_ENABLED and not kill_switch_paused,
            dry_run=AUTO_SELL_DRY_RUN,
            sell_on_wrong_position=AUTO_SELL_ON_WRONG_POSITION,
            place_target_orders=AUTO_SELL_PLACE_TARGETS,
            max_contracts=AUTO_SELL_MAX_CONTRACTS,
            min_profit_cents=AUTO_SELL_MIN_PROFIT_CENTS,
            now_local=datetime.now(self._market_tz),
            start_hour_local=AUTO_SELL_START_HOUR_LOCAL,
            start_minute_local=AUTO_SELL_START_MINUTE_LOCAL,
            non_primary_streaks=self._non_primary_streaks,
            min_non_primary_cycles=AUTO_SELL_MIN_NON_PRIMARY_CYCLES,
            min_primary_gap_pp=AUTO_SELL_MIN_PRIMARY_GAP_PP,
            force_exit_hour_local=AUTO_SELL_FORCE_EXIT_HOUR_LOCAL,
            force_exit_minute_local=AUTO_SELL_FORCE_EXIT_MINUTE_LOCAL,
            position_age_minutes=position_age_minutes,
            min_hold_minutes=AUTO_SELL_MIN_HOLD_MINUTES,
            max_drawdown_fraction=AUTO_SELL_MAX_DRAWDOWN_FRACTION,
            allow_primary_scalp_targets=AUTO_SELL_ALLOW_PRIMARY_SCALP_TARGETS,
            primary_scalp_max_hold_minutes=AUTO_SELL_PRIMARY_SCALP_MAX_HOLD_MINUTES,
            respect_sell_now_actions=True,
        )
        if kill_switch_paused and AUTO_SELL_ENABLED:
            auto_events = [f"KILL SWITCH ACTIVE: auto-sell disabled ({self._kill_switch_mode()})"] + auto_events
        auto_events = auto_buy_events + auto_events
        for msg in auto_events:
            if msg.startswith("SUBMITTED_IOC sell "):
                m = re.search(r"sell\s+(\S+)\s+(\S+)\s+qty=(\d+)\s+px=(\d+)c", msg)
                if m:
                    self._journal.log_order_event(
                        event_type="sell_attempt",
                        target_date=target_date,
                        ticker=m.group(1),
                        side=m.group(2),
                        qty=int(m.group(3)),
                        price_cents=int(m.group(4)),
                        status="submitted",
                        reason=msg[:220],
                    )
            elif msg.startswith("DRY-RUN place sell "):
                m = re.search(r"sell\s+(\S+)\s+(\S+)\s+qty=(\d+)\s+px=(\d+)c", msg)
                if m:
                    self._journal.log_order_event(
                        event_type="sell_attempt",
                        target_date=target_date,
                        ticker=m.group(1),
                        side=m.group(2),
                        qty=int(m.group(3)),
                        price_cents=int(m.group(4)),
                        status="dry_run",
                        reason=msg[:220],
                    )
            elif msg.startswith("FAILED sell "):
                m = re.search(r"sell\s+(\S+)\s+(\S+)\s+qty=(\d+)\s+px=(\d+)c", msg)
                if m:
                    self._journal.log_order_event(
                        event_type="sell_attempt",
                        target_date=target_date,
                        ticker=m.group(1),
                        side=m.group(2),
                        qty=int(m.group(3)),
                        price_cents=int(m.group(4)),
                        status="failed",
                        reason=msg[:220],
                    )
        paper_totals: dict[str, float] = {}
        try:
            paper_marks = {}
            for position in all_open_positions:
                mark_cents = _paper_mark_price_cents(position)
                if mark_cents is None:
                    continue
                paper_marks[_position_key(position.ticker, position.side)] = int(mark_cents)
            paper_totals = self._journal.compute_paper_totals(
                starting_balance_dollars=PAPER_TRADING_START_BALANCE_DOLLARS,
                mark_price_cents_by_key=paper_marks,
                min_event_id_exclusive=self._paper_start_event_id,
            )
        except Exception as exc:
            logger.warning("Paper totals update skipped: %s", exc)

        self._previous_model_probabilities = model_probabilities
        self._previous_open_position_qty = current_qty
        self._previous_all_open_position_qty = all_current_qty
        self._previous_yes_ask_by_ticker = current_yes_ask_by_ticker

        display_mean = decision_mean_f
        display_std = decision_std_f

        return MarketAnalysis(
            city=self.city_config.name,
            target_date=target_date,
            forecasts=forecasts,
            observation=live_observation,
            brackets=brackets,
            signals=signals,
            forecast_mean=display_mean,
            forecast_std=display_std,
            analyzed_at=datetime.now(),
            raw_forecast_mean=combined.mean_temp_f if combined else None,
            raw_forecast_std=combined.std_dev if combined else None,
            adjusted_forecast_mean=adjusted.mean_temp_f if adjusted else None,
            adjusted_forecast_std=adjusted.std_dev if adjusted else None,
            tomorrow_date=next_day_date,
            tomorrow_forecast_mean=tomorrow_forecast_mean,
            source_last_changed_at=source_last_changed_at,
            source_last_change_delta=source_last_change_delta,
            model_probabilities=model_probabilities,
            open_positions=position_recommendations,
            account_open_positions=account_event_positions,
            auto_trader_events=auto_events,
            position_lifecycle_events=lifecycle_events,
            trajectory_assessment=adjusted.trajectory_assessment if adjusted else None,
            portfolio_totals=portfolio_totals,
            paper_trading_totals=paper_totals,
            peak_prediction=peak_prediction,
            decision_snapshot=decision_snapshot,
        )

    def _run_auto_buys(
        self,
        signals,
        model_probabilities: dict[str, float],
        open_positions,
        portfolio_totals: dict,
        observation,
        trajectory_assessment=None,
        adjusted_mean_f: float = 0.0,
        adjusted_std_f: float = 2.5,
        target_date: Optional[str] = None,
        is_same_day_target: bool = True,
        primary_outcome_ticker: Optional[str] = None,
        source_validation_error: Optional[str] = None,
    ) -> tuple[list[str], TradeDecisionSnapshot, Optional[PeakPrediction]]:
        events: list[str] = []
        now_local = datetime.now(self._market_tz)
        if not AUTO_BUY_ENABLED:
            peak = self.decision_engine.estimate_peak_prediction(
                adjusted_mean_f=adjusted_mean_f,
                adjusted_std_f=adjusted_std_f,
                observation=observation,
                trajectory_assessment=trajectory_assessment,
                now_local=now_local,
            )
            return (
                events,
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason="auto-buy disabled",
                    max_feasible_temp_f=peak.max_feasible_temp_f,
                ),
                peak,
            )
        if source_validation_error:
            peak = self.decision_engine.estimate_peak_prediction(
                adjusted_mean_f=adjusted_mean_f,
                adjusted_std_f=adjusted_std_f,
                observation=observation,
                trajectory_assessment=trajectory_assessment,
                now_local=now_local,
            )
            msg = f"SKIP auto-buy: source validation failed ({source_validation_error})"
            events.append(msg)
            return (
                events,
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason=msg,
                    max_feasible_temp_f=peak.max_feasible_temp_f,
                ),
                peak,
            )

        is_future_target = not is_same_day_target
        report_gate_active = (
            AUTO_BUY_AFTER_REPORT_ONLY
            and now_local.hour <= AUTO_BUY_REPORT_GATE_END_HOUR_LOCAL
            and not is_future_target
        )
        if report_gate_active and now_local.minute < AUTO_BUY_REPORT_MINUTE_LOCAL:
            peak = self.decision_engine.estimate_peak_prediction(
                adjusted_mean_f=adjusted_mean_f,
                adjusted_std_f=adjusted_std_f,
                observation=observation,
                trajectory_assessment=trajectory_assessment,
                now_local=now_local,
            )
            msg = (
                "SKIP auto-buy: waiting for hourly 6h-max report "
                f"(local {now_local.strftime('%H:%M')} < :{AUTO_BUY_REPORT_MINUTE_LOCAL:02d})"
            )
            events.append(msg)
            return (
                events,
                TradeDecisionSnapshot(
                    status="SKIP",
                    reason=msg,
                    max_feasible_temp_f=peak.max_feasible_temp_f,
                ),
                peak,
            )

        if observation is None or not observation.readings:
            if is_same_day_target:
                peak = self.decision_engine.estimate_peak_prediction(
                    adjusted_mean_f=adjusted_mean_f,
                    adjusted_std_f=adjusted_std_f,
                    observation=observation,
                    trajectory_assessment=trajectory_assessment,
                    now_local=now_local,
                )
                msg = "SKIP auto-buy: no fresh live observation history available"
                events.append(msg)
                return (
                    events,
                    TradeDecisionSnapshot(
                        status="SKIP",
                        reason=msg,
                        max_feasible_temp_f=peak.max_feasible_temp_f,
                    ),
                    peak,
                )
        else:
            latest_obs = observation.readings[-1].timestamp.astimezone(self._market_tz)
            obs_age_min = max(0.0, (now_local - latest_obs).total_seconds() / 60.0)
            if is_same_day_target and obs_age_min > 75.0:
                peak = self.decision_engine.estimate_peak_prediction(
                    adjusted_mean_f=adjusted_mean_f,
                    adjusted_std_f=adjusted_std_f,
                    observation=observation,
                    trajectory_assessment=trajectory_assessment,
                    now_local=now_local,
                )
                msg = f"SKIP auto-buy: latest observation is stale ({obs_age_min:.0f}m old)"
                events.append(msg)
                return (
                    events,
                    TradeDecisionSnapshot(
                        status="SKIP",
                        reason=msg,
                        max_feasible_temp_f=peak.max_feasible_temp_f,
                    ),
                    peak,
                )

        buy_signals = [s for s in signals if s.direction == "BUY"]
        if primary_outcome_ticker:
            primary_buy_signals = [
                s for s in buy_signals if s.bracket.ticker == primary_outcome_ticker
            ]
            if primary_buy_signals:
                buy_signals = primary_buy_signals
            elif AUTO_BUY_ALLOW_OFF_PRIMARY_WHEN_NO_PRIMARY_EDGE:
                events.append(
                    "Auto-buy note: primary bracket has no BUY edge; evaluating off-primary BUY tails"
                )
            else:
                peak = self.decision_engine.estimate_peak_prediction(
                    adjusted_mean_f=adjusted_mean_f,
                    adjusted_std_f=adjusted_std_f,
                    observation=observation,
                    trajectory_assessment=trajectory_assessment,
                    now_local=now_local,
                )
                msg = "SKIP auto-buy: primary bracket has no BUY edge (off-primary tails blocked)"
                events.append(msg)
                return (
                    events,
                    TradeDecisionSnapshot(
                        status="SKIP",
                        reason=msg,
                        max_feasible_temp_f=peak.max_feasible_temp_f,
                    ),
                    peak,
                )

        midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc = midnight_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
        trades_today = self._journal.count_order_events_since(
            event_type="buy_attempt",
            since_utc=midnight_utc,
            statuses=("submitted", "dry_run"),
        )

        decision_snapshot, selected_signal, peak_prediction = self.decision_engine.choose_trade(
            signals=buy_signals,
            model_probabilities=model_probabilities,
            previous_model_probabilities=self._previous_model_probabilities,
            previous_yes_ask_by_ticker=self._previous_yes_ask_by_ticker,
            observation=observation,
            trajectory_assessment=trajectory_assessment,
            adjusted_mean_f=adjusted_mean_f,
            adjusted_std_f=adjusted_std_f,
            now_local=now_local,
            trades_today=trades_today,
            open_position_count=len(open_positions),
            max_active_positions=AUTO_BUY_MAX_ACTIVE_POSITIONS,
            min_entry_price_cents=AUTO_BUY_MIN_ENTRY_PRICE_CENTS,
            max_entry_price_cents=AUTO_BUY_MAX_ENTRY_PRICE_CENTS,
            max_spread_cents=AUTO_BUY_MAX_SPREAD_CENTS,
            min_volume=AUTO_BUY_MIN_VOLUME,
        )

        if decision_snapshot.status != "TRADE" or selected_signal is None:
            events.append(f"SKIP auto-buy: {decision_snapshot.reason}")
            return events, decision_snapshot, peak_prediction

        signal = selected_signal
        ask_price = int(signal.bracket.yes_ask)
        bid_price = int(signal.bracket.yes_bid)
        entry_price = int(ask_price)
        maker_mode = False
        if AUTO_BUY_MAKER_FIRST_ENABLED and bid_price < ask_price:
            maker_quote = max(
                AUTO_BUY_MIN_ENTRY_PRICE_CENTS,
                bid_price + max(0, int(AUTO_BUY_MAKER_PRICE_OFFSET_CENTS)),
            )
            maker_quote = min(maker_quote, ask_price - 1, AUTO_BUY_MAX_ENTRY_PRICE_CENTS)
            if maker_quote >= AUTO_BUY_MIN_ENTRY_PRICE_CENTS and maker_quote < ask_price:
                entry_price = int(maker_quote)
                maker_mode = True
        decision_snapshot.yes_price_cents = int(entry_price)
        existing_same_ticker_yes_qty = 0
        for p in open_positions:
            if p.side.upper() == "YES" and p.ticker == signal.bracket.ticker:
                existing_same_ticker_yes_qty += int(max(0, p.contracts))
        if existing_same_ticker_yes_qty > 0:
            msg = (
                f"already holding {signal.bracket.ticker} YES "
                f"(qty={existing_same_ticker_yes_qty})"
            )
            events.append(f"SKIP auto-buy: {msg}")
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = msg
            return events, decision_snapshot, peak_prediction

        account_cash = None
        available_cash = None
        buying_power = None
        account_balance = None
        managed_open_exposure_dollars = 0.0
        bankroll_mode = "api-liquidity"
        if portfolio_totals:
            def _to_float_or_none(value):
                if value is None:
                    return None
                try:
                    text = str(value).strip()
                    if text.startswith("$"):
                        text = text[1:]
                    text = text.replace(",", "")
                    return float(text)
                except (TypeError, ValueError):
                    return None

            available_cash = _to_float_or_none(portfolio_totals.get("available_to_trade_dollars"))
            buying_power = _to_float_or_none(portfolio_totals.get("buying_power_dollars"))
            account_balance = _to_float_or_none(portfolio_totals.get("account_balance_dollars"))

            for candidate in (available_cash, buying_power, account_balance):
                if candidate is not None and candidate > 0.0:
                    account_cash = candidate
                    break
            if account_cash is None:
                for candidate in (available_cash, buying_power, account_balance):
                    if candidate is not None:
                        account_cash = candidate
                        break

        for p in open_positions:
            entry_cents = p.average_entry_price_cents
            if entry_cents is None:
                if p.side.upper() == "YES":
                    entry_cents = p.yes_ask if p.yes_ask is not None else p.last_price
                elif p.side.upper() == "NO":
                    if p.yes_bid is not None:
                        entry_cents = max(1, 100 - int(p.yes_bid))
                    elif p.last_price is not None:
                        entry_cents = max(1, 100 - int(p.last_price))
            if entry_cents is None:
                continue
            managed_open_exposure_dollars += max(0, int(entry_cents)) * max(0, int(p.contracts)) / 100.0

        if account_cash is not None:
            budget_cents = int(round(max(0.0, min(AUTO_BUY_MAX_BUDGET_DOLLARS, account_cash)) * 100.0))
            bankroll_mode = "api-liquidity"
        else:
            fallback_budget = max(0.0, AUTO_BUY_MAX_BUDGET_DOLLARS - managed_open_exposure_dollars)
            if fallback_budget <= 0.0:
                fallback_budget = max(0.0, min(AUTO_BUY_MAX_BUDGET_DOLLARS, AUTO_BUY_MAX_ORDER_DOLLARS))
                bankroll_mode = "cash-unknown-order-cap"
            else:
                bankroll_mode = "cash-unknown-exposure-cap"
            budget_cents = int(round(fallback_budget * 100.0))
            if not self._bankroll_fallback_warned:
                events.append(
                    "AUTO-BUY bankroll fallback: account liquidity unavailable; "
                    f"using mode={bankroll_mode} budget=${fallback_budget:.2f}"
                )
                self._bankroll_fallback_warned = True

        risk_cap_base = account_balance
        if risk_cap_base is None:
            risk_cap_base = available_cash if available_cash is not None else AUTO_BUY_MAX_BUDGET_DOLLARS
        max_allowed_exposure = max(0.0, float(risk_cap_base) * self.decision_engine.max_risk_fraction)
        risk_room_dollars = max(0.0, max_allowed_exposure - managed_open_exposure_dollars)
        budget_cents = min(budget_cents, int(round(risk_room_dollars * 100.0)))
        if budget_cents <= 0:
            msg = (
                f"risk cap reached (open=${managed_open_exposure_dollars:.2f} >= "
                f"max ${max_allowed_exposure:.2f})"
            )
            events.append(f"SKIP auto-buy: {msg}")
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = msg
            return events, decision_snapshot, peak_prediction

        effective_cost_per_contract = max(1, entry_price + AUTO_BUY_FEE_BUFFER_CENTS)
        min_order_cents = int(round(max(0.0, AUTO_BUY_MIN_ORDER_DOLLARS) * 100.0))
        max_order_cents = int(round(max(0.0, AUTO_BUY_MAX_ORDER_DOLLARS) * 100.0))
        if max_order_cents > 0:
            budget_cents = min(budget_cents, max_order_cents)

        max_affordable = budget_cents // effective_cost_per_contract
        min_needed = int(math.ceil(min_order_cents / effective_cost_per_contract)) if min_order_cents > 0 else 1
        max_affordable = min(max_affordable, AUTO_BUY_MAX_CONTRACTS_PER_ORDER)

        if max_affordable <= 0:
            should_attempt_unlock = (
                not AUTO_BUY_DRY_RUN
                and (
                    self._last_bankroll_unlock_attempt_at is None
                    or (now_local - self._last_bankroll_unlock_attempt_at).total_seconds() >= 90.0
                )
            )
            if should_attempt_unlock:
                self._last_bankroll_unlock_attempt_at = now_local
                canceled, detail = self.contract.cancel_resting_entry_orders(
                    client_order_prefix=f"{BOT_CLIENT_TAG}autobuy",
                    max_orders=25,
                )
                if canceled > 0:
                    events.append(
                        f"AUTO-UNLOCK: canceled {canceled} resting bot BUY order(s) to free buying power; retrying next cycle"
                    )
                else:
                    events.append(f"AUTO-UNLOCK: no cancellable resting bot BUY orders ({detail})")
            msg = (
                "no bankroll room "
                f"(budget={budget_cents}c, entry={entry_price}c, fee_buf={AUTO_BUY_FEE_BUFFER_CENTS}c, "
                f"available={available_cash}, buying_power={buying_power}, balance={account_balance}, "
                f"open_exposure=${managed_open_exposure_dollars:.2f}, mode={bankroll_mode})"
            )
            events.append(f"SKIP auto-buy: {msg}")
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = msg
            return events, decision_snapshot, peak_prediction

        count = self.decision_engine.size_contracts(
            snapshot=decision_snapshot,
            entry_price_cents=entry_price,
            max_affordable_contracts=max_affordable,
            min_contracts=min_needed,
        )
        if count < min_needed:
            msg = (
                f"cannot reach min order size (${AUTO_BUY_MIN_ORDER_DOLLARS:.2f}) "
                f"with current constraints (max_affordable={max_affordable})"
            )
            events.append(f"SKIP auto-buy: {msg}")
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = msg
            return events, decision_snapshot, peak_prediction

        if self.contract.has_resting_entry_like_order(signal.bracket.ticker, "YES", entry_price):
            msg = f"matching resting BUY exists {signal.bracket.ticker} YES @ {entry_price}c"
            events.append(f"SKIP auto-buy: {msg}")
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = msg
            return events, decision_snapshot, peak_prediction

        target_exit = (
            int(decision_snapshot.target_exit_price_cents)
            if decision_snapshot.target_exit_price_cents is not None
            else min(99, entry_price + AUTO_BUY_TARGET_PROFIT_CENTS + AUTO_BUY_FEE_BUFFER_CENTS)
        )
        decision_snapshot.size_contracts = int(count)
        decision_snapshot.target_exit_price_cents = int(target_exit)
        client_order_id = _build_client_order_id(
            f"{BOT_CLIENT_TAG}autobuy",
            signal.bracket.ticker,
            "YES",
            entry_price,
            count,
        )

        if AUTO_BUY_DRY_RUN:
            self._journal.log_order_event(
                event_type="buy_attempt",
                target_date=target_date,
                ticker=signal.bracket.ticker,
                side="YES",
                qty=count,
                price_cents=entry_price,
                status="dry_run",
                reason=("decision_engine_maker" if maker_mode else "decision_engine_taker"),
                edge=signal.edge,
                confidence=signal.confidence,
                model_prob=signal.model_prob,
                market_prob=signal.market_prob,
                client_order_id=client_order_id,
            )
            mode_label = "MAKER" if maker_mode else "TAKER"
            events.append(
                f"DRY-RUN BUY[{mode_label}] {signal.bracket.ticker} YES qty={count} px={entry_price}c "
                f"(edge={signal.edge:.1%}, conf={signal.confidence:.2f}, lag={decision_snapshot.timing_lag_pp:+.1f}pp, "
                f"pred_high={peak_prediction.predicted_high_f:.1f}F, target_exit~{target_exit}c)"
            )
            return events, decision_snapshot, peak_prediction

        ok, reason = self.contract.place_entry_buy_limit(
            ticker=signal.bracket.ticker,
            side="YES",
            count=count,
            limit_price_cents=entry_price,
            client_order_id=client_order_id,
            prefer_resting=maker_mode,
        )
        if ok:
            self._journal.log_order_event(
                event_type="buy_attempt",
                target_date=target_date,
                ticker=signal.bracket.ticker,
                side="YES",
                qty=count,
                price_cents=entry_price,
                status="submitted",
                reason=reason,
                edge=signal.edge,
                confidence=signal.confidence,
                model_prob=signal.model_prob,
                market_prob=signal.market_prob,
                client_order_id=client_order_id,
            )
            if "duplicate client_order_id" not in reason:
                key = _position_key(signal.bracket.ticker, "YES")
                self._pending_buy_qty[key] = self._pending_buy_qty.get(key, 0) + int(count)
            mode_label = "MAKER" if maker_mode else "TAKER"
            events.append(
                f"SUBMITTED BUY[{mode_label}] {signal.bracket.ticker} YES qty={count} px={entry_price}c "
                f"(target_exit~{target_exit}c, via={reason})"
            )
        else:
            self._journal.log_order_event(
                event_type="buy_attempt",
                target_date=target_date,
                ticker=signal.bracket.ticker,
                side="YES",
                qty=count,
                price_cents=entry_price,
                status="failed",
                reason=reason,
                edge=signal.edge,
                confidence=signal.confidence,
                model_prob=signal.model_prob,
                market_prob=signal.market_prob,
                client_order_id=client_order_id,
            )
            decision_snapshot.status = "SKIP"
            decision_snapshot.reason = reason
            events.append(
                f"FAILED BUY {signal.bracket.ticker} YES qty={count} px={entry_price}c reason={reason}"
            )
        return events, decision_snapshot, peak_prediction

def run_bot(city: str = "NYC", refresh_interval: int = DEFAULT_REFRESH_INTERVAL):
    """Entry point for the bot."""
    bot = WeatherBot(city_code=city, refresh_interval=refresh_interval)
    bot.run()
