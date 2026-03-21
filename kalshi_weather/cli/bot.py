"""
Main Bot Runner.

Orchestrates the data fetching, analysis, and display loop.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from rich.live import Live

from kalshi_weather.config.settings import (
    DEFAULT_REFRESH_INTERVAL,
    CRITICAL_REEVAL_START_MINUTE,
    CRITICAL_REEVAL_END_MINUTE,
    CRITICAL_REEVAL_REFRESH_INTERVAL,
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
)
from kalshi_weather.config import NYC
from kalshi_weather.contracts import HighTempContract
from kalshi_weather.core.models import MarketAnalysis
from kalshi_weather.data.weather import CombinedWeatherSource
from kalshi_weather.data.stations import NWSStationParser
# from kalshi_weather.data.markets import KalshiMarketSource # Assuming this exists or using Contract class
from kalshi_weather.engine.edge_detector import EdgeDetector
from kalshi_weather.engine.calibration import ForecastCalibrator
from kalshi_weather.engine.position_manager import evaluate_open_positions
from kalshi_weather.engine.auto_trader import execute_auto_sells
from kalshi_weather.cli.display import Dashboard
from kalshi_weather.utils.temperature_logs import DailyTemperatureLogger

logger = logging.getLogger(__name__)

class WeatherBot:
    """
    Main bot controller.
    """

    def __init__(self, city_code: str = "NYC", refresh_interval: int = DEFAULT_REFRESH_INTERVAL):
        self.city_code = city_code
        self.refresh_interval = refresh_interval
        self.dashboard = Dashboard()
        self.edge_detector = EdgeDetector()
        
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
        self._non_primary_streaks: dict[str, int] = {}
        self._position_first_seen_at: dict[str, datetime] = {}

    def run(self):
        """Start the main loop."""
        
        with Live(self.dashboard.layout, refresh_per_second=4, screen=True) as live:
            while True:
                try:
                    analysis = self.perform_analysis()
                    self.temperature_logger.append_snapshot(analysis)
                    self.dashboard.update(analysis)
                    
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
        now = datetime.now()
        if self._in_critical_reeval_window(now):
            return max(1, min(self.refresh_interval, CRITICAL_REEVAL_REFRESH_INTERVAL))
        return max(1, min(self.refresh_interval, self._seconds_until_next_critical_window(now)))

    def perform_analysis(self) -> MarketAnalysis:
        """Run one full analysis cycle."""
        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        # Prefer today's market. If absent, fall back to tomorrow.
        target_date = today
        brackets = self.contract.fetch_brackets(target_date)
        if not brackets:
            logger.warning(
                "No market brackets found for %s; falling back to %s",
                target_date,
                tomorrow,
            )
            target_date = tomorrow
            brackets = self.contract.fetch_brackets(target_date)

        # 1. Fetch Forecasts
        forecasts = self.contract.fetch_forecasts(target_date)
        # Update rolling forecast-error calibration store (best effort).
        try:
            self.calibrator.record_forecast_snapshot(forecasts, target_date=target_date)
            self.calibrator.refresh_settlement_errors()
        except Exception as exc:
            logger.warning("Forecast calibration update skipped: %s", exc)
        
        # 2. Fetch Observations
        # The station source needs to implement `get_daily_summary`
        observation = self.station_source.get_daily_summary(target_date)
        
        # 4. Run Edge Detection
        signals = self.edge_detector.analyze(
            forecasts=forecasts,
            observation=observation,
            brackets=brackets,
            min_edge=0.05,
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

        combined = combine_forecasts(forecasts) if forecasts else None
        adjusted = None
        tomorrow_forecasts = self.contract.fetch_forecasts(tomorrow)
        tomorrow_combined = combine_forecasts(tomorrow_forecasts) if tomorrow_forecasts else None
        model_probabilities: dict[str, float] = {}
        if combined and brackets:
            adjusted = adjust_forecast_with_observations(combined, observation)
            bracket_probs = BracketProbabilityCalculator().calculate_all_probabilities(
                brackets,
                adjusted.mean_temp_f,
                adjusted.std_dev,
                lower_bound=adjusted.conditioning_cutoff_f,
                upper_bound=adjusted.conditioning_ceiling_f,
            )
            model_probabilities = {bp.bracket.ticker: bp.model_prob for bp in bracket_probs}
        else:
            logger.warning(
                "Model probability update skipped (combined=%s, brackets=%s)",
                bool(combined),
                bool(brackets),
            )
        open_positions = self.contract.fetch_open_positions()
        active_event_ticker = brackets[0].event_ticker if brackets else None
        portfolio_totals = self.contract.fetch_portfolio_totals(
            event_ticker=active_event_ticker
        )
        now_local = datetime.now(self._market_tz)
        current_qty: dict[str, int] = {
            f"{p.ticker}|{p.side.upper()}": p.contracts for p in open_positions
        }
        active_keys = set(current_qty.keys())
        for key in list(self._position_first_seen_at.keys()):
            if key not in active_keys:
                self._position_first_seen_at.pop(key, None)
        for key in active_keys:
            self._position_first_seen_at.setdefault(key, now_local)
        position_age_minutes: dict[str, float] = {}
        for key, ts in self._position_first_seen_at.items():
            age_min = max(0.0, (now_local - ts).total_seconds() / 60.0)
            position_age_minutes[key] = age_min
        lifecycle_events: list[str] = []
        for key, prev_qty in self._previous_open_position_qty.items():
            cur_qty = current_qty.get(key, 0)
            ticker, side = key.split("|", 1)
            if cur_qty <= 0 and prev_qty > 0:
                lifecycle_events.append(f"SOLD {ticker} {side} qty={prev_qty}")
            elif cur_qty < prev_qty:
                lifecycle_events.append(
                    f"REDUCED {ticker} {side} qty={prev_qty} -> {cur_qty}"
                )

        position_recommendations = evaluate_open_positions(
            positions=open_positions,
            model_probabilities=model_probabilities,
            previous_model_probabilities=self._previous_model_probabilities,
            exit_fee_rate=POSITION_EXIT_FEE_RATE,
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
            enabled=AUTO_SELL_ENABLED,
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
        )
        self._previous_model_probabilities = model_probabilities
        self._previous_open_position_qty = current_qty

        display_mean = adjusted.mean_temp_f if adjusted else (combined.mean_temp_f if combined else 0.0)
        display_std = adjusted.std_dev if adjusted else (combined.std_dev if combined else 0.0)

        return MarketAnalysis(
            city=self.city_config.name,
            target_date=target_date,
            forecasts=forecasts,
            observation=observation,
            brackets=brackets,
            signals=signals,
            forecast_mean=display_mean,
            forecast_std=display_std,
            analyzed_at=datetime.now(),
            raw_forecast_mean=combined.mean_temp_f if combined else None,
            raw_forecast_std=combined.std_dev if combined else None,
            adjusted_forecast_mean=adjusted.mean_temp_f if adjusted else None,
            adjusted_forecast_std=adjusted.std_dev if adjusted else None,
            tomorrow_date=tomorrow,
            tomorrow_forecast_mean=tomorrow_combined.mean_temp_f if tomorrow_combined else None,
            model_probabilities=model_probabilities,
            open_positions=position_recommendations,
            auto_trader_events=auto_events,
            position_lifecycle_events=lifecycle_events,
            trajectory_assessment=adjusted.trajectory_assessment if adjusted else None,
            portfolio_totals=portfolio_totals,
        )

def run_bot(city: str = "NYC", refresh_interval: int = DEFAULT_REFRESH_INTERVAL):
    """Entry point for the bot."""
    bot = WeatherBot(city_code=city, refresh_interval=refresh_interval)
    bot.run()
