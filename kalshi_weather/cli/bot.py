"""
Main Bot Runner.

Orchestrates the data fetching, analysis, and display loop.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Optional

from rich.live import Live

from kalshi_weather.config.settings import DEFAULT_REFRESH_INTERVAL
from kalshi_weather.config import NYC
from kalshi_weather.contracts import HighTempContract
from kalshi_weather.core.models import MarketAnalysis
from kalshi_weather.data.weather import CombinedWeatherSource
from kalshi_weather.data.stations import NWSStationParser
# from kalshi_weather.data.markets import KalshiMarketSource # Assuming this exists or using Contract class
from kalshi_weather.engine.edge_detector import EdgeDetector
from kalshi_weather.cli.display import Dashboard

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

        # Initialize Data Sources
        self.weather_source = CombinedWeatherSource(city=self.city_config)
        self.station_source = NWSStationParser(city=self.city_config)
        self.contract = HighTempContract(self.city_config) 

    def run(self):
        """Start the main loop."""
        
        with Live(self.dashboard.layout, refresh_per_second=4, screen=True) as live:
            while True:
                try:
                    analysis = self.perform_analysis()
                    self.dashboard.update(analysis)
                    
                    # Sleep with countdown? Or just sleep.
                    # For a responsive UI, better to sleep in short chunks or just blocking sleep is fine for now.
                    time.sleep(self.refresh_interval)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.exception("Error in main loop")
                    # TODO: Show error in dashboard footer
                    time.sleep(10) # Retry delay

    def perform_analysis(self) -> MarketAnalysis:
        """Run one full analysis cycle."""
        target_date = datetime.now().strftime("%Y-%m-%d") # Today
        # Or should it be tomorrow if market closed?
        # For now, assume trading today's high.
        
        # 1. Fetch Forecasts
        forecasts = self.contract.fetch_forecasts(target_date)
        
        # 2. Fetch Observations
        # The station source needs to implement `get_daily_summary`
        observation = self.station_source.get_daily_summary(target_date)
        
        # 3. Fetch Market Brackets
        brackets = self.contract.fetch_brackets(target_date)
        
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

        combined = combine_forecasts(forecasts)
        adjusted = adjust_forecast_with_observations(combined, observation)
        bracket_probs = BracketProbabilityCalculator().calculate_all_probabilities(
            brackets,
            adjusted.mean_temp_f,
            adjusted.std_dev,
            lower_bound=adjusted.conditioning_cutoff_f,
            upper_bound=adjusted.conditioning_ceiling_f,
        )
        model_probabilities = {bp.bracket.ticker: bp.model_prob for bp in bracket_probs}

        return MarketAnalysis(
            city=self.city_config.name,
            target_date=target_date,
            forecasts=forecasts,
            observation=observation,
            brackets=brackets,
            signals=signals,
            forecast_mean=adjusted.mean_temp_f,
            forecast_std=adjusted.std_dev,
            analyzed_at=datetime.now(),
            model_probabilities=model_probabilities,
            trajectory_assessment=adjusted.trajectory_assessment,
        )

def run_bot(city: str = "NYC"):
    """Entry point for the bot."""
    bot = WeatherBot(city_code=city)
    bot.run()
