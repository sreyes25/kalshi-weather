"""
Probability engine for Kalshi Weather Bot.

Contains modules for combining forecasts, adjusting for observations,
and calculating bracket probabilities.
"""

from kalshi_weather.engine.probability import (
    # Module 2A: Forecast Combiner
    ForecastCombiner,
    CombinedForecast,
    combine_forecasts,
    DEFAULT_WEIGHTS,
    MIN_STD_DEV,
    # Module 2B: Observation Adjuster
    ObservationAdjuster,
    AdjustedForecast,
    adjust_forecast_with_observations,
    EARLY_CUTOFF_HOURS,
    LATE_CUTOFF_HOURS,
    MAX_OBSERVATION_WEIGHT,
    # Module 2C: Bracket Probability Calculator
    BracketProbabilityCalculator,
    BracketProbability,
    calculate_bracket_probabilities,
    normal_cdf,
)
from kalshi_weather.engine.trajectory import TrajectoryEngine

__all__ = [
    # Module 2A
    "ForecastCombiner",
    "CombinedForecast",
    "combine_forecasts",
    "DEFAULT_WEIGHTS",
    "MIN_STD_DEV",
    # Module 2B
    "ObservationAdjuster",
    "AdjustedForecast",
    "adjust_forecast_with_observations",
    "EARLY_CUTOFF_HOURS",
    "LATE_CUTOFF_HOURS",
    "MAX_OBSERVATION_WEIGHT",
    # Module 2C
    "BracketProbabilityCalculator",
    "BracketProbability",
    "calculate_bracket_probabilities",
    "normal_cdf",
    "TrajectoryEngine",
]
