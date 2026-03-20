"""
Probability Engine

Module 2A: Forecast Combiner
- Combines multiple weather forecasts into a single probability distribution
- Uses weighted averaging with proper uncertainty propagation

Module 2B: Observation Adjuster
- Adjusts the combined forecast based on observed temperatures
- Time-based weighting: early day uses forecast, late day uses observations

Module 2C: Bracket Probability Calculator
- Calculates probability for each market bracket using normal CDF
- Handles boundary logic for BETWEEN, GREATER_THAN, LESS_THAN brackets
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from scipy.stats import norm

from kalshi_weather.core import (
    TemperatureForecast,
    DailyObservation,
    MarketBracket,
    BracketType,
    TrajectoryAssessment,
)
from kalshi_weather.data.stations import get_recent_observation_history
from kalshi_weather.engine.trajectory import TrajectoryEngine

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Default weights by source (higher = more trusted)
# These are relative weights, will be normalized during combination
DEFAULT_WEIGHTS: Dict[str, float] = {
    # NWS is the settlement source, highest weight
    "NWS": 5.0,
    # ECMWF is generally considered the best global model
    "ECMWF": 4.0,
    # Open-Meteo Best Match blends multiple models intelligently
    "Open-Meteo Best Match": 3.5,
    # GFS+HRRR combination - good for short-term US forecasts
    "GFS+HRRR": 3.0,
    "GFS": 2.5,
    "HRRR": 2.5,
    # Ensemble provides uncertainty info but individual mean less reliable
    "Open-Meteo Ensemble": 2.0,
    # Generic fallback for unknown sources
    "default": 1.0,
}

# Minimum standard deviation floor (in Fahrenheit)
# Even the best forecasts have at least this much uncertainty
MIN_STD_DEV: float = 1.5

# Maximum standard deviation cap (sanity check)
MAX_STD_DEV: float = 10.0


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class CombinedForecast:
    """
    Result of combining multiple forecasts into a single distribution.

    Represents the final probability distribution for temperature,
    modeled as a normal distribution with mean and std_dev.
    """
    target_date: str                    # YYYY-MM-DD format
    mean_temp_f: float                  # Combined weighted mean temperature
    std_dev: float                      # Combined uncertainty (std deviation)
    low_f: float                        # 10th percentile estimate
    high_f: float                       # 90th percentile estimate
    source_count: int                   # Number of forecasts combined
    sources_used: List[str]             # Names of sources included
    weights_used: Dict[str, float]      # Normalized weights applied
    individual_forecasts: List[TemperatureForecast] = field(default_factory=list)
    combined_at: datetime = field(default_factory=datetime.now)

    @property
    def variance(self) -> float:
        """Return variance (std_dev squared)."""
        return self.std_dev ** 2


# =============================================================================
# FORECAST COMBINER
# =============================================================================


class ForecastCombiner:
    """
    Combines multiple weather forecasts into a single probability distribution.

    Uses weighted averaging with uncertainty propagation that accounts for:
    1. Individual forecast uncertainty (each source's std_dev)
    2. Disagreement variance (how much sources disagree with each other)

    The combined variance is:
        σ² = Σ(w_i * σ_i²) / Σ(w_i) + Σ(w_i * (x_i - μ)²) / Σ(w_i)

    Where:
        - First term is the weighted pooled variance (individual uncertainties)
        - Second term is the disagreement variance (forecast spread)
    """

    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        min_std_dev: float = MIN_STD_DEV,
        max_std_dev: float = MAX_STD_DEV,
    ):
        """
        Initialize the forecast combiner.

        Args:
            weights: Custom weight dictionary mapping source names to weights.
                    If None, uses DEFAULT_WEIGHTS.
            min_std_dev: Minimum standard deviation floor (prevents overconfidence).
            max_std_dev: Maximum standard deviation cap (sanity check).
        """
        self.weights = weights or DEFAULT_WEIGHTS.copy()
        self.min_std_dev = min_std_dev
        self.max_std_dev = max_std_dev

    def get_weight(self, source: str) -> float:
        """
        Get weight for a forecast source.

        Tries exact match first, then partial match, then default.
        """
        # Exact match
        if source in self.weights:
            return self.weights[source]

        # Partial match (case-insensitive)
        source_lower = source.lower()
        for key, weight in self.weights.items():
            if key.lower() in source_lower or source_lower in key.lower():
                return weight

        # Default fallback
        return self.weights.get("default", 1.0)

    def combine(self, forecasts: List[TemperatureForecast]) -> Optional[CombinedForecast]:
        """
        Combine multiple forecasts into a single probability distribution.

        Args:
            forecasts: List of temperature forecasts to combine.

        Returns:
            CombinedForecast with weighted mean and combined uncertainty,
            or None if no valid forecasts provided.
        """
        if not forecasts:
            logger.warning("No forecasts to combine")
            return None

        # Filter out forecasts with invalid data
        valid_forecasts = [
            f for f in forecasts
            if f.forecast_temp_f is not None and not math.isnan(f.forecast_temp_f)
        ]

        if not valid_forecasts:
            logger.warning("No valid forecasts after filtering")
            return None

        # Get weights for each forecast
        weights = [self.get_weight(f.source) for f in valid_forecasts]
        total_weight = sum(weights)

        # Normalize weights
        normalized_weights = [w / total_weight for w in weights]

        # Calculate weighted mean
        weighted_mean = sum(
            w * f.forecast_temp_f
            for w, f in zip(normalized_weights, valid_forecasts)
        )

        # Calculate combined variance using two components:
        # 1. Pooled variance (weighted average of individual variances)
        pooled_variance = sum(
            w * (f.std_dev ** 2)
            for w, f in zip(normalized_weights, valid_forecasts)
        )

        # 2. Disagreement variance (weighted variance of forecast means)
        disagreement_variance = sum(
            w * ((f.forecast_temp_f - weighted_mean) ** 2)
            for w, f in zip(normalized_weights, valid_forecasts)
        )

        # Combined variance is sum of both components
        combined_variance = pooled_variance + disagreement_variance
        combined_std_dev = math.sqrt(combined_variance)

        # Apply floor and ceiling
        combined_std_dev = max(self.min_std_dev, min(self.max_std_dev, combined_std_dev))

        # Calculate percentiles (assuming normal distribution)
        # 10th percentile: mean - 1.28 * std_dev
        # 90th percentile: mean + 1.28 * std_dev
        z_10 = 1.28155  # More precise z-score for 10th/90th percentile
        low_f = weighted_mean - z_10 * combined_std_dev
        high_f = weighted_mean + z_10 * combined_std_dev

        # Build weights used dict
        weights_used = {
            f.source: w for f, w in zip(valid_forecasts, normalized_weights)
        }

        target_date = valid_forecasts[0].target_date

        logger.info(
            f"Combined {len(valid_forecasts)} forecasts: "
            f"mean={weighted_mean:.1f}°F, std={combined_std_dev:.2f}°F"
        )

        return CombinedForecast(
            target_date=target_date,
            mean_temp_f=weighted_mean,
            std_dev=combined_std_dev,
            low_f=low_f,
            high_f=high_f,
            source_count=len(valid_forecasts),
            sources_used=[f.source for f in valid_forecasts],
            weights_used=weights_used,
            individual_forecasts=valid_forecasts,
        )

    def combine_with_custom_weights(
        self,
        forecasts: List[TemperatureForecast],
        custom_weights: Dict[str, float],
    ) -> Optional[CombinedForecast]:
        """
        Combine forecasts using custom weights for this call only.

        Useful for sensitivity analysis or testing different weight schemes.
        """
        original_weights = self.weights
        self.weights = custom_weights
        try:
            return self.combine(forecasts)
        finally:
            self.weights = original_weights


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================


def combine_forecasts(
    forecasts: List[TemperatureForecast],
    weights: Optional[Dict[str, float]] = None,
) -> Optional[CombinedForecast]:
    """
    Convenience function to combine forecasts with default settings.

    Args:
        forecasts: List of temperature forecasts to combine.
        weights: Optional custom weights. Uses DEFAULT_WEIGHTS if None.

    Returns:
        CombinedForecast or None if no valid forecasts.
    """
    combiner = ForecastCombiner(weights=weights)
    return combiner.combine(forecasts)


# =============================================================================
# MODULE 2B: OBSERVATION ADJUSTER
# =============================================================================

# Time-based weight thresholds (hours after noon in local time)
EARLY_CUTOFF_HOURS = 2.0   # Before 2 PM: mostly forecast
LATE_CUTOFF_HOURS = 4.0    # After 4 PM: mostly observation
MAX_OBSERVATION_WEIGHT = 0.95  # Never fully trust observations (measurement error)

# Observation uncertainty floor
MIN_OBSERVATION_STD = 0.5  # Minimum std dev when using observations


@dataclass
class AdjustedForecast:
    """
    Result of adjusting a combined forecast with observations.

    This represents our best estimate of the daily high temperature
    distribution, incorporating both forecast data and real-time observations.
    """
    target_date: str                    # YYYY-MM-DD format
    mean_temp_f: float                  # Adjusted mean temperature
    std_dev: float                      # Adjusted uncertainty
    low_f: float                        # 10th percentile estimate
    high_f: float                       # 90th percentile estimate

    # Original inputs
    original_forecast: CombinedForecast  # The unadjusted forecast
    observation: Optional[DailyObservation]  # Observation data used (if any)

    # Adjustment details
    observation_weight: float           # Weight given to observation (0-1)
    forecast_weight: float              # Weight given to forecast (0-1)
    hours_since_noon: float             # Hours past noon when adjusted
    observed_high_f: Optional[float]    # Observed high used in adjustment

    # Constraint info
    min_possible_high: float            # High can't be below this
    max_possible_high: float            # High unlikely to exceed this
    conditioning_cutoff_f: Optional[float] = None  # Lower support bound for final-high distribution
    conditioning_ceiling_f: Optional[float] = None  # Optional upper support bound in lock mode
    latest_temp_f: Optional[float] = None
    lock_mode_active: bool = False
    trajectory_assessment: Optional[TrajectoryAssessment] = None

    adjusted_at: datetime = field(default_factory=datetime.now)

    @property
    def variance(self) -> float:
        """Return variance (std_dev squared)."""
        return self.std_dev ** 2

    @property
    def is_observation_dominant(self) -> bool:
        """True if observations are weighted more than forecasts."""
        return self.observation_weight > 0.5


class ObservationAdjuster:
    """
    Adjusts forecast distributions based on observed temperatures.

    Uses time-based weighting:
    - Before 2 PM: Rely mostly on forecast (high hasn't occurred yet)
    - 2 PM - 4 PM: Blend forecast and observations
    - After 4 PM: Rely mostly on observations (high likely already occurred)

    Also accounts for station measurement uncertainty and constrains
    the distribution based on what's already been observed.
    """

    def __init__(
        self,
        timezone: ZoneInfo = None,
        min_std_dev: float = MIN_STD_DEV,
        max_observation_weight: float = MAX_OBSERVATION_WEIGHT,
    ):
        """
        Initialize the observation adjuster.

        Args:
            timezone: Timezone for determining local time (default: America/New_York)
            min_std_dev: Minimum standard deviation floor
            max_observation_weight: Maximum weight for observations (< 1.0)
        """
        self.timezone = timezone or ZoneInfo("America/New_York")
        self.min_std_dev = min_std_dev
        self.max_observation_weight = max_observation_weight
        self.trajectory_engine = TrajectoryEngine(self.timezone)

    def _calculate_hours_since_noon(self, current_time: Optional[datetime] = None) -> float:
        """Calculate hours elapsed since noon in local timezone."""
        if current_time is None:
            current_time = datetime.now(self.timezone)
        elif current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=self.timezone)
        else:
            current_time = current_time.astimezone(self.timezone)

        # Hours since midnight
        hours = current_time.hour + current_time.minute / 60.0

        # Hours since noon (can be negative before noon)
        return hours - 12.0

    def _calculate_observation_weight(self, hours_since_noon: float) -> float:
        """
        Calculate weight to give observations based on time of day.

        Returns a value between 0 and max_observation_weight.

        Weight curve:
        - Before noon: 0 (forecast only)
        - Noon to 2 PM: 0 to 0.3 (linear ramp)
        - 2 PM to 4 PM: 0.3 to 0.8 (steeper ramp - peak heating hours)
        - After 4 PM: 0.8 to max (gradual increase)
        """
        if hours_since_noon <= 0:
            # Before noon - pure forecast
            return 0.0

        if hours_since_noon < EARLY_CUTOFF_HOURS:
            # Noon to 2 PM: gradual ramp from 0 to 0.3
            return 0.15 * hours_since_noon

        if hours_since_noon < LATE_CUTOFF_HOURS:
            # 2 PM to 4 PM: steeper ramp from 0.3 to 0.8
            progress = (hours_since_noon - EARLY_CUTOFF_HOURS) / (LATE_CUTOFF_HOURS - EARLY_CUTOFF_HOURS)
            return 0.3 + 0.5 * progress

        # After 4 PM: gradual approach to max
        # Takes ~4 more hours to reach max_observation_weight
        extra_hours = hours_since_noon - LATE_CUTOFF_HOURS
        progress = min(extra_hours / 4.0, 1.0)
        weight = 0.8 + (self.max_observation_weight - 0.8) * progress

        return min(weight, self.max_observation_weight)

    def _calculate_adjusted_mean(
        self,
        forecast_mean: float,
        observed_high: float,
        observation_weight: float,
    ) -> float:
        """
        Calculate adjusted mean by blending forecast and observation.

        The adjusted mean is constrained to be at least the observed high
        (since the actual high can't be lower than what we've already seen).
        """
        forecast_weight = 1.0 - observation_weight
        blended_mean = forecast_weight * forecast_mean + observation_weight * observed_high

        # The high temperature can't be lower than what we've observed
        return max(blended_mean, observed_high)

    def _calculate_adjusted_std_dev(
        self,
        forecast_std: float,
        observation: DailyObservation,
        observation_weight: float,
        adjusted_mean: float,
    ) -> float:
        """
        Calculate adjusted standard deviation.

        Late in the day, uncertainty shrinks because:
        1. We've observed most of the day's temperatures
        2. The remaining time has limited potential for change

        We also incorporate station measurement uncertainty.
        """
        # Station uncertainty range
        station_uncertainty = (
            observation.possible_actual_high_high - observation.possible_actual_high_low
        ) / 2.0

        # As observation weight increases, std dev shrinks toward station uncertainty
        forecast_weight = 1.0 - observation_weight

        # Blend uncertainties
        # Higher observation weight = more confidence = lower std dev
        if observation_weight > 0.5:
            # Late day: uncertainty mainly from station measurement
            blended_std = (
                forecast_weight * forecast_std +
                observation_weight * station_uncertainty
            )
        else:
            # Early/mid day: forecast uncertainty dominates but shrinks somewhat
            # if observed high is close to forecast mean
            temp_diff = abs(adjusted_mean - observation.observed_high_f)
            agreement_factor = max(0.8, 1.0 - temp_diff / 20.0)
            blended_std = forecast_std * agreement_factor

        return max(self.min_std_dev, blended_std)

    def adjust(
        self,
        combined_forecast: CombinedForecast,
        observation: Optional[DailyObservation] = None,
        current_time: Optional[datetime] = None,
    ) -> AdjustedForecast:
        """
        Adjust a combined forecast using observation data.

        Args:
            combined_forecast: The combined forecast to adjust
            observation: Daily observation data (optional)
            current_time: Current time for weight calculation (default: now)

        Returns:
            AdjustedForecast with blended mean and adjusted uncertainty
        """
        hours_since_noon = self._calculate_hours_since_noon(current_time)

        # If no observation data, return forecast unchanged (wrapped in AdjustedForecast)
        if observation is None or not observation.readings:
            z_10 = 1.28155
            return AdjustedForecast(
                target_date=combined_forecast.target_date,
                mean_temp_f=combined_forecast.mean_temp_f,
                std_dev=combined_forecast.std_dev,
                low_f=combined_forecast.low_f,
                high_f=combined_forecast.high_f,
                original_forecast=combined_forecast,
                observation=None,
                observation_weight=0.0,
                forecast_weight=1.0,
                hours_since_noon=hours_since_noon,
                observed_high_f=None,
                min_possible_high=combined_forecast.low_f,
                max_possible_high=combined_forecast.high_f,
                conditioning_cutoff_f=None,
                conditioning_ceiling_f=None,
                latest_temp_f=None,
                lock_mode_active=False,
                trajectory_assessment=None,
            )

        # Calculate time-based weight
        observation_weight = self._calculate_observation_weight(hours_since_noon)
        forecast_weight = 1.0 - observation_weight

        # Calculate adjusted mean
        adjusted_mean = self._calculate_adjusted_mean(
            combined_forecast.mean_temp_f,
            observation.observed_high_f,
            observation_weight,
        )

        # Calculate adjusted std dev
        adjusted_std = self._calculate_adjusted_std_dev(
            combined_forecast.std_dev,
            observation,
            observation_weight,
            adjusted_mean,
        )

        # Calculate percentiles
        z_10 = 1.28155
        low_f = adjusted_mean - z_10 * adjusted_std
        high_f = adjusted_mean + z_10 * adjusted_std

        # Constrain low_f - can't be below observation bounds
        min_possible = observation.possible_actual_high_low
        low_f = max(low_f, min_possible)

        # Constrain max - use observation upper bound late in day
        if observation_weight > 0.7:
            max_possible = observation.possible_actual_high_high + adjusted_std
        else:
            max_possible = max(high_f, observation.possible_actual_high_high)

        assessment_time = current_time or datetime.now(self.timezone)
        if assessment_time.tzinfo is None:
            assessment_time = assessment_time.replace(tzinfo=self.timezone)
        else:
            assessment_time = assessment_time.astimezone(self.timezone)
        recent_history = get_recent_observation_history(
            readings=observation.readings,
            timezone=self.timezone,
            current_time=assessment_time,
            window_size=6,
            max_age_hours=4.0,
        )

        trajectory = self.trajectory_engine.assess(
            current_time=assessment_time,
            observed_high_f=observation.observed_high_f,
            recent_readings=recent_history,
            combined_forecast=combined_forecast,
        )

        latest_temp = recent_history[-1].reported_temp_f if recent_history else observation.observed_high_f
        latest_time = recent_history[-1].timestamp.astimezone(self.timezone) if recent_history else None
        latest_age_hours = (
            (assessment_time - latest_time).total_seconds() / 3600.0
            if latest_time is not None
            else float("inf")
        )
        stale_observation_window = latest_age_hours > 2.0
        lock_mode = (
            hours_since_noon >= LATE_CUTOFF_HOURS
            and observation_weight >= 0.65
            and latest_temp < observation.observed_high_f
            and trajectory.trend_f_per_hour <= 0.0
            and trajectory.lock_confidence >= 0.75
            and not stale_observation_window
        )
        conditioning_cutoff = observation.observed_high_f - 0.5
        conditioning_ceiling = None
        if lock_mode:
            conditioning_ceiling = observation.observed_high_f + max(
                1.4,
                trajectory.expected_remaining_warming_f + 0.9,
            )

        # Trajectory conditioning: late-day lock collapses around observed high,
        # while preserving a right tail when trajectory supports remaining upside.
        target_mean = observation.observed_high_f + trajectory.expected_remaining_warming_f
        if lock_mode:
            target_mean = observation.observed_high_f + min(
                0.3,
                0.25 * trajectory.expected_remaining_warming_f,
            )

        # Keep trajectory influence weak early, and increase as the day advances.
        time_progress = max(0.0, min(1.0, hours_since_noon / 8.0))
        blend_weight = (0.05 + 0.75 * trajectory.lock_confidence) * time_progress
        if lock_mode:
            blend_weight = max(0.75, blend_weight)
        elif stale_observation_window:
            # Avoid overreacting when recent station history is stale.
            blend_weight *= 0.35

        conditioned_mean = (1.0 - blend_weight) * adjusted_mean + blend_weight * target_mean
        conditioned_mean = max(observation.observed_high_f, conditioned_mean)

        std_scale = 1.0 - 0.50 * blend_weight
        if lock_mode:
            std_scale *= 0.70
        conditioned_std = max(self.min_std_dev, adjusted_std * max(0.35, std_scale))
        low_f = max(conditioning_cutoff, conditioned_mean - z_10 * conditioned_std)
        high_f = conditioned_mean + z_10 * conditioned_std
        max_possible = max(max_possible, observation.observed_high_f + trajectory.expected_remaining_warming_f + conditioned_std)

        logger.info(
            f"Adjusted forecast: mean={conditioned_mean:.1f}°F (was {combined_forecast.mean_temp_f:.1f}°F), "
            f"std={conditioned_std:.2f}°F, obs_weight={observation_weight:.2f}, "
            f"lock={trajectory.lock_confidence:.2f}, lock_mode={lock_mode}, stale_obs={stale_observation_window}"
        )

        return AdjustedForecast(
            target_date=combined_forecast.target_date,
            mean_temp_f=conditioned_mean,
            std_dev=conditioned_std,
            low_f=low_f,
            high_f=high_f,
            original_forecast=combined_forecast,
            observation=observation,
            observation_weight=observation_weight,
            forecast_weight=forecast_weight,
            hours_since_noon=hours_since_noon,
            observed_high_f=observation.observed_high_f,
            min_possible_high=min_possible,
            max_possible_high=max_possible,
            conditioning_cutoff_f=conditioning_cutoff,
            conditioning_ceiling_f=conditioning_ceiling,
            latest_temp_f=latest_temp,
            lock_mode_active=lock_mode,
            trajectory_assessment=trajectory,
        )


def adjust_forecast_with_observations(
    combined_forecast: CombinedForecast,
    observation: Optional[DailyObservation] = None,
    timezone: ZoneInfo = None,
    current_time: Optional[datetime] = None,
) -> AdjustedForecast:
    """
    Convenience function to adjust a forecast with observations.

    Args:
        combined_forecast: The combined forecast to adjust
        observation: Daily observation data (optional)
        timezone: Local timezone (default: America/New_York)
        current_time: Current time override for testing

    Returns:
        AdjustedForecast with blended distribution
    """
    adjuster = ObservationAdjuster(timezone=timezone)
    return adjuster.adjust(combined_forecast, observation, current_time)


# =============================================================================
# MODULE 2C: BRACKET PROBABILITY CALCULATOR
# =============================================================================

@dataclass
class BracketProbability:
    """
    Calculated probability for a single market bracket.

    Contains both the model probability and comparison with market price.
    """
    bracket: MarketBracket           # The market bracket
    model_prob: float                # Our calculated probability (0.0 to 1.0)
    market_prob: float               # Market implied probability (0.0 to 1.0)
    edge: float                      # model_prob - market_prob
    edge_pct: float                  # Edge as percentage points

    @property
    def has_positive_edge(self) -> bool:
        """True if model probability exceeds market probability."""
        return self.edge > 0

    @property
    def edge_direction(self) -> str:
        """Returns 'YES' if we should buy YES, 'NO' if we should buy NO."""
        return "YES" if self.edge > 0 else "NO"


def normal_cdf(x: float, mean: float, std_dev: float) -> float:
    """
    Calculate the cumulative distribution function of a normal distribution.

    Uses scipy.stats.norm.cdf.

    Args:
        x: The value to evaluate
        mean: Distribution mean
        std_dev: Distribution standard deviation

    Returns:
        Probability that a random variable is less than or equal to x
    """
    if std_dev <= 0:
        # Degenerate case: all probability mass at mean
        return 1.0 if x >= mean else 0.0

    return float(norm.cdf(x, loc=mean, scale=std_dev))


class BracketProbabilityCalculator:
    """
    Calculates probability for each market bracket using normal distribution CDF.

    Bracket boundary logic (Kalshi settlement rules):
    - BETWEEN [lower, upper]: temp in {lower, lower+1, ..., upper} wins
      Formula: CDF(upper + 0.5) - CDF(lower - 0.5)

    - GREATER_THAN threshold: temp in {threshold+1, threshold+2, ...} wins
      Formula: 1 - CDF(threshold + 0.5)

    - LESS_THAN threshold: temp in {..., threshold-2, threshold-1} wins
      Formula: CDF(threshold - 0.5)

    The 0.5 adjustments handle the discrete nature of temperature readings
    (whole degrees Fahrenheit) within a continuous normal distribution.
    """

    def __init__(self, min_prob: float = 0.001, max_prob: float = 0.999):
        """
        Initialize the calculator.

        Args:
            min_prob: Minimum probability floor (avoids 0%)
            max_prob: Maximum probability ceiling (avoids 100%)
        """
        self.min_prob = min_prob
        self.max_prob = max_prob

    def _clamp_probability(self, prob: float) -> float:
        """Clamp probability to valid range."""
        return max(self.min_prob, min(self.max_prob, prob))

    def calculate_bracket_probability(
        self,
        bracket: MarketBracket,
        mean: float,
        std_dev: float,
    ) -> float:
        """
        Calculate probability for a single bracket.

        Args:
            bracket: The market bracket
            mean: Temperature distribution mean
            std_dev: Temperature distribution standard deviation

        Returns:
            Probability (0.0 to 1.0) that temperature falls in this bracket
        """
        if bracket.bracket_type == BracketType.BETWEEN:
            # BETWEEN [lower, upper]: inclusive both ends
            # P(lower <= T <= upper) = CDF(upper + 0.5) - CDF(lower - 0.5)
            lower = bracket.lower_bound
            upper = bracket.upper_bound
            prob = (
                normal_cdf(upper + 0.5, mean, std_dev) -
                normal_cdf(lower - 0.5, mean, std_dev)
            )

        elif bracket.bracket_type == BracketType.GREATER_THAN:
            # GREATER_THAN threshold: strictly greater (threshold does NOT win)
            # P(T > threshold) = 1 - CDF(threshold + 0.5)
            threshold = bracket.lower_bound
            prob = 1.0 - normal_cdf(threshold + 0.5, mean, std_dev)

        elif bracket.bracket_type == BracketType.LESS_THAN:
            # LESS_THAN threshold: strictly less (threshold does NOT win)
            # P(T < threshold) = CDF(threshold - 0.5)
            threshold = bracket.upper_bound
            prob = normal_cdf(threshold - 0.5, mean, std_dev)

        else:
            logger.warning(f"Unknown bracket type: {bracket.bracket_type}")
            prob = 0.0

        return self._clamp_probability(prob)

    def calculate_all_probabilities(
        self,
        brackets: List[MarketBracket],
        mean: float,
        std_dev: float,
        lower_bound: Optional[float] = None,
        upper_bound: Optional[float] = None,
    ) -> List[BracketProbability]:
        """
        Calculate probabilities for all brackets.

        Args:
            brackets: List of market brackets
            mean: Temperature distribution mean
            std_dev: Temperature distribution standard deviation

        Returns:
            List of BracketProbability objects with model vs market comparison
        """
        results = []

        for bracket in brackets:
            model_prob = self.calculate_bracket_probability(
                bracket,
                mean,
                std_dev,
            )
            if lower_bound is not None or upper_bound is not None:
                model_prob = self._condition_to_bounds(
                    bracket=bracket,
                    mean=mean,
                    std_dev=std_dev,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                )
            market_prob = bracket.implied_prob
            edge = model_prob - market_prob

            results.append(BracketProbability(
                bracket=bracket,
                model_prob=model_prob,
                market_prob=market_prob,
                edge=edge,
                edge_pct=edge * 100,
            ))

        if results and self._is_exhaustive_structure(brackets):
            total = sum(bp.model_prob for bp in results)
            if total > 0:
                for bp in results:
                    bp.model_prob = max(0.0, min(1.0, bp.model_prob / total))
                    bp.edge = bp.model_prob - bp.market_prob
                    bp.edge_pct = bp.edge * 100

        # Log summary
        total_model_prob = sum(bp.model_prob for bp in results)
        logger.info(
            f"Calculated probabilities for {len(brackets)} brackets: "
            f"total_model_prob={total_model_prob:.1%}"
        )

        return results

    def _condition_to_bounds(
        self,
        bracket: MarketBracket,
        mean: float,
        std_dev: float,
        lower_bound: Optional[float],
        upper_bound: Optional[float],
    ) -> float:
        """
        Condition probabilities on final high lying within support bounds.

        This enforces that materially-below-observed outcomes receive negligible mass,
        and allows late-day lock mode to cap implausible upper tails.
        """
        if std_dev <= 0:
            return self.calculate_bracket_probability(bracket, mean, std_dev)

        lower = lower_bound if lower_bound is not None else float("-inf")
        upper = upper_bound if upper_bound is not None else float("inf")
        if upper <= lower:
            return self.min_prob

        lower_cdf = 0.0 if lower == float("-inf") else normal_cdf(lower, mean, std_dev)
        upper_cdf = 1.0 if upper == float("inf") else normal_cdf(upper, mean, std_dev)
        norm = max(0.0, upper_cdf - lower_cdf)
        if norm <= 0.0:
            probe = lower + 0.6 if lower != float("-inf") else mean
            if bracket.contains_temp(probe):
                return self.max_prob
            return self.min_prob

        left: float
        right: float
        if bracket.bracket_type == BracketType.BETWEEN:
            left = bracket.lower_bound - 0.5
            right = bracket.upper_bound + 0.5
        elif bracket.bracket_type == BracketType.GREATER_THAN:
            left = bracket.lower_bound + 0.5
            right = float("inf")
        elif bracket.bracket_type == BracketType.LESS_THAN:
            left = float("-inf")
            right = bracket.upper_bound - 0.5
        else:
            return self.min_prob

        clipped_left = max(left, lower)
        clipped_right = min(right, upper)
        if clipped_right <= clipped_left:
            return self.min_prob

        if clipped_right == float("inf"):
            clipped_prob = 1.0 - normal_cdf(clipped_left, mean, std_dev)
        elif clipped_left == float("-inf"):
            clipped_prob = max(0.0, normal_cdf(clipped_right, mean, std_dev) - lower_cdf)
        else:
            clipped_prob = max(0.0, normal_cdf(clipped_right, mean, std_dev) - normal_cdf(clipped_left, mean, std_dev))

        conditioned = clipped_prob / norm
        return self._clamp_probability(conditioned)

    def _is_exhaustive_structure(self, brackets: List[MarketBracket]) -> bool:
        """
        Heuristic check for standard Kalshi ladder structure covering all outcomes.
        """
        has_lt = any(b.bracket_type == BracketType.LESS_THAN for b in brackets)
        has_gt = any(b.bracket_type == BracketType.GREATER_THAN for b in brackets)
        return has_lt and has_gt and len(brackets) >= 3

    def calculate_from_adjusted_forecast(
        self,
        adjusted_forecast: AdjustedForecast,
        brackets: List[MarketBracket],
    ) -> List[BracketProbability]:
        """
        Calculate bracket probabilities using an adjusted forecast.

        Args:
            adjusted_forecast: The adjusted temperature forecast
            brackets: List of market brackets

        Returns:
            List of BracketProbability objects
        """
        return self.calculate_all_probabilities(
            brackets,
            adjusted_forecast.mean_temp_f,
            adjusted_forecast.std_dev,
            lower_bound=adjusted_forecast.conditioning_cutoff_f,
            upper_bound=adjusted_forecast.conditioning_ceiling_f,
        )

    def calculate_from_combined_forecast(
        self,
        combined_forecast: CombinedForecast,
        brackets: List[MarketBracket],
    ) -> List[BracketProbability]:
        """
        Calculate bracket probabilities using a combined forecast.

        Args:
            combined_forecast: The combined temperature forecast
            brackets: List of market brackets

        Returns:
            List of BracketProbability objects
        """
        return self.calculate_all_probabilities(
            brackets,
            combined_forecast.mean_temp_f,
            combined_forecast.std_dev,
        )


def calculate_bracket_probabilities(
    brackets: List[MarketBracket],
    mean: float,
    std_dev: float,
) -> List[BracketProbability]:
    """
    Convenience function to calculate bracket probabilities.

    Args:
        brackets: List of market brackets
        mean: Temperature distribution mean
        std_dev: Temperature distribution standard deviation

    Returns:
        List of BracketProbability objects
    """
    calculator = BracketProbabilityCalculator()
    return calculator.calculate_all_probabilities(brackets, mean, std_dev)
