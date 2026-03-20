"""
Tests for the probability engine.

Module 2A: Forecast Combiner
Module 2B: Observation Adjuster
Module 2C: Bracket Probability Calculator
"""

import pytest
import math
from datetime import datetime
from zoneinfo import ZoneInfo

from kalshi_weather.core import (
    TemperatureForecast,
    DailyObservation,
    StationReading,
    StationType,
    MarketBracket,
    BracketType,
)
from kalshi_weather.engine.probability import (
    # Module 2A
    ForecastCombiner,
    CombinedForecast,
    combine_forecasts,
    DEFAULT_WEIGHTS,
    MIN_STD_DEV,
    MAX_STD_DEV,
    # Module 2B
    ObservationAdjuster,
    AdjustedForecast,
    adjust_forecast_with_observations,
    EARLY_CUTOFF_HOURS,
    LATE_CUTOFF_HOURS,
    # Module 2C
    BracketProbabilityCalculator,
    BracketProbability,
    calculate_bracket_probabilities,
    normal_cdf,
)


# =============================================================================
# TEST DATA HELPERS
# =============================================================================

TARGET_DATE = "2026-01-20"


def make_forecast(
    source: str,
    temp_f: float,
    std_dev: float = 2.0,
    target_date: str = TARGET_DATE,
) -> TemperatureForecast:
    """Create a test forecast with sensible defaults."""
    return TemperatureForecast(
        source=source,
        target_date=target_date,
        forecast_temp_f=temp_f,
        low_f=temp_f - 1.28 * std_dev,
        high_f=temp_f + 1.28 * std_dev,
        std_dev=std_dev,
        model_run_time=None,
        fetched_at=datetime.now(),
    )


# =============================================================================
# WEIGHT LOOKUP TESTS
# =============================================================================


class TestGetWeight:
    """Tests for weight lookup functionality."""

    def test_exact_match(self):
        combiner = ForecastCombiner()
        assert combiner.get_weight("NWS") == 5.0
        assert combiner.get_weight("GFS+HRRR") == 3.0

    def test_partial_match(self):
        combiner = ForecastCombiner()
        # "GFS" should match the GFS weight
        assert combiner.get_weight("GFS") == 2.5

    def test_case_insensitive_match(self):
        combiner = ForecastCombiner()
        # Should find NWS even with different case
        assert combiner.get_weight("nws") == 5.0

    def test_unknown_source_gets_default(self):
        combiner = ForecastCombiner()
        assert combiner.get_weight("SomeUnknownModel") == 1.0

    def test_custom_weights(self):
        custom = {"MyModel": 10.0, "default": 0.5}
        combiner = ForecastCombiner(weights=custom)
        assert combiner.get_weight("MyModel") == 10.0
        assert combiner.get_weight("Unknown") == 0.5


# =============================================================================
# WEIGHTED MEAN TESTS
# =============================================================================


class TestWeightedMean:
    """Tests for weighted mean calculation."""

    def test_single_forecast(self):
        forecasts = [make_forecast("NWS", 55.0)]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.mean_temp_f == 55.0

    def test_equal_weight_forecasts(self):
        # Two forecasts with same weight should average
        forecasts = [
            make_forecast("ModelA", 50.0),
            make_forecast("ModelB", 60.0),
        ]
        combiner = ForecastCombiner(weights={"ModelA": 1.0, "ModelB": 1.0, "default": 1.0})
        result = combiner.combine(forecasts)
        assert result is not None
        assert abs(result.mean_temp_f - 55.0) < 0.01

    def test_weighted_mean_favors_higher_weight(self):
        # NWS (weight 5) at 60°F, Unknown (weight 1) at 50°F
        # Weighted mean = (5*60 + 1*50) / 6 = 350/6 ≈ 58.33
        forecasts = [
            make_forecast("NWS", 60.0),
            make_forecast("Unknown", 50.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.mean_temp_f > 55.0  # Should be closer to NWS
        expected = (5 * 60 + 1 * 50) / 6
        assert abs(result.mean_temp_f - expected) < 0.01

    def test_three_forecasts(self):
        # NWS=5, ECMWF=4, GFS=2.5
        forecasts = [
            make_forecast("NWS", 50.0),
            make_forecast("ECMWF", 52.0),
            make_forecast("GFS", 54.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        # Weighted mean = (5*50 + 4*52 + 2.5*54) / 11.5 = 593/11.5 ≈ 51.57
        expected = (5 * 50 + 4 * 52 + 2.5 * 54) / 11.5
        assert abs(result.mean_temp_f - expected) < 0.01


# =============================================================================
# UNCERTAINTY COMBINATION TESTS
# =============================================================================


class TestUncertaintyCombination:
    """Tests for combined uncertainty calculation."""

    def test_single_forecast_preserves_std_dev(self):
        forecasts = [make_forecast("NWS", 55.0, std_dev=3.0)]
        result = combine_forecasts(forecasts)
        assert result is not None
        # Single forecast, no disagreement, std_dev should be preserved
        # (but may be clamped to min)
        assert result.std_dev >= MIN_STD_DEV
        assert abs(result.std_dev - 3.0) < 0.01

    def test_agreeing_forecasts_low_uncertainty(self):
        # All forecasts agree on 55°F
        forecasts = [
            make_forecast("NWS", 55.0, std_dev=2.0),
            make_forecast("ECMWF", 55.0, std_dev=2.0),
            make_forecast("GFS", 55.0, std_dev=2.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        # No disagreement, so combined std is just pooled individual std
        assert result.std_dev == 2.0

    def test_disagreeing_forecasts_higher_uncertainty(self):
        # Forecasts disagree significantly
        forecasts = [
            make_forecast("NWS", 50.0, std_dev=2.0),
            make_forecast("ECMWF", 60.0, std_dev=2.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        # Should have higher uncertainty due to disagreement
        assert result.std_dev > 2.0

    def test_std_dev_minimum_floor(self):
        # Even with very low uncertainty forecasts
        forecasts = [
            make_forecast("NWS", 55.0, std_dev=0.5),
            make_forecast("ECMWF", 55.0, std_dev=0.5),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.std_dev >= MIN_STD_DEV

    def test_std_dev_maximum_cap(self):
        # Wildly disagreeing forecasts
        forecasts = [
            make_forecast("NWS", 30.0, std_dev=5.0),
            make_forecast("Unknown", 70.0, std_dev=5.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.std_dev <= MAX_STD_DEV

    def test_custom_min_std_dev(self):
        combiner = ForecastCombiner(min_std_dev=3.0)
        forecasts = [make_forecast("NWS", 55.0, std_dev=1.0)]
        result = combiner.combine(forecasts)
        assert result is not None
        assert result.std_dev >= 3.0


# =============================================================================
# PERCENTILE TESTS
# =============================================================================


class TestPercentiles:
    """Tests for low/high percentile calculation."""

    def test_percentiles_symmetric(self):
        forecasts = [make_forecast("NWS", 55.0, std_dev=2.0)]
        result = combine_forecasts(forecasts)
        assert result is not None
        # 10th and 90th percentiles should be symmetric around mean
        assert abs((result.mean_temp_f - result.low_f) - (result.high_f - result.mean_temp_f)) < 0.01

    def test_percentiles_use_z_score(self):
        forecasts = [make_forecast("NWS", 55.0, std_dev=2.0)]
        result = combine_forecasts(forecasts)
        assert result is not None
        # Z-score for 10th/90th percentile is ~1.28
        expected_spread = 1.28155 * 2.0
        assert abs(result.high_f - result.low_f - 2 * expected_spread) < 0.1

    def test_wider_std_gives_wider_range(self):
        narrow = combine_forecasts([make_forecast("NWS", 55.0, std_dev=1.5)])
        wide = combine_forecasts([make_forecast("NWS", 55.0, std_dev=4.0)])
        assert narrow is not None and wide is not None
        narrow_range = narrow.high_f - narrow.low_f
        wide_range = wide.high_f - wide.low_f
        assert wide_range > narrow_range


# =============================================================================
# EDGE CASES
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_list_returns_none(self):
        result = combine_forecasts([])
        assert result is None

    def test_nan_temperature_filtered(self):
        forecasts = [
            make_forecast("NWS", 55.0),
            make_forecast("Bad", float('nan')),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.source_count == 1
        assert result.mean_temp_f == 55.0

    def test_all_invalid_returns_none(self):
        forecasts = [
            make_forecast("Bad1", float('nan')),
            make_forecast("Bad2", float('nan')),
        ]
        result = combine_forecasts(forecasts)
        assert result is None

    def test_different_dates_uses_first(self):
        forecasts = [
            make_forecast("NWS", 55.0, target_date="2026-01-20"),
            make_forecast("ECMWF", 56.0, target_date="2026-01-21"),  # Different date
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert result.target_date == "2026-01-20"


# =============================================================================
# COMBINED FORECAST PROPERTIES
# =============================================================================


class TestCombinedForecastProperties:
    """Tests for CombinedForecast data class."""

    def test_variance_property(self):
        forecasts = [make_forecast("NWS", 55.0, std_dev=2.0)]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert abs(result.variance - 4.0) < 0.01  # 2.0^2 = 4.0

    def test_sources_used_populated(self):
        forecasts = [
            make_forecast("NWS", 55.0),
            make_forecast("ECMWF", 56.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert "NWS" in result.sources_used
        assert "ECMWF" in result.sources_used

    def test_weights_used_normalized(self):
        forecasts = [
            make_forecast("NWS", 55.0),
            make_forecast("ECMWF", 56.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        # Weights should sum to 1.0
        total = sum(result.weights_used.values())
        assert abs(total - 1.0) < 0.001

    def test_individual_forecasts_stored(self):
        forecasts = [
            make_forecast("NWS", 55.0),
            make_forecast("ECMWF", 56.0),
        ]
        result = combine_forecasts(forecasts)
        assert result is not None
        assert len(result.individual_forecasts) == 2

    def test_combined_at_timestamp(self):
        before = datetime.now()
        result = combine_forecasts([make_forecast("NWS", 55.0)])
        after = datetime.now()
        assert result is not None
        assert before <= result.combined_at <= after


# =============================================================================
# CUSTOM WEIGHTS TESTS
# =============================================================================


class TestCustomWeights:
    """Tests for custom weight functionality."""

    def test_combine_with_custom_weights(self):
        forecasts = [
            make_forecast("ModelA", 50.0),
            make_forecast("ModelB", 60.0),
        ]
        combiner = ForecastCombiner()

        # Custom weights: ModelB has much higher weight
        custom = {"ModelA": 1.0, "ModelB": 9.0, "default": 1.0}
        result = combiner.combine_with_custom_weights(forecasts, custom)

        assert result is not None
        # Weighted mean = (1*50 + 9*60) / 10 = 59
        assert abs(result.mean_temp_f - 59.0) < 0.01

    def test_custom_weights_doesnt_modify_original(self):
        combiner = ForecastCombiner()
        original_nws_weight = combiner.get_weight("NWS")

        forecasts = [make_forecast("NWS", 55.0)]
        custom = {"NWS": 100.0, "default": 1.0}
        combiner.combine_with_custom_weights(forecasts, custom)

        # Original weights should be unchanged
        assert combiner.get_weight("NWS") == original_nws_weight


# =============================================================================
# INTEGRATION-STYLE TESTS
# =============================================================================


class TestRealWorldScenarios:
    """Tests simulating real-world forecast combinations."""

    def test_typical_four_source_combination(self):
        """Simulate combining Open-Meteo Best Match, GFS+HRRR, Ensemble, and NWS."""
        forecasts = [
            make_forecast("Open-Meteo Best Match", 52.0, std_dev=2.0),
            make_forecast("GFS+HRRR", 51.0, std_dev=2.5),
            make_forecast("Open-Meteo Ensemble", 54.0, std_dev=3.0),
            make_forecast("NWS", 50.0, std_dev=2.0),
        ]
        result = combine_forecasts(forecasts)

        assert result is not None
        assert result.source_count == 4
        # NWS should have highest weight, pulling mean toward 50
        assert result.mean_temp_f < 52.5  # Below simple average
        # Uncertainty should account for 4°F spread in forecasts
        assert result.std_dev > 2.0

    def test_high_agreement_scenario(self):
        """All models agree closely - should have lower uncertainty."""
        forecasts = [
            make_forecast("NWS", 55.0, std_dev=2.0),
            make_forecast("ECMWF", 55.5, std_dev=2.0),
            make_forecast("GFS+HRRR", 54.5, std_dev=2.0),
        ]
        result = combine_forecasts(forecasts)

        assert result is not None
        # Mean should be very close to 55
        assert abs(result.mean_temp_f - 55.0) < 0.5
        # Low disagreement should not inflate uncertainty much
        assert result.std_dev < 2.5

    def test_high_disagreement_scenario(self):
        """Models disagree significantly - should have higher uncertainty."""
        forecasts = [
            make_forecast("NWS", 45.0, std_dev=2.0),
            make_forecast("ECMWF", 55.0, std_dev=2.0),
            make_forecast("GFS+HRRR", 50.0, std_dev=2.0),
        ]
        result = combine_forecasts(forecasts)

        assert result is not None
        # 10°F spread between NWS and ECMWF should inflate uncertainty
        assert result.std_dev > 3.0

    def test_single_nws_forecast(self):
        """Only NWS available - should use NWS values directly."""
        forecasts = [make_forecast("NWS", 53.0, std_dev=2.5)]
        result = combine_forecasts(forecasts)

        assert result is not None
        assert result.mean_temp_f == 53.0
        assert result.std_dev == 2.5
        assert result.weights_used["NWS"] == 1.0


# =============================================================================
# MATHEMATICAL CORRECTNESS TESTS
# =============================================================================


class TestMathematicalCorrectness:
    """Tests to verify the mathematical formulas are correct."""

    def test_variance_formula_pooled_component(self):
        """Verify pooled variance is weighted average of individual variances."""
        forecasts = [
            make_forecast("ModelA", 50.0, std_dev=2.0),
            make_forecast("ModelB", 50.0, std_dev=4.0),  # Same temp, different uncertainty
        ]
        combiner = ForecastCombiner(weights={"ModelA": 1.0, "ModelB": 1.0, "default": 1.0})
        result = combiner.combine(forecasts)

        assert result is not None
        # No disagreement (same temp), so variance is just pooled
        # Pooled variance = (1*4 + 1*16) / 2 = 10
        # Std dev = sqrt(10) ≈ 3.16
        expected_std = math.sqrt((4 + 16) / 2)
        assert abs(result.std_dev - expected_std) < 0.01

    def test_variance_formula_disagreement_component(self):
        """Verify disagreement variance is weighted variance of means."""
        forecasts = [
            make_forecast("ModelA", 50.0, std_dev=0.0),
            make_forecast("ModelB", 60.0, std_dev=0.0),  # No individual uncertainty
        ]
        combiner = ForecastCombiner(weights={"ModelA": 1.0, "ModelB": 1.0, "default": 1.0}, min_std_dev=0.0)
        result = combiner.combine(forecasts)

        assert result is not None
        # Mean = 55, disagreement variance = (1*(50-55)^2 + 1*(60-55)^2) / 2 = 25
        # Std dev = sqrt(25) = 5
        assert abs(result.std_dev - 5.0) < 0.01

    def test_variance_formula_combined(self):
        """Verify total variance is sum of pooled and disagreement."""
        forecasts = [
            make_forecast("ModelA", 50.0, std_dev=3.0),
            make_forecast("ModelB", 56.0, std_dev=3.0),
        ]
        combiner = ForecastCombiner(weights={"ModelA": 1.0, "ModelB": 1.0, "default": 1.0}, min_std_dev=0.0)
        result = combiner.combine(forecasts)

        assert result is not None
        # Mean = 53
        # Pooled variance = (9 + 9) / 2 = 9
        # Disagreement variance = ((50-53)^2 + (56-53)^2) / 2 = (9 + 9) / 2 = 9
        # Total variance = 9 + 9 = 18
        # Std dev = sqrt(18) ≈ 4.24
        expected_std = math.sqrt(18)
        assert abs(result.std_dev - expected_std) < 0.01


# =============================================================================
# MODULE 2B: OBSERVATION ADJUSTER TESTS
# =============================================================================

NYC_TZ = ZoneInfo("America/New_York")


def make_combined_forecast(
    mean: float = 55.0,
    std_dev: float = 2.0,
    target_date: str = TARGET_DATE,
) -> CombinedForecast:
    """Create a test combined forecast."""
    z_10 = 1.28155
    return CombinedForecast(
        target_date=target_date,
        mean_temp_f=mean,
        std_dev=std_dev,
        low_f=mean - z_10 * std_dev,
        high_f=mean + z_10 * std_dev,
        source_count=1,
        sources_used=["NWS"],
        weights_used={"NWS": 1.0},
    )


def make_observation(
    observed_high: float = 54.0,
    low_bound: float = 53.0,
    high_bound: float = 55.0,
    date: str = TARGET_DATE,
) -> DailyObservation:
    """Create a test daily observation."""
    reading = StationReading(
        station_id="KNYC",
        timestamp=datetime.now(NYC_TZ),
        station_type=StationType.FIVE_MINUTE,
        reported_temp_f=observed_high,
        reported_temp_c=None,
        possible_actual_f_low=low_bound,
        possible_actual_f_high=high_bound,
    )
    return DailyObservation(
        station_id="KNYC",
        date=date,
        observed_high_f=observed_high,
        possible_actual_high_low=low_bound,
        possible_actual_high_high=high_bound,
        readings=[reading],
        last_updated=datetime.now(NYC_TZ),
    )


def make_time(hour: int, minute: int = 0) -> datetime:
    """Create a datetime at specific hour in NYC timezone."""
    return datetime(2026, 1, 20, hour, minute, tzinfo=NYC_TZ)


# =============================================================================
# TIME-BASED WEIGHT TESTS
# =============================================================================


class TestObservationWeight:
    """Tests for time-based observation weight calculation."""

    def test_before_noon_zero_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        # 10 AM - should be pure forecast
        hours = adjuster._calculate_hours_since_noon(make_time(10, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert weight == 0.0

    def test_at_noon_zero_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(12, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert weight == 0.0

    def test_at_1pm_low_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(13, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert 0.1 < weight < 0.2  # Should be around 0.15

    def test_at_2pm_transitional_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(14, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert abs(weight - 0.3) < 0.05

    def test_at_3pm_mid_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(15, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert 0.5 < weight < 0.6

    def test_at_4pm_high_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(16, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert abs(weight - 0.8) < 0.05

    def test_at_6pm_very_high_weight(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(18, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert weight > 0.85

    def test_late_evening_approaches_max(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        hours = adjuster._calculate_hours_since_noon(make_time(20, 0))
        weight = adjuster._calculate_observation_weight(hours)
        assert weight >= 0.9
        assert weight <= 0.95  # Never reaches 1.0

    def test_weight_monotonically_increases(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        weights = []
        for hour in range(12, 21):
            hours = adjuster._calculate_hours_since_noon(make_time(hour, 0))
            weights.append(adjuster._calculate_observation_weight(hours))
        # Each weight should be >= previous
        for i in range(1, len(weights)):
            assert weights[i] >= weights[i - 1]


# =============================================================================
# ADJUSTED MEAN TESTS
# =============================================================================


class TestAdjustedMean:
    """Tests for adjusted mean calculation."""

    def test_no_observation_unchanged(self):
        forecast = make_combined_forecast(mean=55.0)
        result = adjust_forecast_with_observations(forecast, None)
        assert result.mean_temp_f == 55.0
        assert result.observation_weight == 0.0

    def test_early_morning_mostly_forecast(self):
        forecast = make_combined_forecast(mean=55.0)
        observation = make_observation(observed_high=50.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(10, 0)
        )
        # Before noon, should be pure forecast
        assert result.mean_temp_f == 55.0

    def test_late_day_mostly_observation(self):
        forecast = make_combined_forecast(mean=55.0)
        observation = make_observation(observed_high=58.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(18, 0)
        )
        # Late day, should be much closer to observation
        assert result.mean_temp_f > 56.5

    def test_mean_never_below_observed_high(self):
        forecast = make_combined_forecast(mean=50.0)
        observation = make_observation(observed_high=55.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # Mean can't be below what we've observed
        assert result.mean_temp_f >= 55.0

    def test_blending_at_midday(self):
        forecast = make_combined_forecast(mean=60.0)
        observation = make_observation(observed_high=50.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # Should be between forecast and observation, but >= observed
        assert result.mean_temp_f >= 50.0
        assert result.mean_temp_f < 60.0


# =============================================================================
# ADJUSTED UNCERTAINTY TESTS
# =============================================================================


class TestAdjustedUncertainty:
    """Tests for adjusted standard deviation calculation."""

    def test_no_observation_preserves_std(self):
        forecast = make_combined_forecast(mean=55.0, std_dev=3.0)
        result = adjust_forecast_with_observations(forecast, None)
        assert result.std_dev == 3.0

    def test_late_day_reduced_uncertainty(self):
        forecast = make_combined_forecast(mean=55.0, std_dev=3.0)
        observation = make_observation(
            observed_high=54.0, low_bound=53.5, high_bound=54.5
        )
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(18, 0)
        )
        # Late day with tight observation bounds should reduce uncertainty
        assert result.std_dev < 3.0

    def test_std_dev_minimum_floor(self):
        forecast = make_combined_forecast(mean=55.0, std_dev=0.5)
        observation = make_observation(
            observed_high=55.0, low_bound=54.9, high_bound=55.1
        )
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(20, 0)
        )
        # Should still have minimum uncertainty
        assert result.std_dev >= MIN_STD_DEV


# =============================================================================
# CONSTRAINT TESTS
# =============================================================================


class TestConstraints:
    """Tests for min/max possible high constraints."""

    def test_min_possible_from_observation(self):
        forecast = make_combined_forecast(mean=55.0)
        observation = make_observation(low_bound=52.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        assert result.min_possible_high == 52.0

    def test_low_f_constrained_by_observation(self):
        forecast = make_combined_forecast(mean=55.0, std_dev=5.0)
        observation = make_observation(low_bound=53.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # low_f should not go below observation bounds
        assert result.low_f >= 53.0


# =============================================================================
# ADJUSTED FORECAST PROPERTIES
# =============================================================================


class TestAdjustedForecastProperties:
    """Tests for AdjustedForecast data class properties."""

    def test_variance_property(self):
        forecast = make_combined_forecast(mean=55.0, std_dev=2.0)
        result = adjust_forecast_with_observations(forecast, None)
        assert abs(result.variance - 4.0) < 0.01

    def test_is_observation_dominant_false_early(self):
        forecast = make_combined_forecast()
        observation = make_observation()
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(13, 0)
        )
        assert not result.is_observation_dominant

    def test_is_observation_dominant_true_late(self):
        forecast = make_combined_forecast()
        observation = make_observation()
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(17, 0)
        )
        assert result.is_observation_dominant

    def test_stores_original_forecast(self):
        forecast = make_combined_forecast(mean=55.0)
        observation = make_observation()
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        assert result.original_forecast.mean_temp_f == 55.0

    def test_stores_observation(self):
        forecast = make_combined_forecast()
        observation = make_observation(observed_high=54.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        assert result.observation is not None
        assert result.observed_high_f == 54.0

    def test_hours_since_noon_recorded(self):
        forecast = make_combined_forecast()
        observation = make_observation()
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(14, 30)
        )
        assert abs(result.hours_since_noon - 2.5) < 0.01


# =============================================================================
# EDGE CASES
# =============================================================================


class TestObservationAdjusterEdgeCases:
    """Tests for edge cases in observation adjustment."""

    def test_empty_readings_treated_as_no_observation(self):
        forecast = make_combined_forecast(mean=55.0)
        observation = DailyObservation(
            station_id="KNYC",
            date=TARGET_DATE,
            observed_high_f=50.0,
            possible_actual_high_low=49.0,
            possible_actual_high_high=51.0,
            readings=[],  # Empty readings
        )
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # Should behave as if no observation
        assert result.observation_weight == 0.0
        assert result.mean_temp_f == 55.0

    def test_different_timezone(self):
        la_tz = ZoneInfo("America/Los_Angeles")
        adjuster = ObservationAdjuster(timezone=la_tz)
        # 3 PM LA time
        la_time = datetime(2026, 1, 20, 15, 0, tzinfo=la_tz)
        hours = adjuster._calculate_hours_since_noon(la_time)
        assert abs(hours - 3.0) < 0.01

    def test_naive_datetime_assumes_local(self):
        adjuster = ObservationAdjuster(timezone=NYC_TZ)
        naive_time = datetime(2026, 1, 20, 14, 0)  # No timezone
        hours = adjuster._calculate_hours_since_noon(naive_time)
        assert abs(hours - 2.0) < 0.01


# =============================================================================
# REAL WORLD SCENARIOS
# =============================================================================


class TestObservationAdjusterScenarios:
    """Tests simulating real-world observation adjustment scenarios."""

    def test_morning_cold_observation(self):
        """Morning: observed high is low, forecast expects warmer - trust forecast."""
        forecast = make_combined_forecast(mean=55.0, std_dev=2.0)
        observation = make_observation(observed_high=45.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(9, 0)
        )
        # Morning: pure forecast
        assert result.mean_temp_f == 55.0
        assert result.observation_weight == 0.0

    def test_afternoon_warm_observation(self):
        """Afternoon: observed high exceeds forecast - adjust upward."""
        forecast = make_combined_forecast(mean=55.0, std_dev=2.0)
        observation = make_observation(observed_high=60.0, high_bound=61.0)
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # Should be pulled toward observation
        assert result.mean_temp_f >= 60.0  # Can't be below observed

    def test_evening_observation_dominates(self):
        """Evening: observed high should dominate."""
        forecast = make_combined_forecast(mean=50.0, std_dev=2.0)
        observation = make_observation(
            observed_high=55.0, low_bound=54.5, high_bound=55.5
        )
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(19, 0)
        )
        # Evening: close to observation
        assert result.mean_temp_f >= 54.5
        assert result.observation_weight > 0.85

    def test_forecast_observation_agree(self):
        """When forecast and observation agree, uncertainty should be lower."""
        forecast = make_combined_forecast(mean=55.0, std_dev=3.0)
        observation = make_observation(
            observed_high=55.0, low_bound=54.5, high_bound=55.5
        )
        result = adjust_forecast_with_observations(
            forecast, observation, current_time=make_time(15, 0)
        )
        # Agreement should reduce uncertainty somewhat
        assert result.std_dev <= 3.0


# =============================================================================
# MODULE 2C: BRACKET PROBABILITY CALCULATOR TESTS
# =============================================================================


def make_bracket(
    bracket_type: BracketType,
    lower: float = None,
    upper: float = None,
    implied_prob: float = 0.20,
    ticker: str = "TEST",
) -> MarketBracket:
    """Create a test market bracket."""
    if bracket_type == BracketType.BETWEEN:
        subtitle = f"{int(lower)}° to {int(upper)}°"
    elif bracket_type == BracketType.GREATER_THAN:
        subtitle = f"Above {int(lower)}°"
    elif bracket_type == BracketType.LESS_THAN:
        subtitle = f"Below {int(upper)}°"
    else:
        subtitle = "Unknown"

    return MarketBracket(
        ticker=ticker,
        event_ticker="TEST-EVENT",
        subtitle=subtitle,
        bracket_type=bracket_type,
        lower_bound=lower,
        upper_bound=upper,
        yes_bid=int(implied_prob * 100) - 2,
        yes_ask=int(implied_prob * 100) + 2,
        last_price=int(implied_prob * 100),
        volume=1000,
        implied_prob=implied_prob,
    )


# =============================================================================
# NORMAL CDF TESTS
# =============================================================================


class TestNormalCdf:
    """Tests for normal CDF calculation."""

    def test_cdf_at_mean_is_half(self):
        # CDF at mean should be 0.5
        assert abs(normal_cdf(50.0, 50.0, 2.0) - 0.5) < 0.001

    def test_cdf_far_below_mean_near_zero(self):
        # 5 std devs below mean should be near 0
        assert normal_cdf(40.0, 50.0, 2.0) < 0.001

    def test_cdf_far_above_mean_near_one(self):
        # 5 std devs above mean should be near 1
        assert normal_cdf(60.0, 50.0, 2.0) > 0.999

    def test_cdf_one_std_below(self):
        # CDF at mean - 1*std should be ~0.159
        result = normal_cdf(48.0, 50.0, 2.0)
        assert abs(result - 0.159) < 0.01

    def test_cdf_one_std_above(self):
        # CDF at mean + 1*std should be ~0.841
        result = normal_cdf(52.0, 50.0, 2.0)
        assert abs(result - 0.841) < 0.01

    def test_cdf_zero_std_dev(self):
        # Degenerate case: all mass at mean
        assert normal_cdf(49.0, 50.0, 0.0) == 0.0
        assert normal_cdf(50.0, 50.0, 0.0) == 1.0
        assert normal_cdf(51.0, 50.0, 0.0) == 1.0


# =============================================================================
# BETWEEN BRACKET TESTS
# =============================================================================


class TestBetweenBracketProbability:
    """Tests for BETWEEN bracket probability calculation."""

    def test_bracket_at_mean_high_probability(self):
        # Bracket containing the mean should have high probability
        bracket = make_bracket(BracketType.BETWEEN, lower=49, upper=51)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob > 0.3  # Should be substantial

    def test_bracket_far_from_mean_low_probability(self):
        # Bracket far from mean should have low probability
        bracket = make_bracket(BracketType.BETWEEN, lower=60, upper=62)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob < 0.01

    def test_wider_bracket_higher_probability(self):
        # Wider bracket should capture more probability
        narrow = make_bracket(BracketType.BETWEEN, lower=49, upper=51)
        wide = make_bracket(BracketType.BETWEEN, lower=47, upper=53)
        calculator = BracketProbabilityCalculator()
        narrow_prob = calculator.calculate_bracket_probability(narrow, mean=50.0, std_dev=2.0)
        wide_prob = calculator.calculate_bracket_probability(wide, mean=50.0, std_dev=2.0)
        assert wide_prob > narrow_prob

    def test_boundary_inclusive(self):
        # Temperature exactly at boundary should be included
        # With mean=55 and std=2, P(54 <= T <= 56) should include 54, 55, 56
        bracket = make_bracket(BracketType.BETWEEN, lower=54, upper=56)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=55.0, std_dev=2.0)
        # Should be CDF(56.5) - CDF(53.5)
        expected = normal_cdf(56.5, 55.0, 2.0) - normal_cdf(53.5, 55.0, 2.0)
        assert abs(prob - expected) < 0.001


# =============================================================================
# GREATER_THAN BRACKET TESTS
# =============================================================================


class TestGreaterThanBracketProbability:
    """Tests for GREATER_THAN bracket probability calculation."""

    def test_threshold_below_mean_high_probability(self):
        # Threshold below mean: high probability of exceeding
        bracket = make_bracket(BracketType.GREATER_THAN, lower=48)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob > 0.7

    def test_threshold_above_mean_low_probability(self):
        # Threshold above mean: low probability of exceeding
        bracket = make_bracket(BracketType.GREATER_THAN, lower=54)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob < 0.05

    def test_threshold_at_mean(self):
        # Threshold at mean: ~50% probability (slightly less due to 0.5 adjustment)
        bracket = make_bracket(BracketType.GREATER_THAN, lower=50)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        # P(T > 50) = 1 - CDF(50.5) < 0.5
        assert prob < 0.5
        assert prob > 0.4

    def test_strictly_greater_boundary(self):
        # Temperature exactly at threshold does NOT win
        # Formula: 1 - CDF(threshold + 0.5)
        bracket = make_bracket(BracketType.GREATER_THAN, lower=55)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=55.0, std_dev=2.0)
        expected = 1.0 - normal_cdf(55.5, 55.0, 2.0)
        assert abs(prob - expected) < 0.001


# =============================================================================
# LESS_THAN BRACKET TESTS
# =============================================================================


class TestLessThanBracketProbability:
    """Tests for LESS_THAN bracket probability calculation."""

    def test_threshold_above_mean_high_probability(self):
        # Threshold above mean: high probability of being below
        bracket = make_bracket(BracketType.LESS_THAN, upper=52)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob > 0.7

    def test_threshold_below_mean_low_probability(self):
        # Threshold below mean: low probability of being below
        bracket = make_bracket(BracketType.LESS_THAN, upper=46)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob < 0.05

    def test_threshold_at_mean(self):
        # Threshold at mean: ~50% probability (slightly less due to 0.5 adjustment)
        bracket = make_bracket(BracketType.LESS_THAN, upper=50)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        # P(T < 50) = CDF(49.5) < 0.5
        assert prob < 0.5
        assert prob > 0.4

    def test_strictly_less_boundary(self):
        # Temperature exactly at threshold does NOT win
        # Formula: CDF(threshold - 0.5)
        bracket = make_bracket(BracketType.LESS_THAN, upper=55)
        calculator = BracketProbabilityCalculator()
        prob = calculator.calculate_bracket_probability(bracket, mean=55.0, std_dev=2.0)
        expected = normal_cdf(54.5, 55.0, 2.0)
        assert abs(prob - expected) < 0.001


# =============================================================================
# PROBABILITY CLAMPING TESTS
# =============================================================================


class TestProbabilityClamping:
    """Tests for probability min/max clamping."""

    def test_min_probability_floor(self):
        # Very unlikely bracket should still have minimum probability
        bracket = make_bracket(BracketType.BETWEEN, lower=100, upper=102)
        calculator = BracketProbabilityCalculator(min_prob=0.001)
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob >= 0.001

    def test_max_probability_ceiling(self):
        # Very likely bracket should still have maximum probability
        bracket = make_bracket(BracketType.BETWEEN, lower=40, upper=60)
        calculator = BracketProbabilityCalculator(max_prob=0.999)
        prob = calculator.calculate_bracket_probability(bracket, mean=50.0, std_dev=2.0)
        assert prob <= 0.999


# =============================================================================
# ALL BRACKETS CALCULATION TESTS
# =============================================================================


class TestCalculateAllProbabilities:
    """Tests for calculating probabilities across all brackets."""

    def test_probabilities_roughly_sum_to_one(self):
        # Mutually exclusive brackets should sum to ~1
        # Note: Kalshi brackets are non-overlapping, e.g., [48,49], [50,51], [52,53]
        brackets = [
            make_bracket(BracketType.LESS_THAN, upper=48),        # T < 48 (47 and below)
            make_bracket(BracketType.BETWEEN, lower=48, upper=49),  # 48-49
            make_bracket(BracketType.BETWEEN, lower=50, upper=51),  # 50-51
            make_bracket(BracketType.BETWEEN, lower=52, upper=53),  # 52-53
            make_bracket(BracketType.GREATER_THAN, lower=53),       # T > 53 (54 and above)
        ]
        results = calculate_bracket_probabilities(brackets, mean=51.0, std_dev=2.0)
        total = sum(bp.model_prob for bp in results)
        # Should be very close to 1.0
        assert abs(total - 1.0) < 0.01

    def test_edge_calculation(self):
        # Edge should be model_prob - market_prob
        bracket = make_bracket(BracketType.BETWEEN, lower=49, upper=51, implied_prob=0.30)
        results = calculate_bracket_probabilities([bracket], mean=50.0, std_dev=2.0)
        result = results[0]
        assert abs(result.edge - (result.model_prob - 0.30)) < 0.001
        assert abs(result.edge_pct - result.edge * 100) < 0.001

    def test_positive_edge_detection(self):
        # When model prob > market prob, edge should be positive
        bracket = make_bracket(BracketType.BETWEEN, lower=49, upper=51, implied_prob=0.10)
        results = calculate_bracket_probabilities([bracket], mean=50.0, std_dev=2.0)
        result = results[0]
        assert result.has_positive_edge
        assert result.edge_direction == "YES"

    def test_negative_edge_detection(self):
        # When model prob < market prob, edge should be negative
        bracket = make_bracket(BracketType.BETWEEN, lower=49, upper=51, implied_prob=0.90)
        results = calculate_bracket_probabilities([bracket], mean=50.0, std_dev=2.0)
        result = results[0]
        assert not result.has_positive_edge
        assert result.edge_direction == "NO"


# =============================================================================
# INTEGRATION WITH FORECASTS TESTS
# =============================================================================


class TestBracketCalculatorIntegration:
    """Tests for integration with forecast objects."""

    def test_from_combined_forecast(self):
        forecast = make_combined_forecast(mean=52.0, std_dev=2.5)
        bracket = make_bracket(BracketType.BETWEEN, lower=51, upper=53)
        calculator = BracketProbabilityCalculator()
        results = calculator.calculate_from_combined_forecast(forecast, [bracket])
        assert len(results) == 1
        assert results[0].model_prob > 0.3

    def test_from_adjusted_forecast(self):
        combined = make_combined_forecast(mean=52.0, std_dev=2.5)
        observation = make_observation(observed_high=54.0)
        adjusted = adjust_forecast_with_observations(
            combined, observation, current_time=make_time(16, 0)
        )
        bracket = make_bracket(BracketType.BETWEEN, lower=53, upper=55)
        calculator = BracketProbabilityCalculator()
        results = calculator.calculate_from_adjusted_forecast(adjusted, [bracket])
        assert len(results) == 1
        # Adjusted mean should be pulled toward observation
        assert results[0].model_prob > 0.2


# =============================================================================
# REAL WORLD SCENARIO TESTS
# =============================================================================


class TestBracketProbabilityScenarios:
    """Tests simulating real-world bracket probability scenarios."""

    def test_typical_market_structure(self):
        """Test a typical market with less_than, between, and greater_than brackets."""
        brackets = [
            make_bracket(BracketType.LESS_THAN, upper=48, implied_prob=0.05),
            make_bracket(BracketType.BETWEEN, lower=48, upper=50, implied_prob=0.15),
            make_bracket(BracketType.BETWEEN, lower=50, upper=52, implied_prob=0.35),
            make_bracket(BracketType.BETWEEN, lower=52, upper=54, implied_prob=0.30),
            make_bracket(BracketType.BETWEEN, lower=54, upper=56, implied_prob=0.10),
            make_bracket(BracketType.GREATER_THAN, lower=56, implied_prob=0.05),
        ]
        # Model predicts 51°F with std 2.5
        results = calculate_bracket_probabilities(brackets, mean=51.0, std_dev=2.5)

        # 50-52 bracket (containing mean) should have highest probability
        probs = {r.bracket.subtitle: r.model_prob for r in results}
        assert probs["50° to 52°"] > probs["48° to 50°"]
        assert probs["50° to 52°"] > probs["52° to 54°"]

    def test_model_vs_market_disagreement(self):
        """Test identifying edges when model disagrees with market."""
        # Market prices suggest 54°F most likely
        brackets = [
            make_bracket(BracketType.BETWEEN, lower=50, upper=52, implied_prob=0.10),
            make_bracket(BracketType.BETWEEN, lower=52, upper=54, implied_prob=0.25),
            make_bracket(BracketType.BETWEEN, lower=54, upper=56, implied_prob=0.40),
            make_bracket(BracketType.BETWEEN, lower=56, upper=58, implied_prob=0.20),
        ]
        # But model predicts 52°F
        results = calculate_bracket_probabilities(brackets, mean=52.0, std_dev=2.0)

        # Should find positive edge on 50-52 and 52-54, negative on 54-56
        edges = {r.bracket.subtitle: r.edge for r in results}
        assert edges["50° to 52°"] > 0  # Model higher than market
        assert edges["52° to 54°"] > 0  # Model higher than market
        assert edges["54° to 56°"] < 0  # Model lower than market

    def test_narrow_std_dev_concentrated_probability(self):
        """High confidence forecast should concentrate probability."""
        bracket_at_mean = make_bracket(BracketType.BETWEEN, lower=54, upper=56)
        bracket_away = make_bracket(BracketType.BETWEEN, lower=50, upper=52)

        calculator = BracketProbabilityCalculator()

        # Low uncertainty: probability concentrated at mean
        narrow_at_mean = calculator.calculate_bracket_probability(
            bracket_at_mean, mean=55.0, std_dev=1.0
        )
        narrow_away = calculator.calculate_bracket_probability(
            bracket_away, mean=55.0, std_dev=1.0
        )

        # High uncertainty: probability more spread out
        wide_at_mean = calculator.calculate_bracket_probability(
            bracket_at_mean, mean=55.0, std_dev=4.0
        )
        wide_away = calculator.calculate_bracket_probability(
            bracket_away, mean=55.0, std_dev=4.0
        )

        # Narrow should have higher prob at mean, lower away
        assert narrow_at_mean > wide_at_mean
        assert narrow_away < wide_away
