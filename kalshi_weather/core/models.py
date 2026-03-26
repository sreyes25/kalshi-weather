"""
Core data models for Kalshi Weather Bot.

Contains all shared data classes, enums, and abstract interfaces.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


# =============================================================================
# ENUMS
# =============================================================================


class ContractType(Enum):
    """Type of Kalshi weather contract."""
    HIGH_TEMP = "high_temp"      # Daily high temperature
    LOW_TEMP = "low_temp"        # Daily low temperature
    SNOWFALL = "snowfall"        # Daily snowfall amount
    RAINFALL = "rainfall"        # Daily rainfall amount


class BracketType(Enum):
    """Type of Kalshi bracket."""
    BETWEEN = "between"           # e.g., "54° to 56°" (inclusive both ends)
    GREATER_THAN = "greater_than" # e.g., "Above 56°" (strictly greater)
    LESS_THAN = "less_than"       # e.g., "Below 50°" (strictly less)


class StationType(Enum):
    """Type of NWS weather station."""
    HOURLY = "hourly"
    FIVE_MINUTE = "5-minute"
    UNKNOWN = "unknown"


# =============================================================================
# DATA CLASSES - WEATHER
# =============================================================================


@dataclass
class TemperatureForecast:
    """
    A temperature forecast from a weather model.

    All temperatures are in Fahrenheit.
    """
    source: str                    # e.g., "GFS", "ECMWF", "HRRR", "NWS"
    target_date: str               # YYYY-MM-DD format
    forecast_temp_f: float         # Point estimate (mean) in Fahrenheit
    low_f: float                   # 10th percentile estimate
    high_f: float                  # 90th percentile estimate
    std_dev: float                 # Standard deviation (uncertainty)
    model_run_time: Optional[datetime]  # When this model run was produced
    fetched_at: datetime           # When we fetched this data
    ensemble_members: List[float] = field(default_factory=list)


@dataclass
class StationReading:
    """
    A single observation from an NWS station.

    Includes both reported values and reverse-engineered actual bounds
    to handle the °C/°F conversion issues in 5-minute stations.
    """
    station_id: str                # e.g., "KNYC"
    timestamp: datetime            # Observation time
    station_type: StationType      # hourly or 5-minute
    reported_temp_f: float         # What NWS displays (may have conversion error)
    reported_temp_c: Optional[float]  # The Celsius value if available
    possible_actual_f_low: float   # Lower bound of actual temp
    possible_actual_f_high: float  # Upper bound of actual temp
    dewpoint_f: Optional[float] = None
    relative_humidity_pct: Optional[float] = None
    wind_direction_deg: Optional[float] = None
    wind_speed_mph: Optional[float] = None


@dataclass
class DailyObservation:
    """
    Aggregated observation data for a single day.

    Includes uncertainty bounds accounting for station data quirks.
    """
    station_id: str                # e.g., "KNYC"
    date: str                      # YYYY-MM-DD format
    observed_high_f: float         # Highest reading seen on time series
    possible_actual_high_low: float   # Actual settlement high could be this low
    possible_actual_high_high: float  # Actual settlement high could be this high
    reported_series_high_f: Optional[float] = None   # Max from fetched observation series
    reported_max_6h_f: Optional[float] = None        # NWS maxTemperatureLast6Hours
    readings: List[StationReading] = field(default_factory=list)
    last_updated: Optional[datetime] = None


# =============================================================================
# DATA CLASSES - MARKET
# =============================================================================


@dataclass
class MarketBracket:
    """
    A single bracket in a Kalshi temperature market.

    Bracket boundary logic:
    - BETWEEN: lower ≤ temp ≤ upper (inclusive both ends)
    - GREATER_THAN: temp > threshold (strictly greater, threshold does NOT win)
    - LESS_THAN: temp < threshold (strictly less, threshold does NOT win)
    """
    ticker: str                    # e.g., "KXHIGHNY-26JAN12-B54"
    event_ticker: str              # e.g., "KXHIGHNY-26JAN12"
    subtitle: str                  # e.g., "54° to 56°", "Above 58°", "Below 50°"
    bracket_type: BracketType      # between, greater_than, or less_than
    lower_bound: Optional[float]   # None for less_than brackets
    upper_bound: Optional[float]   # None for greater_than brackets
    yes_bid: int                   # Best bid in cents (0-99)
    yes_ask: int                   # Best ask in cents (1-100)
    last_price: int                # Last trade price in cents
    volume: int                    # Contracts traded
    implied_prob: float            # Mid-market probability (0.0 to 1.0)

    def contains_temp(self, temp: float) -> bool:
        """Check if a temperature would settle in this bracket."""
        if self.bracket_type == BracketType.BETWEEN:
            return self.lower_bound <= temp <= self.upper_bound
        elif self.bracket_type == BracketType.GREATER_THAN:
            return temp > self.lower_bound  # Strictly greater
        elif self.bracket_type == BracketType.LESS_THAN:
            return temp < self.upper_bound  # Strictly less
        return False


@dataclass
class TradingSignal:
    """
    A detected trading opportunity.

    Generated when model probability diverges significantly from market price.
    """
    bracket: MarketBracket         # The bracket to trade
    direction: str                 # "YES" or "NO"
    model_prob: float              # Our calculated probability (0.0 to 1.0)
    market_prob: float             # Market implied probability (0.0 to 1.0)
    edge: float                    # model_prob - market_prob (adjusted for direction)
    confidence: float              # 0.0 to 1.0 confidence score
    reasoning: str                 # Human-readable explanation


@dataclass
class OpenPosition:
    """
    A currently open account position in a Kalshi market.
    """
    ticker: str
    side: str                      # "YES" or "NO"
    contracts: int                 # Open quantity
    average_entry_price_cents: Optional[int] # Average fill/open price for this side
    event_ticker: Optional[str] = None
    subtitle: Optional[str] = None
    yes_bid: Optional[int] = None
    yes_ask: Optional[int] = None
    last_price: Optional[int] = None


@dataclass
class PositionRecommendation:
    """
    Deterministic recommendation for managing an open position.
    """
    position: OpenPosition
    model_yes_probability: Optional[float]
    side_probability: Optional[float]
    side_probability_change_pp: Optional[float]
    is_primary_outcome_position: bool
    top_model_probability: Optional[float]
    primary_gap_pp: Optional[float]
    fair_value_cents: Optional[float]
    trend_adjusted_fair_value_cents: Optional[float]
    liquidation_price_cents: Optional[int]
    liquidation_net_cents: Optional[float]
    edge_vs_liquidation_cents: Optional[float]
    action: str
    target_exit_price_cents: Optional[int]
    rationale: str
    distance_to_losing_edge_f: Optional[float] = None
    risk_buffer_f: Optional[float] = None
    final_window_open: bool = False
    edge_exceed_prob: Optional[float] = None


@dataclass
class TrajectoryAssessment:
    """
    Intraday trajectory-based assessment of whether the daily high is likely set.

    This is advisory metadata used to condition probabilities, not execution logic.
    """
    prob_high_already_set: float
    prob_exceed_observed_high: float
    expected_remaining_warming_f: float
    lock_confidence: float
    trend_f_per_hour: float
    reasoning: str


@dataclass
class PeakPrediction:
    """
    Early estimate of final daily high and confidence.

    This is used by the decision engine for timing/feasibility checks and by
    the dashboard for explicit "predicted high" output.
    """
    predicted_high_f: float
    confidence: float
    expected_remaining_warming_f: float
    trend_f_per_hour: float
    max_feasible_temp_f: Optional[float] = None
    reasoning: str = ""


@dataclass
class TradeDecisionSnapshot:
    """
    Structured summary of the current entry decision.

    status values:
    - "TRADE": all filters passed and a trade was selected
    - "SKIP": at least one quality/risk filter blocked entry
    """
    status: str
    reason: str
    ticker: Optional[str] = None
    bracket_subtitle: Optional[str] = None
    yes_price_cents: Optional[int] = None
    model_prob: Optional[float] = None
    market_prob: Optional[float] = None
    edge: Optional[float] = None
    confidence: Optional[float] = None
    size_contracts: Optional[int] = None
    target_exit_price_cents: Optional[int] = None
    max_feasible_temp_f: Optional[float] = None
    timing_lag_pp: Optional[float] = None
    model_trend_pp: Optional[float] = None


@dataclass
class MarketAnalysis:
    """
    Complete analysis for a single market/date.

    Combines all data sources and generated signals.
    """
    city: str                      # e.g., "NYC"
    target_date: str               # YYYY-MM-DD
    forecasts: List[TemperatureForecast]
    observation: Optional[DailyObservation]
    brackets: List[MarketBracket]
    signals: List[TradingSignal]
    forecast_mean: float           # Backward-compatible displayed model mean
    forecast_std: float            # Backward-compatible displayed model std dev
    analyzed_at: datetime
    raw_forecast_mean: Optional[float] = None      # Pre-observation combined mean
    raw_forecast_std: Optional[float] = None       # Pre-observation combined std
    adjusted_forecast_mean: Optional[float] = None # Post-observation/trajectory mean
    adjusted_forecast_std: Optional[float] = None  # Post-observation/trajectory std
    tomorrow_date: Optional[str] = None            # YYYY-MM-DD reference
    tomorrow_forecast_mean: Optional[float] = None # Forecast-only mean for tomorrow
    source_last_changed_at: Dict[str, datetime] = field(default_factory=dict)  # source -> last value-change time
    source_last_change_delta: Dict[str, Optional[float]] = field(default_factory=dict)  # source -> signed delta at last value-change
    model_probabilities: Dict[str, float] = field(default_factory=dict)  # ticker -> probability
    open_positions: List[PositionRecommendation] = field(default_factory=list)
    account_open_positions: List[OpenPosition] = field(default_factory=list)
    auto_trader_events: List[str] = field(default_factory=list)
    position_lifecycle_events: List[str] = field(default_factory=list)
    trajectory_assessment: Optional[TrajectoryAssessment] = None
    portfolio_totals: Dict[str, float] = field(default_factory=dict)
    paper_trading_totals: Dict[str, float] = field(default_factory=dict)
    peak_prediction: Optional[PeakPrediction] = None
    decision_snapshot: Optional[TradeDecisionSnapshot] = None


# =============================================================================
# ABSTRACT INTERFACES
# =============================================================================


class WeatherModelSource(ABC):
    """Interface for fetching weather model forecasts."""

    @abstractmethod
    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch all available forecasts for a target date."""
        pass

    @abstractmethod
    def get_latest_model_run_time(self) -> Optional[datetime]:
        """Get timestamp of most recent model run fetched."""
        pass


class StationDataSource(ABC):
    """Interface for fetching NWS station observations."""

    @abstractmethod
    def fetch_current_observations(self) -> List[StationReading]:
        """Fetch recent observations from the station."""
        pass

    @abstractmethod
    def get_daily_summary(self, date: str) -> Optional[DailyObservation]:
        """Get aggregated observation data for a specific date."""
        pass


class MarketDataSource(ABC):
    """Interface for fetching Kalshi market data."""

    @abstractmethod
    def fetch_brackets(self, target_date: str) -> List[MarketBracket]:
        """Fetch all brackets for a target date's temperature market."""
        pass

    @abstractmethod
    def get_market_status(self) -> Dict:
        """Get current market status (open/closed, etc.)."""
        pass


class EdgeEngine(ABC):
    """Interface for calculating edges and generating signals."""

    @abstractmethod
    def analyze(
        self,
        forecasts: List[TemperatureForecast],
        observation: Optional[DailyObservation],
        brackets: List[MarketBracket],
        min_edge: float = 0.08
    ) -> List[TradingSignal]:
        """Analyze market and return trading signals."""
        pass
