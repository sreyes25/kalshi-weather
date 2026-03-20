"""
Edge Detector Module (Module 3A & 3B)

Responsible for:
1. Orchestrating the probability calculation (combining forecasts + adjusting for observations).
2. Comparing model probabilities vs market prices.
3. Identifying trading edges using EV = model_prob - market_prob.
4. Generating human-readable trading signals.
"""

import logging
from typing import List, Optional

from kalshi_weather.core.models import (
    EdgeEngine,
    TemperatureForecast,
    DailyObservation,
    MarketBracket,
    TradingSignal,
)
from kalshi_weather.config.settings import (
    MIN_EDGE_THRESHOLD,
    KALSHI_FEE_RATE,
)
from kalshi_weather.engine.probability import (
    combine_forecasts,
    adjust_forecast_with_observations,
    BracketProbabilityCalculator,
)

logger = logging.getLogger(__name__)


class EdgeDetector(EdgeEngine):
    """
    Implementation of the Edge Engine.
    
    Orchestrates the pipeline:
    Forecasts -> CombinedForecast -> Observation Adjustment -> AdjustedForecast -> Bracket Probabilities -> Signals
    """

    def __init__(self, fee_rate: float = KALSHI_FEE_RATE):
        # Kept for backward compatibility; current EV logic uses raw probability edge.
        self.fee_rate = fee_rate
        self.prob_calculator = BracketProbabilityCalculator()

    def analyze(
        self,
        forecasts: List[TemperatureForecast],
        observation: Optional[DailyObservation],
        brackets: List[MarketBracket],
        min_edge: float = MIN_EDGE_THRESHOLD
    ) -> List[TradingSignal]:
        """
        Analyze market and return trading signals.
        
        Args:
            forecasts: List of weather forecasts.
            observation: Current day's station observation (optional).
            brackets: List of Kalshi market brackets.
            min_edge: Minimum edge required to generate a signal.
            
        Returns:
            List of TradingSignal objects sorted by edge strength.
        """
        if not forecasts:
            logger.warning("No forecasts provided for edge analysis")
            return []

        if not brackets:
            logger.warning("No brackets provided for edge analysis")
            return []

        # 1. Combine Forecasts
        logger.info(f"Combining {len(forecasts)} forecasts...")
        combined = combine_forecasts(forecasts)
        if not combined:
            logger.error("Failed to combine forecasts")
            return []

        # 2. Adjust for Observations
        logger.info("Adjusting for observations...")
        adjusted = adjust_forecast_with_observations(combined, observation)

        # 3. Calculate Model Probabilities
        logger.info("Calculating bracket probabilities...")
        # We use the calculator directly on the adjusted forecast
        model_probs = self.prob_calculator.calculate_from_adjusted_forecast(
            adjusted, brackets
        )

        # 4. Find Edges
        signals = []
        for bp in model_probs:
            ev = bp.model_prob - bp.market_prob
            reasoning = self._build_reasoning(bp.bracket.subtitle, ev, bp.model_prob, bp.market_prob, adjusted)

            if ev > min_edge:
                signals.append(TradingSignal(
                    bracket=bp.bracket,
                    direction="BUY",
                    model_prob=bp.model_prob,
                    market_prob=bp.market_prob,
                    edge=ev,
                    confidence=self._calculate_confidence(abs(ev), adjusted.std_dev),
                    reasoning=reasoning,
                ))
            elif ev < -min_edge:
                signals.append(TradingSignal(
                    bracket=bp.bracket,
                    direction="SELL",
                    model_prob=bp.model_prob,
                    market_prob=bp.market_prob,
                    edge=ev,
                    confidence=self._calculate_confidence(abs(ev), adjusted.std_dev),
                    reasoning=reasoning,
                ))

        # Sort signals by edge strength (descending)
        signals.sort(key=lambda s: abs(s.edge), reverse=True)
        
        return signals

    def _calculate_confidence(self, edge: float, std_dev: float) -> float:
        """
        Calculate a confidence score (0-1) for the signal.
        
        Factors:
        - Magnitude of edge (larger is better)
        - Forecast uncertainty (lower std dev is better)
        """
        # 1. Edge Score: Map 0.05-0.20 edge to 0.5-1.0
        edge_score = min(1.0, max(0.0, (edge - 0.05) / 0.15 * 0.5 + 0.5))
        
        # 2. Uncertainty Score: Map 1.5-5.0 std dev to 1.0-0.5
        # Lower std dev = higher confidence
        unc_score = max(0.0, min(1.0, 1.0 - (std_dev - 1.5) / 3.5 * 0.5))
        
        return (edge_score + unc_score) / 2.0

    def _build_reasoning(
        self,
        subtitle: str,
        ev: float,
        model_prob: float,
        market_prob: float,
        adjusted,
    ) -> str:
        action = "BUY" if ev > 0 else "SELL"
        base = (
            f"{action} {subtitle}: EV {ev:+.1%} "
            f"(Model {model_prob:.1%} vs Market {market_prob:.1%})"
        )
        traj = getattr(adjusted, "trajectory_assessment", None)
        if not traj:
            return base

        if getattr(adjusted, "lock_mode_active", False):
            return (
                f"{base}. Observed high likely already set; falling late-day "
                f"temperatures reduce upside probability."
            )
        if traj.trend_f_per_hour > 0.2 and traj.prob_exceed_observed_high >= 0.08:
            return (
                f"{base}. Current temperature still rising near observed high; "
                f"some late upside remains plausible."
            )
        return f"{base}. {traj.reasoning}"
