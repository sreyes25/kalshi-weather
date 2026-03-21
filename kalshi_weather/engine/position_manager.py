"""
Deterministic open-position evaluation utilities.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from kalshi_weather.core.models import OpenPosition, PositionRecommendation


def _clamp_cents(value: float) -> int:
    return max(1, min(99, int(round(value))))


def _liquidation_price_cents(position: OpenPosition) -> Optional[int]:
    if position.side.upper() == "YES":
        return position.yes_bid

    if position.side.upper() == "NO":
        if position.yes_ask is None:
            return None
        return max(0, 100 - position.yes_ask)

    return None


def evaluate_open_positions(
    positions: List[OpenPosition],
    model_probabilities: Dict[str, float],
    hold_edge_threshold_cents: float = 2.0,
    sell_edge_threshold_cents: float = -2.0,
) -> List[PositionRecommendation]:
    """
    Score open positions with deterministic hold/sell logic.

    Decision framework:
    - fair_value = side_probability * 100
    - edge_vs_liquidation = fair_value - liquidation_price
    - SELL_NOW when edge_vs_liquidation <= sell threshold
    - HOLD when edge_vs_liquidation >= hold threshold
    - otherwise HOLD_FOR_TARGET around fair_value - 1c
    """
    recommendations: List[PositionRecommendation] = []

    for position in positions:
        model_yes_prob = model_probabilities.get(position.ticker)
        if model_yes_prob is None:
            recommendations.append(
                PositionRecommendation(
                    position=position,
                    model_yes_probability=None,
                    side_probability=None,
                    fair_value_cents=None,
                    liquidation_price_cents=_liquidation_price_cents(position),
                    edge_vs_liquidation_cents=None,
                    action="NO_MODEL",
                    target_exit_price_cents=None,
                    rationale="No model probability available for this ticker yet.",
                )
            )
            continue

        side = position.side.upper()
        side_prob = model_yes_prob if side == "YES" else 1.0 - model_yes_prob
        fair_value = side_prob * 100.0
        liquidation = _liquidation_price_cents(position)

        if liquidation is None:
            recommendations.append(
                PositionRecommendation(
                    position=position,
                    model_yes_probability=model_yes_prob,
                    side_probability=side_prob,
                    fair_value_cents=fair_value,
                    liquidation_price_cents=None,
                    edge_vs_liquidation_cents=None,
                    action="NO_QUOTE",
                    target_exit_price_cents=None,
                    rationale="Missing live quote for liquidation price.",
                )
            )
            continue

        edge = fair_value - float(liquidation)
        side_entry_prob = None
        if position.average_entry_price_cents is not None:
            entry_prob = position.average_entry_price_cents / 100.0
            side_entry_prob = entry_prob if side == "YES" else (1.0 - entry_prob)

        if edge <= sell_edge_threshold_cents:
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Model fair value {fair_value:.1f}c is below liquidation {liquidation}c "
                f"by {abs(edge):.1f}c."
            )
        elif edge >= hold_edge_threshold_cents:
            action = "HOLD"
            min_target = (
                (position.average_entry_price_cents + 1.0)
                if position.average_entry_price_cents is not None
                else (fair_value - 1.0)
            )
            target_exit = _clamp_cents(max(fair_value - 1.0, min_target))
            rationale = (
                f"Model fair value {fair_value:.1f}c is above liquidation {liquidation}c "
                f"by {edge:.1f}c."
            )
        else:
            action = "HOLD_FOR_TARGET"
            min_target = (
                float(position.average_entry_price_cents)
                if position.average_entry_price_cents is not None
                else (fair_value - 1.0)
            )
            target_exit = _clamp_cents(max(fair_value - 1.0, min_target))
            rationale = (
                f"Value is near fair ({edge:+.1f}c vs liquidation); wait for {target_exit}c "
                f"or re-evaluate on model move."
            )

        if side_entry_prob is not None:
            rationale = (
                f"{rationale} Entry side probability was {side_entry_prob:.1%}; "
                f"model side probability is {side_prob:.1%}."
            )
        else:
            rationale = f"{rationale} Model side probability is {side_prob:.1%}."

        recommendations.append(
            PositionRecommendation(
                position=position,
                model_yes_probability=model_yes_prob,
                side_probability=side_prob,
                fair_value_cents=fair_value,
                liquidation_price_cents=liquidation,
                edge_vs_liquidation_cents=edge,
                action=action,
                target_exit_price_cents=target_exit,
                rationale=rationale,
            )
        )

    recommendations.sort(
        key=lambda r: (
            2 if r.action == "SELL_NOW" else 1 if r.action == "HOLD_FOR_TARGET" else 0,
            abs(r.edge_vs_liquidation_cents) if r.edge_vs_liquidation_cents is not None else -1.0,
        ),
        reverse=True,
    )
    return recommendations
