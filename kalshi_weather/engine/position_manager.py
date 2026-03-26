"""
Deterministic open-position evaluation utilities.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from kalshi_weather.core.models import BracketType, MarketBracket, OpenPosition, PositionRecommendation


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


def _distance_to_losing_edge_f(
    *,
    bracket: Optional[MarketBracket],
    observed_high_f: Optional[float],
) -> Optional[float]:
    """Distance from observed high to the next losing edge for a YES bracket."""
    if bracket is None or observed_high_f is None:
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


def evaluate_open_positions(
    positions: List[OpenPosition],
    model_probabilities: Dict[str, float],
    previous_model_probabilities: Optional[Dict[str, float]] = None,
    hold_edge_threshold_cents: float = 3.0,
    sell_edge_threshold_cents: float = -3.0,
    exit_fee_rate: float = 0.0,
    trend_weight_cents_per_pp: float = 0.22,
    max_trend_adjustment_cents: float = 1.5,
    brackets_by_ticker: Optional[Dict[str, MarketBracket]] = None,
    observed_high_f: Optional[float] = None,
    final_window_open: bool = False,
    primary_profit_lock_warn_prob: float = 0.97,
    primary_profit_lock_trigger_prob: float = 0.99,
    primary_risk_buffer_f: float = 1.0,
    primary_flip_risk_by_ticker: Optional[Dict[str, bool]] = None,
    primary_edge_exceed_prob_by_ticker: Optional[Dict[str, float]] = None,
    stop_loss_dollars: Optional[float] = None,
    take_profit_dollars: Optional[float] = None,
    take_profit_fraction: Optional[float] = None,
    confidence_drop_trigger_pp: Optional[float] = None,
    min_model_prob_after_drop: float = 0.0,
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
    top_ticker: Optional[str] = None
    top_prob: Optional[float] = None
    if model_probabilities:
        top_ticker, top_prob = max(model_probabilities.items(), key=lambda item: item[1])

    for position in positions:
        model_yes_prob = model_probabilities.get(position.ticker)
        is_primary_outcome = (
            position.side.upper() == "YES"
            and top_ticker is not None
            and position.ticker == top_ticker
        )
        if model_yes_prob is None:
            primary_gap_pp = None
            if top_prob is not None:
                primary_gap_pp = None
            recommendations.append(
                PositionRecommendation(
                    position=position,
                    model_yes_probability=None,
                    side_probability=None,
                    side_probability_change_pp=None,
                    is_primary_outcome_position=is_primary_outcome,
                    top_model_probability=top_prob,
                    primary_gap_pp=primary_gap_pp,
                    fair_value_cents=None,
                    trend_adjusted_fair_value_cents=None,
                    liquidation_price_cents=_liquidation_price_cents(position),
                    liquidation_net_cents=None,
                    edge_vs_liquidation_cents=None,
                    action="NO_MODEL",
                    target_exit_price_cents=None,
                    rationale="No model probability available for this ticker yet.",
                )
            )
            continue

        side = position.side.upper()
        side_prob = model_yes_prob if side == "YES" else 1.0 - model_yes_prob
        bracket = (brackets_by_ticker or {}).get(position.ticker)
        distance_to_losing_edge = _distance_to_losing_edge_f(
            bracket=bracket,
            observed_high_f=observed_high_f,
        )
        explicit_flip_risk = (
            (primary_flip_risk_by_ticker or {}).get(position.ticker)
            if primary_flip_risk_by_ticker is not None
            else None
        )
        flip_risk = explicit_flip_risk
        if flip_risk is None and distance_to_losing_edge is not None:
            flip_risk = distance_to_losing_edge <= primary_risk_buffer_f
        edge_exceed_prob = (
            (primary_edge_exceed_prob_by_ticker or {}).get(position.ticker)
            if primary_edge_exceed_prob_by_ticker is not None
            else None
        )
        primary_gap_pp = None
        if top_prob is not None:
            primary_gap_pp = (top_prob - side_prob) * 100.0
        fair_value = side_prob * 100.0
        prev_yes_prob = None if previous_model_probabilities is None else previous_model_probabilities.get(position.ticker)
        side_prob_change_pp = None
        if prev_yes_prob is not None:
            prev_side_prob = prev_yes_prob if side == "YES" else 1.0 - prev_yes_prob
            side_prob_change_pp = (side_prob - prev_side_prob) * 100.0

        trend_adjustment = 0.0
        if side_prob_change_pp is not None:
            # Ignore tiny probability jitter to reduce intraday churn.
            if abs(side_prob_change_pp) >= 0.35:
                trend_adjustment = side_prob_change_pp * trend_weight_cents_per_pp
            trend_adjustment = max(-max_trend_adjustment_cents, min(max_trend_adjustment_cents, trend_adjustment))
        trend_fair_value = fair_value + trend_adjustment
        liquidation = _liquidation_price_cents(position)

        if liquidation is None:
            recommendations.append(
                PositionRecommendation(
                    position=position,
                    model_yes_probability=model_yes_prob,
                    side_probability=side_prob,
                    side_probability_change_pp=side_prob_change_pp,
                    is_primary_outcome_position=is_primary_outcome,
                    top_model_probability=top_prob,
                    primary_gap_pp=primary_gap_pp,
                    fair_value_cents=fair_value,
                    trend_adjusted_fair_value_cents=trend_fair_value,
                    liquidation_price_cents=None,
                    liquidation_net_cents=None,
                    edge_vs_liquidation_cents=None,
                    action="NO_QUOTE",
                    target_exit_price_cents=None,
                    rationale="Missing live quote for liquidation price.",
                )
            )
            continue

        liquidation_net = float(liquidation) * max(0.0, 1.0 - exit_fee_rate)
        edge = trend_fair_value - liquidation_net
        pnl_now_dollars = None
        if position.average_entry_price_cents is not None:
            pnl_now_dollars = (
                (liquidation_net - float(position.average_entry_price_cents))
                * position.contracts
                / 100.0
            )
        side_entry_prob = None
        if position.average_entry_price_cents is not None:
            entry_prob = position.average_entry_price_cents / 100.0
            side_entry_prob = entry_prob if side == "YES" else (1.0 - entry_prob)

        if (
            pnl_now_dollars is not None
            and stop_loss_dollars is not None
            and pnl_now_dollars <= -abs(float(stop_loss_dollars))
        ):
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Stop-loss triggered: unrealized P&L {pnl_now_dollars:+.2f} <= "
                f"-${abs(float(stop_loss_dollars)):.2f}."
            )
        elif (
            pnl_now_dollars is not None
            and take_profit_dollars is not None
            and pnl_now_dollars >= abs(float(take_profit_dollars))
        ):
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Take-profit triggered: unrealized P&L {pnl_now_dollars:+.2f} >= "
                f"${abs(float(take_profit_dollars)):.2f}."
            )
        elif (
            position.average_entry_price_cents is not None
            and take_profit_fraction is not None
            and float(position.average_entry_price_cents) > 0.0
            and liquidation_net >= float(position.average_entry_price_cents) * (1.0 + max(0.0, float(take_profit_fraction)))
        ):
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Take-profit fraction hit: liquidation {liquidation_net:.1f}c >= "
                f"entry {float(position.average_entry_price_cents):.1f}c * "
                f"(1 + {max(0.0, float(take_profit_fraction)):.2f})."
            )
        elif (
            side_prob_change_pp is not None
            and confidence_drop_trigger_pp is not None
            and side_prob_change_pp <= -abs(float(confidence_drop_trigger_pp))
            and side_prob <= max(0.0, min(1.0, float(min_model_prob_after_drop)))
        ):
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Model confidence dropped {side_prob_change_pp:.1f}pp and current side probability "
                f"{side_prob:.1%} <= floor {float(min_model_prob_after_drop):.1%}; exit to cut risk."
            )
        elif is_primary_outcome and side == "YES" and edge <= sell_edge_threshold_cents:
            action = "SELL_NOW"
            target_exit = liquidation
            rationale = (
                f"Primary bracket lost model support: trend-adjusted fair value {trend_fair_value:.1f}c "
                f"is below liquidation {liquidation}c by {abs(edge):.1f}c."
            )
        elif is_primary_outcome and side == "YES":
            target_exit = _clamp_cents(max(fair_value - 1.0, float(liquidation or 1)))
            force_lock_primary = final_window_open and side_prob >= primary_profit_lock_trigger_prob
            risk_lock_primary = (
                final_window_open
                and side_prob >= primary_profit_lock_warn_prob
                and bool(flip_risk)
            )
            if force_lock_primary or risk_lock_primary:
                action = "LOCK_PROFIT_PRIMARY"
                exceed_note = ""
                if edge_exceed_prob is not None:
                    exceed_note = f" Edge-exceed risk={edge_exceed_prob:.1%}."
                rationale = (
                    f"Primary bracket reached {side_prob:.1%} with unresolved final-window risk; "
                    f"lock profits via reduce-only target {target_exit}c.{exceed_note}"
                )
            else:
                action = "HOLD_PRIMARY"
                rationale = (
                    "Primary final-outcome bracket for current model; hold-to-settlement mode active."
                )
        elif edge <= sell_edge_threshold_cents:
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
                f"Trend-adjusted fair value {trend_fair_value:.1f}c is above liquidation {liquidation}c "
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
        if side_prob_change_pp is not None:
            rationale += f" Side probability trend: {side_prob_change_pp:+.1f}pp since last cycle."
        if position.average_entry_price_cents is not None and side == "YES":
            max_settlement_profit = (100 - position.average_entry_price_cents) * position.contracts
            rationale += (
                f" If this bracket settles, gross settlement profit is ~${max_settlement_profit / 100:.2f}."
            )

        recommendations.append(
            PositionRecommendation(
                position=position,
                model_yes_probability=model_yes_prob,
                side_probability=side_prob,
                side_probability_change_pp=side_prob_change_pp,
                is_primary_outcome_position=is_primary_outcome,
                top_model_probability=top_prob,
                primary_gap_pp=primary_gap_pp,
                fair_value_cents=fair_value,
                trend_adjusted_fair_value_cents=trend_fair_value,
                liquidation_price_cents=liquidation,
                liquidation_net_cents=liquidation_net,
                edge_vs_liquidation_cents=edge,
                action=action,
                target_exit_price_cents=target_exit,
                rationale=rationale,
                distance_to_losing_edge_f=distance_to_losing_edge,
                risk_buffer_f=primary_risk_buffer_f if distance_to_losing_edge is not None else None,
                final_window_open=final_window_open,
                edge_exceed_prob=edge_exceed_prob,
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
