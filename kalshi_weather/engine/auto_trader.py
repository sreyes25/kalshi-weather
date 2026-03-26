"""
Guarded auto-sell execution for open positions.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import List

from kalshi_weather.core.models import PositionRecommendation
from kalshi_weather.config.settings import BOT_CLIENT_TAG

logger = logging.getLogger(__name__)


def _build_client_order_id(prefix: str, ticker: str, side: str, price_cents: int, count: int) -> str:
    ticker_key = re.sub(r"[^A-Za-z0-9]", "", ticker)[-16:]
    raw = f"{prefix}-{ticker_key}-{side.lower()}-{price_cents}-{count}"
    return raw[:64]


def _is_transient_order_error(reason: str) -> bool:
    text = (reason or "").lower()
    return any(token in text for token in ("http 429", "http 500", "http 502", "http 503", "http 504", "timeout", "temporar"))


def execute_auto_sells(
    recommendations: List[PositionRecommendation],
    contract,
    *,
    enabled: bool,
    dry_run: bool,
    sell_on_wrong_position: bool,
    place_target_orders: bool,
    max_contracts: int,
    min_profit_cents: float,
    now_local: datetime,
    start_hour_local: int,
    start_minute_local: int,
    non_primary_streaks: dict[str, int],
    min_non_primary_cycles: int,
    min_primary_gap_pp: float,
    force_exit_hour_local: int,
    force_exit_minute_local: int,
    position_age_minutes: dict[str, float],
    min_hold_minutes: int,
    max_drawdown_fraction: float,
    allow_primary_scalp_targets: bool,
    primary_scalp_max_hold_minutes: int,
    respect_sell_now_actions: bool = True,
) -> List[str]:
    """
    Execute auto-sell actions with strict safeguards.
    """
    events: List[str] = []
    if not enabled:
        return events
    if (now_local.hour, now_local.minute) < (start_hour_local, start_minute_local):
        events.append(
            f"SKIP auto-sell: locked until {start_hour_local:02d}:{start_minute_local:02d} local "
            "(day-trader mode: hold primary bracket, avoid early churn)"
        )
        return events
    in_force_exit_window = (now_local.hour, now_local.minute) >= (
        force_exit_hour_local,
        force_exit_minute_local,
    )

    for rec in recommendations:
        position = rec.position
        if position.contracts <= 0:
            continue
        if position.contracts > max_contracts:
            message = (
                f"SKIP {position.ticker}: qty {position.contracts} > max {max_contracts}"
            )
            logger.warning(message)
            events.append(message)
            continue

        side_upper = position.side.upper()
        streak_key = f"{position.ticker}|{position.side.upper()}"
        streak = non_primary_streaks.get(streak_key, 0)
        age_min = position_age_minutes.get(streak_key, 0.0)
        trigger_wrong = (
            sell_on_wrong_position
            and side_upper == "YES"
            and not rec.is_primary_outcome_position
        )
        trigger_sell_now_action = (
            respect_sell_now_actions
            and rec.action == "SELL_NOW"
        )
        trigger_wrong_non_yes = sell_on_wrong_position and side_upper != "YES" and rec.action == "SELL_NOW"
        trigger_no_model_scalp = (
            place_target_orders
            and rec.action == "NO_MODEL"
            and side_upper == "YES"
            and position.average_entry_price_cents is not None
            and rec.liquidation_net_cents is not None
            and rec.liquidation_net_cents >= float(position.average_entry_price_cents + min_profit_cents)
        )
        trigger_target = (
            place_target_orders
            and not rec.is_primary_outcome_position
            and rec.action in {"HOLD", "HOLD_FOR_TARGET"}
        )
        trigger_primary_lock = (
            place_target_orders
            and rec.is_primary_outcome_position
            and side_upper == "YES"
            and rec.action == "LOCK_PROFIT_PRIMARY"
        )
        trigger_primary_scalp_target = (
            allow_primary_scalp_targets
            and place_target_orders
            and rec.is_primary_outcome_position
            and side_upper == "YES"
            and rec.action == "HOLD_PRIMARY"
            and rec.target_exit_price_cents is not None
        )
        trigger_primary_scalp_timeout = (
            allow_primary_scalp_targets
            and rec.is_primary_outcome_position
            and side_upper == "YES"
            and rec.action == "HOLD_PRIMARY"
            and age_min >= float(max(1, primary_scalp_max_hold_minutes))
        )
        trigger_force_exit = (
            in_force_exit_window
            and side_upper == "YES"
            and not rec.is_primary_outcome_position
        )
        if not (
            trigger_wrong
            or trigger_sell_now_action
            or trigger_wrong_non_yes
            or trigger_no_model_scalp
            or trigger_target
            or trigger_primary_lock
            or trigger_primary_scalp_target
            or trigger_primary_scalp_timeout
            or trigger_force_exit
        ):
            events.append(f"SKIP {position.ticker}: no trigger (action={rec.action})")
            continue

        if rec.is_primary_outcome_position and not (
            trigger_primary_lock
            or trigger_primary_scalp_target
            or trigger_primary_scalp_timeout
            or trigger_sell_now_action
            or trigger_force_exit
        ):
            events.append(
                f"SKIP {position.ticker}: primary-outcome position (hold for settlement)"
            )
            continue
        if not (trigger_force_exit or trigger_primary_lock or trigger_primary_scalp_timeout or trigger_sell_now_action) and age_min < float(min_hold_minutes):
            events.append(
                f"SKIP {position.ticker}: age {age_min:.1f}m < hold-min {min_hold_minutes}m"
            )
            continue
        if (
            (trigger_wrong or trigger_target)
            and not trigger_force_exit
            and not trigger_primary_lock
            and side_upper == "YES"
        ):
            if streak < min_non_primary_cycles:
                events.append(
                    f"SKIP {position.ticker}: non-primary streak {streak} < required {min_non_primary_cycles}"
                )
                continue
            if rec.primary_gap_pp is not None and rec.primary_gap_pp < min_primary_gap_pp:
                events.append(
                    f"SKIP {position.ticker}: ambiguity guard (model lead {rec.primary_gap_pp:.1f}pp < required {min_primary_gap_pp:.1f}pp)"
                )
                continue
            if (
                position.average_entry_price_cents is not None
                and rec.liquidation_net_cents is not None
                and position.average_entry_price_cents > 0
            ):
                drawdown_fraction = max(
                    0.0,
                    (float(position.average_entry_price_cents) - rec.liquidation_net_cents)
                    / float(position.average_entry_price_cents),
                )
                if drawdown_fraction < max_drawdown_fraction:
                    events.append(
                        f"SKIP {position.ticker}: drawdown {drawdown_fraction:.2f} < threshold {max_drawdown_fraction:.2f}"
                    )
                    continue

        target_price = None
        if trigger_force_exit:
            target_price = rec.liquidation_price_cents
        elif trigger_wrong or trigger_wrong_non_yes or trigger_sell_now_action or trigger_no_model_scalp:
            target_price = rec.liquidation_price_cents
        elif trigger_primary_lock or trigger_primary_scalp_target:
            target_price = rec.target_exit_price_cents
        elif trigger_primary_scalp_timeout:
            target_price = rec.liquidation_price_cents
        else:
            target_price = rec.target_exit_price_cents
        if target_price is None:
            events.append(f"SKIP {position.ticker}: no target price")
            continue
        # Avoid repeatedly submitting no-liquidity IOC sells at 1c.
        if (
            rec.liquidation_price_cents is not None
            and int(rec.liquidation_price_cents) <= 1
            and not (trigger_force_exit or trigger_primary_scalp_timeout)
            and (trigger_wrong or trigger_wrong_non_yes or trigger_sell_now_action)
        ):
            events.append(
                f"SKIP {position.ticker}: thin book (liquidation {int(rec.liquidation_price_cents)}c) - waiting for bid support"
            )
            continue
        model_target_price = int(target_price)
        if (
            trigger_target
            or trigger_primary_lock
            or trigger_primary_scalp_target
            or trigger_primary_scalp_timeout
        ) and rec.liquidation_price_cents is not None:
            # Keep target sells adaptive to current market to avoid stale low limits.
            target_price = max(int(target_price), int(rec.liquidation_price_cents))
            # Safety: do not auto-target-sell below entry basis + min desired net profit.
            if (
                position.average_entry_price_cents is not None
                and rec.liquidation_net_cents is not None
                and rec.liquidation_net_cents < float(position.average_entry_price_cents + min_profit_cents)
            ):
                events.append(
                    f"SKIP {position.ticker}: target sell blocked (net {rec.liquidation_net_cents:.1f}c < entry+profit {(position.average_entry_price_cents + min_profit_cents):.1f}c)"
                )
                continue

        # Skip if we already have a matching resting reduce-only sell.
        if contract.has_resting_reduce_like_order(position.ticker, position.side, int(target_price)):
            events.append(
                f"SKIP {position.ticker}: matching resting reduce-only sell exists at {int(target_price)}c"
            )
            continue

        if trigger_primary_scalp_timeout:
            order_prefix = "autoscalptimeout"
        elif trigger_primary_lock:
            order_prefix = "autolock"
        elif trigger_wrong:
            order_prefix = "autoexit"
        elif trigger_no_model_scalp:
            order_prefix = "autonomodel"
        else:
            order_prefix = "autotarget"
        client_order_id = _build_client_order_id(
            f"{BOT_CLIENT_TAG}{order_prefix}",
            position.ticker,
            position.side,
            int(target_price),
            position.contracts,
        )

        if dry_run:
            message = (
                f"DRY-RUN place sell {position.ticker} {position.side} "
                f"qty={position.contracts} px={int(target_price)}c "
                f"(action={rec.action}, model_target={model_target_price}c)"
            )
            logger.info(message)
            events.append(message)
            continue

        ok, reason = contract.place_reduce_only_sell_limit(
            ticker=position.ticker,
            side=position.side,
            count=position.contracts,
            limit_price_cents=int(target_price),
            client_order_id=client_order_id,
        )
        if ok:
            if "duplicate client_order_id" in reason:
                message = (
                    f"SKIP {position.ticker}: duplicate client_order_id "
                    f"(already submitted this cycle pattern)"
                )
                events.append(message)
            else:
                message = (
                    f"SUBMITTED_IOC sell {position.ticker} {position.side} qty={position.contracts} "
                    f"px={int(target_price)}c (action={rec.action}, model_target={model_target_price}c, via={reason}) at "
                    f"{datetime.now().isoformat(timespec='seconds')}"
                )
                logger.info(message)
                events.append(message)
                events.append(
                    f"PENDING_FILL_CHECK {position.ticker}: confirm via qty drop (REDUCED/SOLD event)"
                )
        else:
            message = (
                f"FAILED sell {position.ticker} {position.side} qty={position.contracts} "
                f"px={int(target_price)}c (action={rec.action}, model_target={model_target_price}c) reason={reason}"
            )
            events.append(message)
            if _is_transient_order_error(reason):
                events.append(
                    f"BACKOFF {position.ticker}: transient order error detected; retry next cycle with fresh quotes"
                )
    return events
