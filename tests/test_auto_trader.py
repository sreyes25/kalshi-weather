from datetime import datetime

from kalshi_weather.core.models import OpenPosition, PositionRecommendation
from kalshi_weather.engine.auto_trader import execute_auto_sells


class _DummyContract:
    def has_resting_reduce_like_order(self, ticker: str, side: str, price_cents: int) -> bool:
        return False

    def place_reduce_only_sell_limit(self, **kwargs):
        return (True, "ok")


def _make_rec(
    *,
    ticker: str,
    action: str,
    is_primary: bool,
    liquidation_cents: int,
    target_cents: int | None,
    entry_cents: int = 50,
    contracts: int = 2,
) -> PositionRecommendation:
    return PositionRecommendation(
        position=OpenPosition(
            ticker=ticker,
            side="YES",
            contracts=contracts,
            average_entry_price_cents=entry_cents,
            yes_bid=liquidation_cents,
            yes_ask=min(99, liquidation_cents + 2),
        ),
        model_yes_probability=0.72,
        side_probability=0.72,
        side_probability_change_pp=0.0,
        is_primary_outcome_position=is_primary,
        top_model_probability=0.72,
        primary_gap_pp=0.0,
        fair_value_cents=72.0,
        trend_adjusted_fair_value_cents=72.0,
        liquidation_price_cents=liquidation_cents,
        liquidation_net_cents=float(liquidation_cents),
        edge_vs_liquidation_cents=2.0,
        action=action,
        target_exit_price_cents=target_cents,
        rationale="test",
    )


def test_primary_sell_now_not_blocked_by_hold_gate():
    rec = _make_rec(
        ticker="T-PRIMARY-SELLNOW",
        action="SELL_NOW",
        is_primary=True,
        liquidation_cents=61,
        target_cents=70,
        entry_cents=58,
    )
    events = execute_auto_sells(
        recommendations=[rec],
        contract=_DummyContract(),
        enabled=True,
        dry_run=True,
        sell_on_wrong_position=False,
        place_target_orders=True,
        max_contracts=250,
        min_profit_cents=1.0,
        now_local=datetime(2026, 3, 24, 16, 30),
        start_hour_local=0,
        start_minute_local=0,
        non_primary_streaks={},
        min_non_primary_cycles=1,
        min_primary_gap_pp=6.0,
        force_exit_hour_local=23,
        force_exit_minute_local=59,
        position_age_minutes={"T-PRIMARY-SELLNOW|YES": 5.0},
        min_hold_minutes=30,
        max_drawdown_fraction=0.5,
        allow_primary_scalp_targets=True,
        primary_scalp_max_hold_minutes=60,
        respect_sell_now_actions=True,
    )
    assert any("DRY-RUN place sell T-PRIMARY-SELLNOW YES" in e for e in events)


def test_primary_lock_target_not_overridden_by_scalp_timeout():
    rec = _make_rec(
        ticker="T-PRIMARY-LOCK",
        action="LOCK_PROFIT_PRIMARY",
        is_primary=True,
        liquidation_cents=70,
        target_cents=82,
        entry_cents=60,
    )
    events = execute_auto_sells(
        recommendations=[rec],
        contract=_DummyContract(),
        enabled=True,
        dry_run=True,
        sell_on_wrong_position=False,
        place_target_orders=True,
        max_contracts=250,
        min_profit_cents=1.0,
        now_local=datetime(2026, 3, 24, 18, 30),
        start_hour_local=0,
        start_minute_local=0,
        non_primary_streaks={},
        min_non_primary_cycles=1,
        min_primary_gap_pp=6.0,
        force_exit_hour_local=23,
        force_exit_minute_local=59,
        position_age_minutes={"T-PRIMARY-LOCK|YES": 120.0},
        min_hold_minutes=30,
        max_drawdown_fraction=0.5,
        allow_primary_scalp_targets=True,
        primary_scalp_max_hold_minutes=60,
        respect_sell_now_actions=True,
    )
    assert any("DRY-RUN place sell T-PRIMARY-LOCK YES qty=2 px=82c" in e for e in events)
