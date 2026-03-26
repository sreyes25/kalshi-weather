from kalshi_weather.core.models import BracketType, MarketBracket, OpenPosition
from kalshi_weather.engine.position_manager import evaluate_open_positions


def test_evaluate_open_positions_recommends_sell_when_fair_below_liquidation():
    positions = [
        OpenPosition(
            ticker="TEST-YES",
            side="YES",
            contracts=10,
            average_entry_price_cents=55,
            yes_bid=62,
            yes_ask=64,
        )
    ]
    model_probabilities = {"TEST-YES": 0.56}

    recs = evaluate_open_positions(positions, model_probabilities)

    assert len(recs) == 1
    assert recs[0].action == "SELL_NOW"
    assert recs[0].target_exit_price_cents == 62


def test_evaluate_open_positions_recommends_hold_when_fair_above_liquidation():
    positions = [
        OpenPosition(
            ticker="TEST-HOLD",
            side="YES",
            contracts=5,
            average_entry_price_cents=52,
            yes_bid=54,
            yes_ask=56,
        )
    ]
    model_probabilities = {"TEST-HOLD": 0.60}

    recs = evaluate_open_positions(positions, model_probabilities)

    assert recs[0].action == "HOLD_PRIMARY"
    assert recs[0].target_exit_price_cents >= 55
    assert recs[0].is_primary_outcome_position is True


def test_evaluate_open_positions_no_side_uses_inverse_for_no_contracts():
    positions = [
        OpenPosition(
            ticker="TEST-NO",
            side="NO",
            contracts=3,
            average_entry_price_cents=42,
            yes_bid=57,
            yes_ask=59,
        )
    ]
    model_probabilities = {"TEST-NO": 0.40}

    recs = evaluate_open_positions(positions, model_probabilities)

    assert recs[0].side_probability is not None
    assert abs(recs[0].side_probability - 0.60) < 1e-6


def test_evaluate_open_positions_handles_missing_model_probability():
    positions = [
        OpenPosition(
            ticker="TEST-MISSING",
            side="YES",
            contracts=1,
            average_entry_price_cents=50,
            yes_bid=49,
            yes_ask=51,
        )
    ]

    recs = evaluate_open_positions(positions, model_probabilities={})

    assert recs[0].action == "NO_MODEL"


def test_evaluate_open_positions_includes_side_probability_trend():
    positions = [
        OpenPosition(
            ticker="TEST-TREND",
            side="YES",
            contracts=2,
            average_entry_price_cents=24,
            yes_bid=27,
            yes_ask=29,
        )
    ]
    recs = evaluate_open_positions(
        positions,
        model_probabilities={"TEST-TREND": 0.31},
        previous_model_probabilities={"TEST-TREND": 0.27},
    )
    assert recs[0].side_probability_change_pp is not None
    assert recs[0].side_probability_change_pp > 0


def test_primary_locks_profit_when_prob_is_extreme_in_final_window():
    positions = [
        OpenPosition(
            ticker="TEST-PRIMARY",
            side="YES",
            contracts=4,
            average_entry_price_cents=61,
            yes_bid=95,
            yes_ask=97,
        )
    ]
    bracket = MarketBracket(
        ticker="TEST-PRIMARY",
        event_ticker="EVENT",
        subtitle="62° to 64°",
        bracket_type=BracketType.BETWEEN,
        lower_bound=62.0,
        upper_bound=64.0,
        yes_bid=95,
        yes_ask=97,
        last_price=96,
        volume=100,
        implied_prob=0.96,
    )
    recs = evaluate_open_positions(
        positions=positions,
        model_probabilities={"TEST-PRIMARY": 0.991},
        brackets_by_ticker={"TEST-PRIMARY": bracket},
        observed_high_f=63.8,
        final_window_open=True,
        primary_risk_buffer_f=0.8,
    )
    assert recs[0].action == "LOCK_PROFIT_PRIMARY"


def test_primary_remains_hold_when_final_window_closed():
    positions = [
        OpenPosition(
            ticker="TEST-PRIMARY-CLOSED",
            side="YES",
            contracts=4,
            average_entry_price_cents=61,
            yes_bid=95,
            yes_ask=97,
        )
    ]
    bracket = MarketBracket(
        ticker="TEST-PRIMARY-CLOSED",
        event_ticker="EVENT",
        subtitle="62° to 64°",
        bracket_type=BracketType.BETWEEN,
        lower_bound=62.0,
        upper_bound=64.0,
        yes_bid=95,
        yes_ask=97,
        last_price=96,
        volume=100,
        implied_prob=0.96,
    )
    recs = evaluate_open_positions(
        positions=positions,
        model_probabilities={"TEST-PRIMARY-CLOSED": 0.995},
        brackets_by_ticker={"TEST-PRIMARY-CLOSED": bracket},
        observed_high_f=63.9,
        final_window_open=False,
        primary_risk_buffer_f=0.8,
    )
    assert recs[0].action == "HOLD_PRIMARY"


def test_take_profit_fraction_triggers_sell_now():
    positions = [
        OpenPosition(
            ticker="TEST-TAKEPROFIT",
            side="YES",
            contracts=5,
            average_entry_price_cents=40,
            yes_bid=48,
            yes_ask=50,
        )
    ]
    recs = evaluate_open_positions(
        positions=positions,
        model_probabilities={"TEST-TAKEPROFIT": 0.62},
        take_profit_fraction=0.15,
    )
    assert recs[0].action == "SELL_NOW"


def test_confidence_drop_trigger_exits_position():
    positions = [
        OpenPosition(
            ticker="TEST-DROP",
            side="YES",
            contracts=3,
            average_entry_price_cents=51,
            yes_bid=38,
            yes_ask=40,
        )
    ]
    recs = evaluate_open_positions(
        positions=positions,
        model_probabilities={"TEST-DROP": 0.30},
        previous_model_probabilities={"TEST-DROP": 0.46},
        confidence_drop_trigger_pp=8.0,
        min_model_prob_after_drop=0.40,
    )
    assert recs[0].action == "SELL_NOW"
