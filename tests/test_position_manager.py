from kalshi_weather.core.models import OpenPosition
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

    assert recs[0].action == "HOLD"
    assert recs[0].target_exit_price_cents >= 55


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
