from datetime import datetime
from zoneinfo import ZoneInfo

from kalshi_weather.core.models import (
    BracketType,
    DailyObservation,
    MarketBracket,
    TradeDecisionSnapshot,
    TradingSignal,
    TrajectoryAssessment,
)
from kalshi_weather.engine.decision_engine import DecisionEngine


NY_TZ = ZoneInfo("America/New_York")


def _make_signal(
    *,
    ticker: str,
    subtitle: str,
    model_prob: float,
    market_prob: float,
    edge: float,
    lower: float,
    upper: float,
    ask: int = 40,
    bid: int = 38,
    volume: int = 100,
    confidence: float = 0.70,
) -> TradingSignal:
    bracket = MarketBracket(
        ticker=ticker,
        event_ticker="EVENT",
        subtitle=subtitle,
        bracket_type=BracketType.BETWEEN,
        lower_bound=lower,
        upper_bound=upper,
        yes_bid=bid,
        yes_ask=ask,
        last_price=ask,
        volume=volume,
        implied_prob=market_prob,
    )
    return TradingSignal(
        bracket=bracket,
        direction="BUY",
        model_prob=model_prob,
        market_prob=market_prob,
        edge=edge,
        confidence=confidence,
        reasoning="test",
    )


def _engine(**overrides) -> DecisionEngine:
    params = dict(
        min_confidence_threshold=0.25,
        min_edge_threshold=0.05,
        max_risk_fraction=0.20,
        target_profit_fraction=0.15,
        max_trades_per_day=5,
        min_model_trend_pp=0.8,
        min_market_lag_pp=0.6,
    )
    params.update(overrides)
    return DecisionEngine(**params)


def _trajectory(expected_warm: float = 1.0) -> TrajectoryAssessment:
    return TrajectoryAssessment(
        prob_high_already_set=0.40,
        prob_exceed_observed_high=0.32,
        expected_remaining_warming_f=expected_warm,
        lock_confidence=0.45,
        trend_f_per_hour=0.3,
        reasoning="test",
    )


def test_choose_trade_rejects_low_model_probability():
    engine = _engine()
    signal = _make_signal(
        ticker="T-LOWPROB",
        subtitle="58-59",
        model_prob=0.15,
        market_prob=0.03,
        edge=0.12,
        lower=58,
        upper=59,
    )
    snapshot, selected, _ = engine.choose_trade(
        signals=[signal],
        model_probabilities={"T-LOWPROB": 0.15},
        previous_model_probabilities={},
        previous_yes_ask_by_ticker={},
        observation=None,
        trajectory_assessment=None,
        adjusted_mean_f=57.0,
        adjusted_std_f=2.0,
        now_local=datetime(2026, 3, 24, 14, 0, tzinfo=NY_TZ),
        trades_today=0,
        open_position_count=0,
        max_active_positions=1,
        min_entry_price_cents=8,
        max_entry_price_cents=60,
        max_spread_cents=8,
        min_volume=25,
    )
    assert selected is None
    assert snapshot.status == "SKIP"
    assert "confidence floor" in snapshot.reason


def test_choose_trade_rejects_infeasible_bracket():
    engine = _engine()
    signal = _make_signal(
        ticker="T-INFEASIBLE",
        subtitle="52-53",
        model_prob=0.58,
        market_prob=0.31,
        edge=0.27,
        lower=52,
        upper=53,
    )
    obs = DailyObservation(
        station_id="KNYC",
        date="2026-03-24",
        observed_high_f=45.0,
        possible_actual_high_low=44.7,
        possible_actual_high_high=45.3,
    )
    snapshot, selected, peak = engine.choose_trade(
        signals=[signal],
        model_probabilities={"T-INFEASIBLE": 0.58},
        previous_model_probabilities={"T-INFEASIBLE": 0.54},
        previous_yes_ask_by_ticker={"T-INFEASIBLE": 39},
        observation=obs,
        trajectory_assessment=_trajectory(expected_warm=0.8),
        adjusted_mean_f=46.0,
        adjusted_std_f=1.2,
        now_local=datetime(2026, 3, 24, 17, 0, tzinfo=NY_TZ),
        trades_today=0,
        open_position_count=0,
        max_active_positions=1,
        min_entry_price_cents=8,
        max_entry_price_cents=60,
        max_spread_cents=8,
        min_volume=25,
    )
    assert selected is None
    assert snapshot.status == "SKIP"
    assert "infeasible" in snapshot.reason
    assert peak.max_feasible_temp_f is not None
    assert peak.max_feasible_temp_f < 52.0


def test_choose_trade_allows_when_model_trending_and_market_lagging():
    engine = _engine()
    signal = _make_signal(
        ticker="T-TRADE",
        subtitle="58-59",
        model_prob=0.63,
        market_prob=0.42,
        edge=0.21,
        lower=58,
        upper=59,
        ask=42,
        bid=40,
    )
    snapshot, selected, _ = engine.choose_trade(
        signals=[signal],
        model_probabilities={"T-TRADE": 0.63},
        previous_model_probabilities={"T-TRADE": 0.56},  # +7pp model move
        previous_yes_ask_by_ticker={"T-TRADE": 41},      # +1pp market move
        observation=None,
        trajectory_assessment=None,
        adjusted_mean_f=58.4,
        adjusted_std_f=1.7,
        now_local=datetime(2026, 3, 24, 15, 30, tzinfo=NY_TZ),
        trades_today=1,
        open_position_count=0,
        max_active_positions=2,
        min_entry_price_cents=8,
        max_entry_price_cents=60,
        max_spread_cents=8,
        min_volume=25,
    )
    assert selected is not None
    assert snapshot.status == "TRADE"
    assert snapshot.ticker == "T-TRADE"
    assert snapshot.timing_lag_pp is not None
    assert snapshot.timing_lag_pp > 0


def test_size_contracts_scales_with_edge_and_confidence():
    engine = _engine()
    weak = TradeDecisionSnapshot(
        status="TRADE",
        reason="weak",
        edge=0.06,
        confidence=0.35,
    )
    strong = TradeDecisionSnapshot(
        status="TRADE",
        reason="strong",
        edge=0.20,
        confidence=0.85,
    )
    weak_size = engine.size_contracts(
        snapshot=weak,
        entry_price_cents=40,
        max_affordable_contracts=10,
        min_contracts=1,
    )
    strong_size = engine.size_contracts(
        snapshot=strong,
        entry_price_cents=40,
        max_affordable_contracts=10,
        min_contracts=1,
    )
    assert weak_size >= 1
    assert strong_size > weak_size


def test_dynamic_edge_floor_blocks_marginal_edge_in_high_uncertainty():
    signal = _make_signal(
        ticker="T-DYNEDGE",
        subtitle="55-56",
        model_prob=0.52,
        market_prob=0.46,
        edge=0.06,
        lower=55,
        upper=56,
        ask=46,
        bid=44,
    )
    common_kwargs = dict(
        signals=[signal],
        model_probabilities={"T-DYNEDGE": 0.52},
        previous_model_probabilities={},
        previous_yes_ask_by_ticker={},
        observation=None,
        trajectory_assessment=None,
        adjusted_mean_f=55.8,
        adjusted_std_f=4.0,
        now_local=datetime(2026, 3, 24, 14, 30, tzinfo=NY_TZ),
        trades_today=0,
        open_position_count=0,
        max_active_positions=1,
        min_entry_price_cents=8,
        max_entry_price_cents=60,
        max_spread_cents=8,
        min_volume=25,
    )

    dynamic_engine = _engine(
        dynamic_edge_floor_enabled=True,
        dynamic_edge_std_bump_per_f=0.01,
        dynamic_edge_boundary_bump_max=0.01,
        dynamic_edge_max_extra=0.03,
    )
    dynamic_snapshot, dynamic_selected, _ = dynamic_engine.choose_trade(**common_kwargs)
    assert dynamic_selected is None
    assert dynamic_snapshot.status == "SKIP"
    assert "edge floor" in dynamic_snapshot.reason

    fixed_engine = _engine(dynamic_edge_floor_enabled=False)
    fixed_snapshot, fixed_selected, _ = fixed_engine.choose_trade(**common_kwargs)
    assert fixed_selected is not None
    assert fixed_snapshot.status == "TRADE"
