import pytest
from datetime import datetime
from kalshi_weather.core.models import (
    TemperatureForecast,
    MarketBracket,
    BracketType,
    TradingSignal
)
from kalshi_weather.engine.edge_detector import EdgeDetector

@pytest.fixture
def mock_forecasts():
    return [
        TemperatureForecast(
            source="TestModel",
            target_date="2024-01-01",
            forecast_temp_f=55.0,
            low_f=52.0,
            high_f=58.0,
            std_dev=2.0,
            model_run_time=datetime.now(),
            fetched_at=datetime.now()
        )
    ]

@pytest.fixture
def mock_brackets():
    return [
        # Bracket covering 54-56 (Mean 55 should have high prob here)
        MarketBracket(
            ticker="TEST-B54",
            event_ticker="TEST",
            subtitle="54 to 56",
            bracket_type=BracketType.BETWEEN,
            lower_bound=54.0,
            upper_bound=56.0,
            yes_bid=10,
            yes_ask=20,  # Price 20 cents
            last_price=15,
            volume=100,
            implied_prob=0.15
        ),
        # Bracket covering 90-92 (Mean 55 should have ~0 prob here)
        MarketBracket(
            ticker="TEST-B90",
            event_ticker="TEST",
            subtitle="90 to 92",
            bracket_type=BracketType.BETWEEN,
            lower_bound=90.0,
            upper_bound=92.0,
            yes_bid=1,
            yes_ask=2,
            last_price=2,
            volume=10,
            implied_prob=0.015
        )
    ]

def test_edge_detection_high_edge(mock_forecasts, mock_brackets):
    """Test that meaningful edge generates a signal."""
    detector = EdgeDetector(fee_rate=0.0)
    
    # Model 55, Bracket 54-56.
    # Prob should be high. ~38% for range +/- 0.5 sigma? 
    # Normal CDF(56.5, 55, 2) - CDF(53.5, 55, 2)
    # 56.5 is +0.75 sigma. 53.5 is -0.75 sigma.
    # Area roughly 54%.
    # Market Ask is 20 cents (0.20).
    # Edge ~ 0.54 - 0.20 = 0.34.
    
    signals = detector.analyze(mock_forecasts, None, mock_brackets, min_edge=0.05)
    
    assert len(signals) >= 1
    signal = signals[0]
    assert signal.bracket.ticker == "TEST-B54"
    assert signal.direction == "BUY"
    assert signal.model_prob > 0.4
    assert signal.edge > 0.2

def test_edge_detection_sell_signal_for_negative_ev(mock_forecasts, mock_brackets):
    """Test that negative EV generates a SELL signal."""
    detector = EdgeDetector()

    # Force clear overpricing in one bracket.
    mock_brackets[0].implied_prob = 0.95
    signals = detector.analyze(mock_forecasts, None, mock_brackets, min_edge=0.05)
    sell_signals = [s for s in signals if s.bracket.ticker == "TEST-B54"]
    assert len(sell_signals) == 1
    assert sell_signals[0].direction == "SELL"
    assert sell_signals[0].edge < -0.05

def test_no_forecasts_returns_empty():
    detector = EdgeDetector()
    assert detector.analyze([], None, []) == []

def test_edge_detection_short_signal(mock_forecasts):
    """Test generating a NO signal."""
    detector = EdgeDetector(fee_rate=0.0)
    
    # Forecast Mean 55.
    # Bracket "Greater than 80". Probability ~ 0.
    # Market thinks probability is high (mispriced).
    bracket = MarketBracket(
        ticker="TEST-GT80",
        event_ticker="TEST",
        subtitle="Above 80",
        bracket_type=BracketType.GREATER_THAN,
        lower_bound=80.0,
        upper_bound=None,
        yes_bid=80,  # We can sell at 80 cents!
        yes_ask=90,
        last_price=85,
        volume=100,
        implied_prob=0.85
    )
    
    # We sell at 80. Implied NO price = 20c.
    # Model NO prob = 1.0 (since model prob YES is 0).
    # Edge = 1.0 - 0.20 = 0.80.
    
    signals = detector.analyze(mock_forecasts, None, [bracket], min_edge=0.1)
    
    assert len(signals) == 1
    assert signals[0].direction == "SELL"
    assert signals[0].bracket.ticker == "TEST-GT80"
    assert signals[0].edge < -0.5
