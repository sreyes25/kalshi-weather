from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock, patch

from kalshi_weather.cli.bot import WeatherBot
from kalshi_weather.core.models import (
    BracketType,
    MarketAnalysis,
    MarketBracket,
    OpenPosition,
    PositionRecommendation,
    TemperatureForecast,
)


def _make_recommendation(*, mark_cents: float, ticker: str = "KXTEST-YES") -> PositionRecommendation:
    position = OpenPosition(
        ticker=ticker,
        side="YES",
        contracts=10,
        average_entry_price_cents=50,
        yes_bid=int(mark_cents),
        yes_ask=min(99, int(mark_cents) + 2),
    )
    return PositionRecommendation(
        position=position,
        model_yes_probability=0.55,
        side_probability=0.55,
        side_probability_change_pp=0.0,
        is_primary_outcome_position=True,
        top_model_probability=0.55,
        primary_gap_pp=0.0,
        fair_value_cents=55.0,
        trend_adjusted_fair_value_cents=55.0,
        liquidation_price_cents=int(mark_cents),
        liquidation_net_cents=float(mark_cents),
        edge_vs_liquidation_cents=2.0,
        action="HOLD",
        target_exit_price_cents=56,
        rationale="test",
    )


def _make_analysis(
    *,
    target_date: str,
    model_probabilities: dict[str, float],
    open_positions: list[PositionRecommendation] | None = None,
) -> MarketAnalysis:
    now = datetime(2026, 3, 26, 15, 0, 0)
    brackets = [
        MarketBracket(
            ticker="KX-A",
            event_ticker="KX-EVENT",
            subtitle="61° to 63°",
            bracket_type=BracketType.BETWEEN,
            lower_bound=61.0,
            upper_bound=63.0,
            yes_bid=40,
            yes_ask=42,
            last_price=41,
            volume=100,
            implied_prob=0.41,
        ),
        MarketBracket(
            ticker="KX-B",
            event_ticker="KX-EVENT",
            subtitle="64° to 66°",
            bracket_type=BracketType.BETWEEN,
            lower_bound=64.0,
            upper_bound=66.0,
            yes_bid=35,
            yes_ask=37,
            last_price=36,
            volume=100,
            implied_prob=0.36,
        ),
    ]
    return MarketAnalysis(
        city="New York City",
        target_date=target_date,
        forecasts=[
            TemperatureForecast(
                source="NWS",
                target_date=target_date,
                forecast_temp_f=64.0,
                low_f=62.0,
                high_f=66.0,
                std_dev=2.0,
                model_run_time=now,
                fetched_at=now,
            )
        ],
        observation=None,
        brackets=brackets,
        signals=[],
        forecast_mean=64.0,
        forecast_std=2.0,
        analyzed_at=now,
        tomorrow_date="2026-03-27",
        tomorrow_forecast_mean=61.5,
        model_probabilities=model_probabilities,
        open_positions=open_positions or [],
    )


def test_drawdown_alert_triggers_only_on_crossing(tmp_path):
    state_path = tmp_path / "source_change_state_fixture.json"
    events_path = tmp_path / "source_change_events_fixture.jsonl"
    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_path)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_path)):
        bot = WeatherBot(city_code="NYC")

    ok_response = Mock()
    ok_response.raise_for_status.return_value = None

    with patch("kalshi_weather.cli.bot.ALERTS_ENABLED", True), \
         patch("kalshi_weather.cli.bot.ALERTS_PROVIDER", "twilio"), \
         patch("kalshi_weather.cli.bot.ALERT_POSITION_LOSS_FRACTION", 0.35), \
         patch("kalshi_weather.cli.bot.TWILIO_ACCOUNT_SID", "AC123"), \
         patch("kalshi_weather.cli.bot.TWILIO_AUTH_TOKEN", "auth"), \
         patch("kalshi_weather.cli.bot.TWILIO_TO_NUMBER", "+15550001111"), \
         patch("kalshi_weather.cli.bot.REMOTE_KILL_ALLOWED_FROM", "+15550002222"), \
         patch("kalshi_weather.cli.bot.requests.post", return_value=ok_response) as mock_post:
        safe = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.55, "KX-B": 0.45},
            open_positions=[_make_recommendation(mark_cents=42.0)],
        )
        bot._maybe_alert_on_drawdown(safe)
        assert mock_post.call_count == 0

        breached = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.55, "KX-B": 0.45},
            open_positions=[_make_recommendation(mark_cents=31.0)],
        )
        bot._maybe_alert_on_drawdown(breached)
        assert mock_post.call_count == 1

        still_breached = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.55, "KX-B": 0.45},
            open_positions=[_make_recommendation(mark_cents=30.0)],
        )
        bot._maybe_alert_on_drawdown(still_breached)
        assert mock_post.call_count == 1


def test_bracket_shift_sends_summary_and_llm_prompt(tmp_path):
    state_path = tmp_path / "source_change_state_fixture.json"
    events_path = tmp_path / "source_change_events_fixture.jsonl"
    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_path)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_path)):
        bot = WeatherBot(city_code="NYC")

    ok_response = Mock()
    ok_response.raise_for_status.return_value = None

    with patch("kalshi_weather.cli.bot.ALERTS_ENABLED", True), \
         patch("kalshi_weather.cli.bot.ALERTS_PROVIDER", "twilio"), \
         patch("kalshi_weather.cli.bot.ALERT_BRACKET_CHANGE_ENABLED", True), \
         patch("kalshi_weather.cli.bot.ALERT_LLM_PROMPT_ENABLED", True), \
         patch("kalshi_weather.cli.bot.TWILIO_ACCOUNT_SID", "AC123"), \
         patch("kalshi_weather.cli.bot.TWILIO_AUTH_TOKEN", "auth"), \
         patch("kalshi_weather.cli.bot.TWILIO_TO_NUMBER", "+15550001111"), \
         patch("kalshi_weather.cli.bot.REMOTE_KILL_ALLOWED_FROM", "+15550002222"), \
         patch("kalshi_weather.cli.bot.requests.post", return_value=ok_response) as mock_post:
        initial = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.60, "KX-B": 0.40},
        )
        bot._maybe_alert_on_bracket_shift(initial)
        assert mock_post.call_count == 0

        shifted = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.45, "KX-B": 0.55},
        )
        bot._maybe_alert_on_bracket_shift(shifted)
        assert mock_post.call_count == 2

        first_body = mock_post.call_args_list[0].kwargs["data"]["Body"]
        second_body = mock_post.call_args_list[1].kwargs["data"]["Body"]
        assert "KWBOT BRACKET SHIFT" in first_body
        assert "LLM PROMPT INPUT" in second_body


def test_whatsapp_cloud_alert_payload(tmp_path):
    state_path = tmp_path / "source_change_state_fixture.json"
    events_path = tmp_path / "source_change_events_fixture.jsonl"
    with patch("kalshi_weather.cli.bot.CombinedWeatherSource"), \
         patch("kalshi_weather.cli.bot.NWSStationParser"), \
         patch("kalshi_weather.cli.bot.HighTempContract"), \
         patch("kalshi_weather.cli.bot.Dashboard"), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_STATE_PATH", str(state_path)), \
         patch("kalshi_weather.cli.bot.SOURCE_CHANGE_EVENTS_PATH", str(events_path)):
        bot = WeatherBot(city_code="NYC")

    ok_response = Mock()
    ok_response.raise_for_status.return_value = None

    with patch("kalshi_weather.cli.bot.ALERTS_ENABLED", True), \
         patch("kalshi_weather.cli.bot.ALERTS_PROVIDER", "whatsapp_cloud"), \
         patch("kalshi_weather.cli.bot.ALERT_POSITION_LOSS_FRACTION", 0.35), \
         patch("kalshi_weather.cli.bot.WHATSAPP_PHONE_NUMBER_ID", "1234567890"), \
         patch("kalshi_weather.cli.bot.WHATSAPP_ACCESS_TOKEN", "token-abc"), \
         patch("kalshi_weather.cli.bot.WHATSAPP_CLOUD_API_VERSION", "v21.0"), \
         patch("kalshi_weather.cli.bot.ALERT_TO_NUMBER", "+1 (555) 123-4567"), \
         patch("kalshi_weather.cli.bot.requests.post", return_value=ok_response) as mock_post:
        breached = _make_analysis(
            target_date="2026-03-26",
            model_probabilities={"KX-A": 0.55, "KX-B": 0.45},
            open_positions=[_make_recommendation(mark_cents=31.0)],
        )
        bot._maybe_alert_on_drawdown(breached)
        assert mock_post.call_count == 1

        call = mock_post.call_args
        assert "graph.facebook.com/v21.0/1234567890/messages" in call.args[0]
        assert call.kwargs["headers"]["Authorization"] == "Bearer token-abc"
        assert call.kwargs["json"]["messaging_product"] == "whatsapp"
        assert call.kwargs["json"]["to"] == "15551234567"
