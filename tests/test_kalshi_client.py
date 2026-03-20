"""
Tests for Kalshi market client module.

Uses the `responses` library to mock HTTP requests.
"""

import pytest
import responses
from datetime import datetime

from kalshi_weather.data.markets import (
    KalshiMarketClient,
    fetch_brackets_for_date,
    get_market_summary,
    parse_bracket_subtitle,
    calculate_implied_probability,
    format_date_for_ticker,
    parse_market_to_bracket,
)
from kalshi_weather.core import BracketType, ContractType
from kalshi_weather.config import KALSHI_MARKETS_URL, NYC


# =============================================================================
# TEST DATA
# =============================================================================

TARGET_DATE = "2026-01-20"
SERIES_TICKER = "KXHIGHNY"
EVENT_TICKER = "KXHIGHNY-26JAN20"


def make_market(ticker: str, event_ticker: str, subtitle: str, yes_bid: int = 25, yes_ask: int = 27, last_price: int = 26, volume: int = 1000) -> dict:
    return {"ticker": ticker, "event_ticker": event_ticker, "subtitle": subtitle, "yes_bid": yes_bid, "yes_ask": yes_ask, "last_price": last_price, "volume": volume, "status": "open"}


def make_api_response(markets: list) -> dict:
    return {"markets": markets}


SAMPLE_MARKETS = [
    make_market(f"{EVENT_TICKER}-B48", EVENT_TICKER, "Below 50°", yes_bid=5, yes_ask=7, volume=234),
    make_market(f"{EVENT_TICKER}-B50", EVENT_TICKER, "50° to 52°", yes_bid=13, yes_ask=15, volume=567),
    make_market(f"{EVENT_TICKER}-B52", EVENT_TICKER, "52° to 54°", yes_bid=25, yes_ask=27, volume=1234),
    make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°", yes_bid=23, yes_ask=25, volume=1890),
    make_market(f"{EVENT_TICKER}-B56", EVENT_TICKER, "56° to 58°", yes_bid=17, yes_ask=19, volume=678),
    make_market(f"{EVENT_TICKER}-B58", EVENT_TICKER, "Above 58°", yes_bid=11, yes_ask=13, volume=345),
]


# =============================================================================
# BRACKET SUBTITLE PARSING TESTS
# =============================================================================


class TestParseBracketSubtitle:
    def test_parse_between_with_degree_symbol(self):
        bracket_type, lower, upper = parse_bracket_subtitle("54° to 56°")
        assert bracket_type == BracketType.BETWEEN
        assert lower == 54.0
        assert upper == 56.0

    def test_parse_between_with_fahrenheit(self):
        bracket_type, lower, upper = parse_bracket_subtitle("54°F to 56°F")
        assert bracket_type == BracketType.BETWEEN
        assert lower == 54.0
        assert upper == 56.0

    def test_parse_between_simple(self):
        bracket_type, lower, upper = parse_bracket_subtitle("54 to 56")
        assert bracket_type == BracketType.BETWEEN
        assert lower == 54.0
        assert upper == 56.0

    def test_parse_greater_than_above(self):
        bracket_type, lower, upper = parse_bracket_subtitle("Above 58°")
        assert bracket_type == BracketType.GREATER_THAN
        assert lower == 58.0
        assert upper is None

    def test_parse_greater_than_text(self):
        bracket_type, lower, upper = parse_bracket_subtitle("Greater than 58")
        assert bracket_type == BracketType.GREATER_THAN
        assert lower == 58.0
        assert upper is None

    def test_parse_greater_than_symbol(self):
        bracket_type, lower, upper = parse_bracket_subtitle("> 58°F")
        assert bracket_type == BracketType.GREATER_THAN
        assert lower == 58.0
        assert upper is None

    def test_parse_greater_than_or_above(self):
        bracket_type, lower, upper = parse_bracket_subtitle("24° or above")
        assert bracket_type == BracketType.GREATER_THAN
        assert lower == 24.0
        assert upper is None

    def test_parse_less_than_below(self):
        bracket_type, lower, upper = parse_bracket_subtitle("Below 50°")
        assert bracket_type == BracketType.LESS_THAN
        assert lower is None
        assert upper == 50.0

    def test_parse_less_than_text(self):
        bracket_type, lower, upper = parse_bracket_subtitle("Less than 50")
        assert bracket_type == BracketType.LESS_THAN
        assert lower is None
        assert upper == 50.0

    def test_parse_less_than_symbol(self):
        bracket_type, lower, upper = parse_bracket_subtitle("< 50°F")
        assert bracket_type == BracketType.LESS_THAN
        assert lower is None
        assert upper == 50.0

    def test_parse_less_than_or_below(self):
        bracket_type, lower, upper = parse_bracket_subtitle("15° or below")
        assert bracket_type == BracketType.LESS_THAN
        assert lower is None
        assert upper == 15.0

    def test_parse_invalid_subtitle_raises(self):
        with pytest.raises(ValueError):
            parse_bracket_subtitle("Invalid bracket text")

    def test_parse_empty_subtitle_raises(self):
        with pytest.raises(ValueError):
            parse_bracket_subtitle("")


# =============================================================================
# IMPLIED PROBABILITY TESTS
# =============================================================================


class TestCalculateImpliedProbability:
    def test_mid_market_calculation(self):
        assert abs(calculate_implied_probability(25, 27) - 0.26) < 0.001

    def test_wide_spread(self):
        assert abs(calculate_implied_probability(10, 30) - 0.20) < 0.001

    def test_zero_bid(self):
        assert abs(calculate_implied_probability(0, 5) - 0.025) < 0.001

    def test_both_zero(self):
        assert calculate_implied_probability(0, 0) == 0.0

    def test_high_probability(self):
        assert abs(calculate_implied_probability(90, 95) - 0.925) < 0.001

    def test_at_100(self):
        assert calculate_implied_probability(99, 100) == 1.0


# =============================================================================
# DATE FORMATTING TESTS
# =============================================================================


class TestFormatDateForTicker:
    def test_format_january(self):
        assert format_date_for_ticker("2026-01-20") == "26JAN20"

    def test_format_december(self):
        assert format_date_for_ticker("2026-12-25") == "26DEC25"

    def test_format_single_digit_day(self):
        assert format_date_for_ticker("2026-01-05") == "26JAN05"

    def test_format_different_year(self):
        assert format_date_for_ticker("2025-06-15") == "25JUN15"


# =============================================================================
# MARKET PARSING TESTS
# =============================================================================


class TestParseMarketToBracket:
    def test_parse_between_market(self):
        market = make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°", yes_bid=25, yes_ask=27, volume=1000)
        bracket = parse_market_to_bracket(market)
        assert bracket is not None
        assert bracket.ticker == f"{EVENT_TICKER}-B54"
        assert bracket.bracket_type == BracketType.BETWEEN
        assert bracket.lower_bound == 54.0
        assert bracket.upper_bound == 56.0

    def test_parse_greater_than_market(self):
        market = make_market(f"{EVENT_TICKER}-B58", EVENT_TICKER, "Above 58°", yes_bid=11, yes_ask=13)
        bracket = parse_market_to_bracket(market)
        assert bracket is not None
        assert bracket.bracket_type == BracketType.GREATER_THAN
        assert bracket.lower_bound == 58.0
        assert bracket.upper_bound is None

    def test_parse_less_than_market(self):
        market = make_market(f"{EVENT_TICKER}-B48", EVENT_TICKER, "Below 50°", yes_bid=5, yes_ask=7)
        bracket = parse_market_to_bracket(market)
        assert bracket is not None
        assert bracket.bracket_type == BracketType.LESS_THAN
        assert bracket.lower_bound is None
        assert bracket.upper_bound == 50.0

    def test_parse_invalid_market_returns_none(self):
        market = {"ticker": "TEST", "subtitle": "Invalid text"}
        assert parse_market_to_bracket(market) is None

    def test_parse_missing_fields_uses_defaults(self):
        market = {"ticker": f"{EVENT_TICKER}-B54", "event_ticker": EVENT_TICKER, "subtitle": "54° to 56°"}
        bracket = parse_market_to_bracket(market)
        assert bracket is not None
        assert bracket.yes_bid == 0
        assert bracket.yes_ask == 100


# =============================================================================
# KALSHI MARKET CLIENT TESTS
# =============================================================================


class TestKalshiMarketClient:
    @responses.activate
    def test_fetch_brackets_success(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(SAMPLE_MARKETS), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        brackets = client.fetch_brackets(TARGET_DATE)
        assert len(brackets) == 6

    @responses.activate
    def test_fetch_brackets_filters_by_date(self):
        other_date_market = make_market("KXHIGHNY-26JAN21-B54", "KXHIGHNY-26JAN21", "54° to 56°")
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(SAMPLE_MARKETS + [other_date_market]), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        brackets = client.fetch_brackets(TARGET_DATE)
        assert len(brackets) == 6

    @responses.activate
    def test_fetch_brackets_api_error(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, status=500)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        assert client.fetch_brackets(TARGET_DATE) == []

    @responses.activate
    def test_fetch_brackets_timeout(self):
        from requests.exceptions import Timeout
        responses.add(responses.GET, KALSHI_MARKETS_URL, body=Timeout("Connection timed out"))
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        assert client.fetch_brackets(TARGET_DATE) == []

    @responses.activate
    def test_fetch_brackets_empty_response(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response([]), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        assert client.fetch_brackets(TARGET_DATE) == []

    @responses.activate
    def test_get_market_status_success(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(SAMPLE_MARKETS[:1]), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        status = client.get_market_status()
        assert status["api_available"] is True
        assert status["markets_found"] is True

    @responses.activate
    def test_get_market_status_api_error(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, status=500)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        status = client.get_market_status()
        assert status["api_available"] is False

    @responses.activate
    def test_get_available_dates(self):
        multi_date_markets = SAMPLE_MARKETS + [
            make_market("KXHIGHNY-26JAN21-B54", "KXHIGHNY-26JAN21", "54° to 56°"),
            make_market("KXHIGHNY-26JAN22-B54", "KXHIGHNY-26JAN22", "54° to 56°"),
        ]
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(multi_date_markets), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        dates = client.get_available_dates()
        assert len(dates) == 3
        assert dates == sorted(dates)

    @responses.activate
    def test_default_series_ticker(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response([]), status=200)
        client = KalshiMarketClient(NYC)
        assert client.series_ticker == NYC.high_temp_ticker


# =============================================================================
# CONVENIENCE FUNCTION TESTS
# =============================================================================


class TestConvenienceFunctions:
    @responses.activate
    def test_fetch_brackets_for_date(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(SAMPLE_MARKETS), status=200)
        brackets = fetch_brackets_for_date(TARGET_DATE, NYC)
        assert len(brackets) == 6

    @responses.activate
    def test_get_market_summary(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(SAMPLE_MARKETS), status=200)
        summary = get_market_summary(TARGET_DATE, NYC)
        assert summary["target_date"] == TARGET_DATE
        assert summary["bracket_count"] == 6
        assert summary["total_volume"] > 0

    @responses.activate
    def test_get_market_summary_no_data(self):
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response([]), status=200)
        summary = get_market_summary(TARGET_DATE, NYC)
        assert summary["bracket_count"] == 0


# =============================================================================
# BRACKET BOUNDARY LOGIC TESTS
# =============================================================================


class TestBracketBoundaryLogic:
    def test_between_contains_lower_bound(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°"))
        assert bracket.contains_temp(54.0) is True

    def test_between_contains_upper_bound(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°"))
        assert bracket.contains_temp(56.0) is True

    def test_between_contains_middle(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°"))
        assert bracket.contains_temp(55.0) is True

    def test_between_excludes_outside(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B54", EVENT_TICKER, "54° to 56°"))
        assert bracket.contains_temp(53.0) is False
        assert bracket.contains_temp(57.0) is False

    def test_greater_than_excludes_threshold(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B58", EVENT_TICKER, "Above 58°"))
        assert bracket.contains_temp(58.0) is False

    def test_greater_than_includes_above(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B58", EVENT_TICKER, "Above 58°"))
        assert bracket.contains_temp(59.0) is True

    def test_less_than_excludes_threshold(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B48", EVENT_TICKER, "Below 50°"))
        assert bracket.contains_temp(50.0) is False

    def test_less_than_includes_below(self):
        bracket = parse_market_to_bracket(make_market(f"{EVENT_TICKER}-B48", EVENT_TICKER, "Below 50°"))
        assert bracket.contains_temp(49.0) is True


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


class TestEdgeCases:
    @responses.activate
    def test_malformed_market_skipped(self):
        markets = [SAMPLE_MARKETS[0], {"ticker": "BAD", "subtitle": "Not a valid bracket"}, SAMPLE_MARKETS[1]]
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response(markets), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        brackets = client.fetch_brackets(TARGET_DATE)
        assert len(brackets) == 2

    @responses.activate
    def test_null_values_handled(self):
        market_with_nulls = {"ticker": f"{EVENT_TICKER}-B54", "event_ticker": EVENT_TICKER, "subtitle": "54° to 56°", "yes_bid": None, "yes_ask": None, "last_price": None, "volume": None}
        responses.add(responses.GET, KALSHI_MARKETS_URL, json=make_api_response([market_with_nulls]), status=200)
        client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
        brackets = client.fetch_brackets(TARGET_DATE)
        assert len(brackets) == 1
        assert brackets[0].yes_bid == 0

    def test_invalid_json_response(self):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, KALSHI_MARKETS_URL, body="not valid json", status=200)
            client = KalshiMarketClient(NYC, ContractType.HIGH_TEMP)
            assert client.fetch_brackets(TARGET_DATE) == []

    def test_parse_subtitle_with_extra_whitespace(self):
        bracket_type, lower, upper = parse_bracket_subtitle("  54°   to   56°  ")
        assert bracket_type == BracketType.BETWEEN
        assert lower == 54.0

    def test_parse_subtitle_case_insensitive(self):
        bracket_type1, _, _ = parse_bracket_subtitle("ABOVE 58°")
        bracket_type2, _, _ = parse_bracket_subtitle("above 58°")
        assert bracket_type1 == BracketType.GREATER_THAN
        assert bracket_type2 == BracketType.GREATER_THAN
