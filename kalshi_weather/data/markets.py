"""
Kalshi Market Client for Weather Trading Bot.

Fetches and parses Kalshi market data for temperature brackets.
"""

import logging
import os
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Optional, Tuple, Any

import requests

from kalshi_weather.core import MarketBracket, MarketDataSource, BracketType, ContractType
from kalshi_weather.config import (
    CityConfig,
    DEFAULT_CITY,
    KALSHI_API_BASE,
    KALSHI_MARKETS_URL,
    API_TIMEOUT,
)

logger = logging.getLogger(__name__)

def _safe_price_cents(value: Optional[object], default: int) -> int:
    """
    Convert API price values to integer cents.

    Handles both price-in-cents (e.g., 57) and probability-style decimals
    (e.g., 0.57), though Kalshi typically returns cents.
    """
    if value is None:
        return default
    try:
        numeric = float(value)
        if 0.0 <= numeric <= 1.0:
            return int(round(numeric * 100))
        return int(round(numeric))
    except (TypeError, ValueError):
        return default


def _safe_dollar_str_to_cents(value: Optional[object], default: int) -> int:
    """Convert Kalshi fixed-point dollar string values like '0.57' into cents."""
    if value is None:
        return default
    try:
        return int((Decimal(str(value)) * 100).quantize(Decimal("1")))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _extract_probability(value: Optional[object]) -> Optional[float]:
    """
    Parse implied probability from API response.

    Kalshi responses can provide probability either in [0, 1] or [0, 100].
    """
    if value is None:
        return None
    try:
        prob = float(value)
    except (TypeError, ValueError):
        return None

    if prob > 1.0:
        prob /= 100.0
    return max(0.0, min(1.0, prob))


# Regex patterns for parsing bracket subtitles
BETWEEN_PATTERN = re.compile(
    r"(\d+)°?\s*(?:F)?\s*to\s*(\d+)°?\s*(?:F)?",
    re.IGNORECASE
)

GREATER_THAN_PATTERN = re.compile(
    r"(?:(?:above|greater\s*than|>)\s*(\d+)|(\d+)°?\s*(?:F)?\s*or\s*above)°?\s*(?:F)?",
    re.IGNORECASE
)

LESS_THAN_PATTERN = re.compile(
    r"(?:(?:below|less\s*than|<)\s*(\d+)|(\d+)°?\s*(?:F)?\s*or\s*below)°?\s*(?:F)?",
    re.IGNORECASE
)


def parse_bracket_subtitle(subtitle: str) -> Tuple[BracketType, Optional[float], Optional[float]]:
    """Parse a bracket subtitle to extract type and bounds."""
    match = BETWEEN_PATTERN.search(subtitle)
    if match:
        lower = float(match.group(1))
        upper = float(match.group(2))
        return (BracketType.BETWEEN, lower, upper)

    match = GREATER_THAN_PATTERN.search(subtitle)
    if match:
        threshold = float(match.group(1) or match.group(2))
        return (BracketType.GREATER_THAN, threshold, None)

    match = LESS_THAN_PATTERN.search(subtitle)
    if match:
        threshold = float(match.group(1) or match.group(2))
        return (BracketType.LESS_THAN, None, threshold)

    raise ValueError(f"Could not parse bracket subtitle: {subtitle}")


def calculate_implied_probability(yes_bid: int, yes_ask: int) -> float:
    """Calculate implied probability from bid/ask prices."""
    if yes_bid == 0 and yes_ask == 0:
        return 0.0
    if yes_bid >= 100 or yes_ask >= 100:
        return 1.0
    mid = (yes_bid + yes_ask) / 2.0
    return mid / 100.0


def format_date_for_ticker(target_date: str) -> str:
    """Format a date string for matching Kalshi event tickers."""
    dt = datetime.strptime(target_date, "%Y-%m-%d")
    return dt.strftime("%y%b%d").upper()


def parse_market_to_bracket(market: Dict) -> Optional[MarketBracket]:
    """Parse a Kalshi market dict into a MarketBracket."""
    try:
        ticker = market.get("ticker", "")
        event_ticker = market.get("event_ticker", "")
        subtitle = market.get("subtitle", "")

        bracket_type, lower_bound, upper_bound = parse_bracket_subtitle(subtitle)

        yes_bid = _safe_dollar_str_to_cents(market.get("yes_bid_dollars"), -1)
        if yes_bid < 0:
            yes_bid = _safe_price_cents(market.get("yes_bid", market.get("bid")), 0)

        yes_ask = _safe_dollar_str_to_cents(market.get("yes_ask_dollars"), -1)
        if yes_ask < 0:
            yes_ask = _safe_price_cents(market.get("yes_ask", market.get("ask")), 100)

        last_price = _safe_dollar_str_to_cents(market.get("last_price_dollars"), -1)
        if last_price < 0:
            last_price = _safe_price_cents(market.get("last_price"), 0)

        volume_raw = market.get("volume")
        try:
            volume = int(volume_raw) if volume_raw is not None else 0
        except (TypeError, ValueError):
            volume = 0

        implied_prob = _extract_probability(market.get("last_price_dollars"))
        if implied_prob is None:
            implied_prob = _extract_probability(market.get("last_price"))
        if implied_prob is None:
            implied_prob = max(0.0, min(1.0, yes_bid / 100.0))

        return MarketBracket(
            ticker=ticker,
            event_ticker=event_ticker,
            subtitle=subtitle,
            bracket_type=bracket_type,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            last_price=last_price,
            volume=volume,
            implied_prob=implied_prob,
        )
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Failed to parse market {market.get('ticker', 'unknown')}: {e}")
        return None


class KalshiMarketClient(MarketDataSource):
    """Fetches and parses Kalshi temperature market data."""

    def __init__(
        self,
        city: CityConfig = None,
        contract_type: ContractType = ContractType.HIGH_TEMP,
    ):
        """
        Initialize the Kalshi market client.

        Args:
            city: CityConfig object (default: NYC)
            contract_type: Type of contract (default: HIGH_TEMP)
        """
        self.city = city or DEFAULT_CITY
        self.contract_type = contract_type
        self.series_ticker = self._get_series_ticker()
        self._last_status: Optional[Dict] = None
        self.api_key = os.getenv("KALSHI_API_KEY", "") or os.getenv("KALSHI_API_KEY_ID", "")
        self.api_secret = os.getenv("KALSHI_API_SECRET", "")
        self._auth_warning_logged = False
        self._debug_logged_tickers: set[str] = set()

    def _get_series_ticker(self) -> str:
        """Get the series ticker based on city and contract type."""
        if self.contract_type == ContractType.HIGH_TEMP:
            return self.city.high_temp_ticker
        elif self.contract_type == ContractType.LOW_TEMP:
            return self.city.low_temp_ticker
        else:
            raise ValueError(f"Unsupported contract type: {self.contract_type}")

    def _get_headers(self) -> Dict:
        """Return headers for Kalshi API requests."""
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }


    def _get_signed_headers(self, method: str, request_path: str) -> Dict:
        headers = self._get_headers().copy()

        if not self.api_key or not self.api_secret:
            return headers

        logger.warning(
            "Authenticated Kalshi requests are currently disabled in this client; using public market-data endpoints."
        )
        return headers

    def _fetch_markets(self, event_ticker: str = None) -> List[Dict]:
        """Fetch markets from Kalshi API."""
        try:
            params = {
                "limit": 100,
                "status": "open",
            }

            if event_ticker:
                params["event_ticker"] = event_ticker
            else:
                params["series_ticker"] = self.series_ticker

            response = requests.get(
                KALSHI_MARKETS_URL,
                params=params,
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            return data.get("markets", [])
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch Kalshi markets: {e}")
            return []
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse Kalshi markets response: {e}")
            return []

    def _fetch_market_detail(self, market_ticker: str) -> Optional[Dict]:
        """
        Fetch live quote for a single market.

        Uses Kalshi endpoint:
        /trade-api/v2/markets/{market_ticker}
        """
        try:
            request_path = f"/trade-api/v2/markets/{market_ticker}"
            response = requests.get(
                f"{KALSHI_API_BASE}/markets/{market_ticker}",
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            if market_ticker not in self._debug_logged_tickers:
                logger.warning(
                    "Kalshi detail response %s: status=%s body=%s",
                    market_ticker,
                    response.status_code,
                    response.text[:500],
                )
                self._debug_logged_tickers.add(market_ticker)
            data = response.json()

            if isinstance(data, dict):
                if "market" in data and isinstance(data["market"], dict):
                    return data["market"]
                return data
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch market detail {market_ticker}: {e}")
            return None
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse market detail {market_ticker}: {e}")
            return None

    def fetch_brackets(self, target_date: str) -> List[MarketBracket]:
        """Fetch all brackets for a target date's temperature market."""
        date_str = format_date_for_ticker(target_date)
        expected_event_ticker = f"{self.series_ticker}-{date_str}"

        markets = self._fetch_markets()

        if not markets:
            markets = self._fetch_markets(event_ticker=expected_event_ticker)

        brackets = []
        for market in markets:
            event_ticker = market.get("event_ticker", "")

            if date_str not in event_ticker:
                continue

            ticker = market.get("ticker")
            live_market = self._fetch_market_detail(ticker) if ticker else None

            # Keep structural fields from list response, and overwrite quote fields
            # with live detail fields when available.
            merged_market = market.copy()
            if live_market:
                merged_market.update(live_market)

            bracket = parse_market_to_bracket(merged_market)
            if bracket:
                brackets.append(bracket)

        brackets.sort(key=lambda b: b.lower_bound if b.lower_bound is not None else (b.upper_bound or 0))

        return brackets

    def fetch_all_open_markets(self) -> List[MarketBracket]:
        """Fetch all open markets for the series (all dates)."""
        markets = self._fetch_markets()

        brackets = []
        for market in markets:
            bracket = parse_market_to_bracket(market)
            if bracket:
                brackets.append(bracket)

        return brackets

    def get_market_status(self) -> Dict:
        """Get current market status."""
        try:
            response = requests.get(
                KALSHI_MARKETS_URL,
                params={"series_ticker": self.series_ticker, "limit": 1},
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            markets = data.get("markets", [])
            self._last_status = {
                "api_available": True,
                "markets_found": len(markets) > 0,
                "series_ticker": self.series_ticker,
                "city": self.city.code,
                "contract_type": self.contract_type.value,
                "timestamp": datetime.now().isoformat(),
            }
            return self._last_status
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to get market status: {e}")
            self._last_status = {
                "api_available": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
            return self._last_status

    def get_available_dates(self) -> List[str]:
        """Get list of dates with open markets."""
        markets = self._fetch_markets()

        dates = set()
        for market in markets:
            event_ticker = market.get("event_ticker", "")
            if "-" in event_ticker:
                date_part = event_ticker.split("-")[-1]
                try:
                    dt = datetime.strptime(date_part, "%y%b%d")
                    dates.add(dt.strftime("%Y-%m-%d"))
                except ValueError:
                    continue

        return sorted(dates)


def fetch_brackets_for_date(
    target_date: str,
    city: CityConfig = None,
    contract_type: ContractType = ContractType.HIGH_TEMP,
) -> List[MarketBracket]:
    """Convenience function to fetch brackets for a specific date."""
    client = KalshiMarketClient(city, contract_type)
    return client.fetch_brackets(target_date)


def get_market_summary(
    target_date: str,
    city: CityConfig = None,
    contract_type: ContractType = ContractType.HIGH_TEMP,
) -> Dict:
    """Get a summary of market data for a target date."""
    brackets = fetch_brackets_for_date(target_date, city, contract_type)

    if not brackets:
        return {
            "target_date": target_date,
            "bracket_count": 0,
            "total_volume": 0,
            "brackets": [],
        }

    total_volume = sum(b.volume for b in brackets)
    avg_spread = sum(b.yes_ask - b.yes_bid for b in brackets) / len(brackets)

    return {
        "target_date": target_date,
        "bracket_count": len(brackets),
        "total_volume": total_volume,
        "avg_spread_cents": round(avg_spread, 1),
        "brackets": [
            {
                "subtitle": b.subtitle,
                "implied_prob": round(b.implied_prob, 3),
                "bid": b.yes_bid,
                "ask": b.yes_ask,
                "volume": b.volume,
            }
            for b in brackets
        ],
    }



def get_kalshi_auth_debug_info() -> Dict[str, Any]:
    """Return safe debug info for troubleshooting local Kalshi setup."""
    api_secret = os.getenv("KALSHI_API_SECRET", "")
    api_key = os.getenv("KALSHI_API_KEY", "") or os.getenv("KALSHI_API_KEY_ID", "")

    return {
        "has_api_key": bool(api_key),
        "api_key_prefix": api_key[:8] if api_key else "",
        "has_api_secret": bool(api_secret),
        "using_public_market_data": True,
        "api_base": KALSHI_API_BASE,
        "markets_url": KALSHI_MARKETS_URL,
    }
