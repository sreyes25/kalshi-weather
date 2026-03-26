"""
Kalshi Market Client for Weather Trading Bot.

Fetches and parses Kalshi market data for temperature brackets.
"""

import logging
import os
import re
import time
import base64
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

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:  # pragma: no cover - optional dependency fallback
    hashes = None
    serialization = None
    padding = None

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


def _fixed_point_to_float(value: Any) -> Optional[float]:
    """
    Best-effort parse for Kalshi *_fp fields.

    Kalshi fixed-point fields are commonly scaled by 1e4.
    """
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if abs(numeric) >= 1_000:
        return numeric / 10_000.0
    return numeric


def _to_float_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        if text.startswith("$"):
            text = text[1:]
        text = text.replace(",", "")
        if text.endswith("%"):
            text = text[:-1]
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_transient_http_status(status_code: int) -> bool:
    return status_code in {429, 500, 502, 503, 504}


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

    s = subtitle.strip().lower()
    match = GREATER_THAN_PATTERN.search(subtitle)
    if match:
        threshold = float(match.group(1) or match.group(2))
        # Internal GREATER_THAN logic is strict (> threshold).
        # Market copy "X or above" is inclusive (>= X), so convert to > (X-1).
        if "or above" in s:
            return (BracketType.GREATER_THAN, threshold - 1.0, None)
        return (BracketType.GREATER_THAN, threshold, None)

    match = LESS_THAN_PATTERN.search(subtitle)
    if match:
        threshold = float(match.group(1) or match.group(2))
        # Internal LESS_THAN logic is strict (< threshold).
        # Market copy "X or below" is inclusive (<= X), so convert to < (X+1).
        if "or below" in s:
            return (BracketType.LESS_THAN, None, threshold + 1.0)
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


def _derive_market_prob(
    *,
    yes_bid: int,
    yes_ask: int,
    last_price: int,
) -> float:
    """
    Prefer live bid/ask midpoint for market probability.

    Last trade can be stale during rapid repricing; midpoint is more robust for
    real-time edge comparison. Falls back to last_price if quotes are invalid.
    """
    if 0 <= yes_bid <= 99 and 1 <= yes_ask <= 100 and yes_ask >= yes_bid:
        return calculate_implied_probability(yes_bid, yes_ask)
    if 0 <= last_price <= 100:
        return max(0.0, min(1.0, last_price / 100.0))
    return max(0.0, min(1.0, yes_bid / 100.0))


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

        implied_prob = _derive_market_prob(
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            last_price=last_price,
        )

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
        self.private_key = os.getenv("KALSHI_PRIVATE_KEY", "") or self.api_secret
        self._auth_warning_logged = False
        self._debug_logged_tickers: set[str] = set()
        self._signing_key = None

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


    def _load_private_key(self):
        if self._signing_key is not None:
            return self._signing_key

        if not self.private_key or serialization is None:
            return None

        key_text = self.private_key.replace("\\n", "\n").strip()
        try:
            self._signing_key = serialization.load_pem_private_key(
                key_text.encode("utf-8"),
                password=None,
            )
        except Exception as exc:
            logger.warning("Failed to load Kalshi private key: %s", exc)
            self._signing_key = None
        return self._signing_key

    def _get_signed_headers(self, method: str, request_path: str) -> Dict:
        headers = self._get_headers().copy()

        if not self.api_key:
            return headers
        if not request_path.startswith("/"):
            request_path = f"/{request_path}"

        key = self._load_private_key()
        if key is None or hashes is None or padding is None:
            if not self._auth_warning_logged:
                logger.warning(
                    "Kalshi authenticated calls need a valid RSA private key and cryptography package."
                )
                self._auth_warning_logged = True
            return headers

        timestamp_ms = str(int(time.time() * 1000))
        message = f"{timestamp_ms}{method.upper()}{request_path}".encode("utf-8")
        try:
            signature = key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            signature_b64 = base64.b64encode(signature).decode("utf-8")
        except Exception as exc:
            logger.warning("Failed to sign Kalshi request: %s", exc)
            return headers

        headers.update(
            {
                "KALSHI-ACCESS-KEY": self.api_key,
                "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
                "KALSHI-ACCESS-SIGNATURE": signature_b64,
            }
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

    def _extract_open_positions_payload(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, dict):
            payload = data
        for key in ("market_positions", "positions", "open_positions"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []

    def _pick_first(self, data: Dict[str, Any], keys: List[str]) -> Any:
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    def _to_cents(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        parsed = _safe_dollar_str_to_cents(value, -1)
        if parsed >= 0:
            return parsed
        cents = _safe_price_cents(value, -1)
        return cents if cents >= 0 else None

    def _to_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _parse_open_position(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        market_ticker = str(
            self._pick_first(raw, ["ticker", "market_ticker", "instrument_ticker"]) or ""
        ).strip()
        if not market_ticker:
            return None

        side_raw = str(self._pick_first(raw, ["side", "position_side", "direction"]) or "").upper()
        quantity = self._to_int(
            self._pick_first(raw, ["position", "quantity", "contracts", "open_contracts"])
        )
        position_fp = _fixed_point_to_float(raw.get("position_fp"))
        if quantity is None and position_fp is not None:
            quantity = int(round(position_fp))
        yes_quantity = self._to_int(self._pick_first(raw, ["yes_position", "yes_contracts"]))
        no_quantity = self._to_int(self._pick_first(raw, ["no_position", "no_contracts"]))
        quantity = quantity if quantity is not None else 0

        # If side missing, infer from signed quantity convention.
        if side_raw not in {"YES", "NO"} and quantity != 0:
            side_raw = "NO" if quantity < 0 else "YES"
        if side_raw not in {"YES", "NO"}:
            if (yes_quantity or 0) > 0:
                side_raw = "YES"
            elif (no_quantity or 0) > 0:
                side_raw = "NO"

        if side_raw == "YES":
            contracts = (
                abs(quantity)
                if quantity != 0
                else abs(yes_quantity or 0)
            )
        elif side_raw == "NO":
            contracts = (
                abs(quantity)
                if quantity != 0
                else abs(no_quantity or 0)
            )
        else:
            contracts = abs(quantity) if quantity != 0 else max(abs(yes_quantity or 0), abs(no_quantity or 0))
        if contracts == 0:
            return None

        avg_yes = self._to_cents(
            self._pick_first(raw, ["average_yes_price", "avg_yes_price", "yes_average_price"])
        )
        avg_no = self._to_cents(
            self._pick_first(raw, ["average_no_price", "avg_no_price", "no_average_price"])
        )
        average_entry = self._to_cents(
            self._pick_first(
                raw,
                [
                    "average_open_price",
                    "avg_open_price",
                    "average_price",
                    "avg_price",
                    "cost_basis",
                    "entry_price",
                ],
            )
        )
        if average_entry is None:
            if side_raw == "YES" and avg_yes is not None:
                average_entry = avg_yes
            elif side_raw == "NO" and avg_no is not None:
                average_entry = avg_no
        # Do not infer entry from exposure/turnover fields; these can be cumulative
        # and may not represent current average open price after partial exits.

        return {
            "ticker": market_ticker,
            "side": side_raw,
            "contracts": contracts,
            "average_entry_price_cents": average_entry,
            "event_ticker": self._pick_first(raw, ["event_ticker"]),
        }

    def fetch_open_positions(self) -> List[Dict[str, Any]]:
        """
        Fetch account open positions and enrich with live quote fields.

        Returns:
            List of dicts with ticker/side/contracts/average_entry_price_cents and quote metadata.
        """
        if not self.api_key:
            return []
        if self._load_private_key() is None:
            return []

        request_path = "/trade-api/v2/portfolio/positions"
        try:
            response = requests.get(
                f"{KALSHI_API_BASE}/portfolio/positions",
                params={"status": "open", "limit": 200},
                headers=self._get_signed_headers("GET", request_path),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            logger.warning("Failed to fetch open positions: HTTP %s", status_code)
            return []
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to fetch open positions: %s", exc)
            return []
        except (ValueError, TypeError) as exc:
            logger.warning("Failed to parse open positions response: %s", exc)
            return []

        parsed: List[Dict[str, Any]] = []
        for raw_position in self._extract_open_positions_payload(payload):
            if not isinstance(raw_position, dict):
                continue
            parsed_row = self._parse_open_position(raw_position)
            if not parsed_row:
                continue

            detail = self._fetch_market_detail(parsed_row["ticker"]) or {}
            parsed_row["subtitle"] = detail.get("subtitle")
            parsed_row["event_ticker"] = parsed_row.get("event_ticker") or detail.get("event_ticker")
            parsed_row["yes_bid"] = self._to_cents(detail.get("yes_bid_dollars"))
            if parsed_row["yes_bid"] is None:
                parsed_row["yes_bid"] = self._to_cents(detail.get("yes_bid", detail.get("bid")))
            parsed_row["yes_ask"] = self._to_cents(detail.get("yes_ask_dollars"))
            if parsed_row["yes_ask"] is None:
                parsed_row["yes_ask"] = self._to_cents(detail.get("yes_ask", detail.get("ask")))
            parsed_row["last_price"] = self._to_cents(detail.get("last_price_dollars"))
            if parsed_row["last_price"] is None:
                parsed_row["last_price"] = self._to_cents(detail.get("last_price"))
            parsed.append(parsed_row)

        return parsed

    def fetch_portfolio_totals(
        self,
        event_ticker: Optional[str] = None,
    ) -> Dict[str, float]:
        """
        Aggregate realized/fee/traded totals for current portfolio positions.

        If event_ticker is provided, rows are filtered to that event.
        """
        if not self.api_key:
            return {}
        if self._load_private_key() is None:
            return {}

        request_path = "/trade-api/v2/portfolio/positions"
        statuses = ("open", "closed")
        rows: List[Dict[str, Any]] = []
        for status in statuses:
            try:
                response = requests.get(
                    f"{KALSHI_API_BASE}/portfolio/positions",
                    params={"status": status, "limit": 200},
                    headers=self._get_signed_headers("GET", request_path),
                    timeout=API_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
            except requests.exceptions.RequestException:
                continue
            except (ValueError, TypeError):
                continue
            for raw in self._extract_open_positions_payload(payload):
                if isinstance(raw, dict):
                    rows.append(raw)

        realized = 0.0
        fees = 0.0
        traded = 0.0
        exposure = 0.0
        matched_rows = 0
        seen = set()

        for row in rows:
            row_event = str(row.get("event_ticker") or "").strip()
            if event_ticker:
                # Strict event scoping: when a specific event is requested,
                # ignore rows that do not explicitly match it.
                if not row_event or row_event != event_ticker:
                    continue
            ticker = str(self._pick_first(row, ["ticker", "market_ticker", "instrument_ticker"]) or "")
            side = str(self._pick_first(row, ["side", "position_side", "direction"]) or "")
            signature = (
                ticker,
                side,
                str(row.get("position") or row.get("contracts") or row.get("position_fp") or ""),
                f"{_to_float_value(row.get('realized_pnl_dollars'))}",
                f"{_to_float_value(row.get('fees_paid_dollars'))}",
            )
            if signature in seen:
                continue
            seen.add(signature)
            matched_rows += 1
            realized += _to_float_value(row.get("realized_pnl_dollars")) or 0.0
            fees += _to_float_value(row.get("fees_paid_dollars")) or 0.0
            traded += _to_float_value(row.get("total_traded_dollars")) or 0.0
            exposure += _to_float_value(row.get("market_exposure_dollars")) or 0.0

        if matched_rows == 0:
            totals: Dict[str, float] = {}
        else:
            totals = {
            "realized_pnl_dollars": realized,
            "fees_paid_dollars": fees,
            "total_traded_dollars": traded,
            "market_exposure_dollars": exposure,
            "net_realized_after_fees_dollars": realized - fees,
            "markets_count": float(matched_rows),
            }
        totals.update(self.fetch_account_summary())
        return totals

    def fetch_account_summary(self) -> Dict[str, float]:
        """
        Best-effort account-level totals (cash/equity/buying power) from Kalshi.

        Endpoint payload shapes vary by account/permission tier, so we probe a few
        likely routes and normalize whatever fields are present.
        """
        if not self.api_key:
            return {}
        if self._load_private_key() is None:
            return {}

        endpoints = (
            "/portfolio/balance",
            "/portfolio/account",
            "/portfolio/cash",
            "/portfolio",
            "/portfolio/summary",
        )
        for request_path in endpoints:
            try:
                response = requests.get(
                    f"{KALSHI_API_BASE}{request_path}",
                    headers=self._get_signed_headers("GET", f"/trade-api/v2{request_path}"),
                    timeout=API_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
            except requests.exceptions.RequestException as exc:
                logger.debug("Account summary endpoint failed %s: %s", request_path, exc)
                continue
            except (ValueError, TypeError):
                continue

            nodes: list[Dict[str, Any]] = []
            stack: list[Any] = [payload]
            seen_ids: set[int] = set()
            while stack:
                node = stack.pop()
                node_id = id(node)
                if node_id in seen_ids:
                    continue
                seen_ids.add(node_id)
                if isinstance(node, dict):
                    nodes.append(node)
                    for value in node.values():
                        if isinstance(value, (dict, list)):
                            stack.append(value)
                elif isinstance(node, list):
                    for value in node:
                        if isinstance(value, (dict, list)):
                            stack.append(value)

            if not nodes:
                continue

            out: Dict[str, float] = {}

            def _extract_money(raw: Any, scale: str) -> Optional[float]:
                if raw is None:
                    return None
                if isinstance(raw, dict):
                    # Common nested money object forms: {"amount":"12.34"} / {"dollars":12.34}
                    for key in ("amount", "dollars", "value", "usd", "cash", "balance"):
                        if key in raw:
                            nested = _extract_money(raw.get(key), "dollars")
                            if nested is not None:
                                return nested
                    for key in ("fp", "fixed_point", "value_fp"):
                        if key in raw:
                            nested = _extract_money(raw.get(key), "fp")
                            if nested is not None:
                                return nested
                    for key in ("cents", "value_cents"):
                        if key in raw:
                            nested = _extract_money(raw.get(key), "cents")
                            if nested is not None:
                                return nested
                    return None
                if scale == "fp":
                    return _fixed_point_to_float(raw)
                parsed = _to_float_value(raw)
                if parsed is None:
                    return None
                if scale == "cents":
                    return parsed / 100.0
                return parsed

            field_map: Dict[str, tuple[tuple[str, str], ...]] = {
                "account_balance_dollars": (
                    ("account_balance_dollars", "dollars"),
                    ("balance_dollars", "dollars"),
                    ("cash_balance_dollars", "dollars"),
                    ("account_balance_fp", "fp"),
                    ("balance_fp", "fp"),
                    ("cash_balance_fp", "fp"),
                    ("account_balance_cents", "cents"),
                    ("balance_cents", "cents"),
                    ("cash_balance_cents", "cents"),
                    ("accountBalanceDollars", "dollars"),
                    ("balanceDollars", "dollars"),
                    ("cashBalanceDollars", "dollars"),
                ),
                "available_to_trade_dollars": (
                    ("available_to_trade_dollars", "dollars"),
                    ("available_balance_dollars", "dollars"),
                    ("cash_available_dollars", "dollars"),
                    ("available_to_trade_fp", "fp"),
                    ("available_balance_fp", "fp"),
                    ("cash_available_fp", "fp"),
                    ("available_to_trade_cents", "cents"),
                    ("available_balance_cents", "cents"),
                    ("cash_available_cents", "cents"),
                    ("availableToTradeDollars", "dollars"),
                    ("availableBalanceDollars", "dollars"),
                    ("cashAvailableDollars", "dollars"),
                ),
                "buying_power_dollars": (
                    ("buying_power_dollars", "dollars"),
                    ("available_buying_power_dollars", "dollars"),
                    ("buying_power_fp", "fp"),
                    ("available_buying_power_fp", "fp"),
                    ("buying_power_cents", "cents"),
                    ("available_buying_power_cents", "cents"),
                    ("buyingPowerDollars", "dollars"),
                    ("availableBuyingPowerDollars", "dollars"),
                ),
                "portfolio_value_dollars": (
                    ("portfolio_value_dollars", "dollars"),
                    ("account_value_dollars", "dollars"),
                    ("equity_dollars", "dollars"),
                    ("portfolio_value_fp", "fp"),
                    ("account_value_fp", "fp"),
                    ("equity_fp", "fp"),
                    ("portfolio_value_cents", "cents"),
                    ("account_value_cents", "cents"),
                    ("equity_cents", "cents"),
                    ("portfolioValueDollars", "dollars"),
                    ("accountValueDollars", "dollars"),
                    ("equityDollars", "dollars"),
                ),
            }
            for target, aliases in field_map.items():
                value = None
                for node in nodes:
                    for alias, scale in aliases:
                        if alias not in node:
                            continue
                        value = _extract_money(node.get(alias), scale)
                        if value is not None:
                            break
                    if value is not None:
                        break
                if value is not None:
                    out[target] = value

            # If buying power is absent but we do have available cash, use that as
            # a safe lower-bound proxy for bankroll gating.
            if (
                out.get("buying_power_dollars") is None
                and out.get("available_to_trade_dollars") is not None
            ):
                out["buying_power_dollars"] = float(out["available_to_trade_dollars"])

            if out:
                return out
        return {}

    def fetch_resting_orders(self, ticker: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch currently resting orders for dedupe checks."""
        if not self.api_key:
            return []
        if self._load_private_key() is None:
            return []

        request_path = "/trade-api/v2/portfolio/orders"
        params: Dict[str, Any] = {"status": "resting", "limit": 200}
        if ticker:
            params["ticker"] = ticker
        try:
            response = requests.get(
                f"{KALSHI_API_BASE}/portfolio/orders",
                params=params,
                headers=self._get_signed_headers("GET", request_path),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to fetch resting orders: %s", exc)
            return []
        except (ValueError, TypeError) as exc:
            logger.warning("Failed to parse resting orders response: %s", exc)
            return []

        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            payload = data
        for key in ("orders", "resting_orders", "portfolio_orders"):
            rows = payload.get(key) if isinstance(payload, dict) else None
            if isinstance(rows, list):
                return rows
        return []

    def cancel_order(self, order_id: str) -> Tuple[bool, str]:
        """Best-effort order cancel across known API variants."""
        if not self.api_key:
            return (False, "missing API key")
        if self._load_private_key() is None:
            return (False, "missing/invalid private key")
        order_id = str(order_id).strip()
        if not order_id:
            return (False, "missing order_id")

        endpoint_variants: List[Tuple[str, str, str, Optional[Dict[str, Any]]]] = [
            ("DELETE", f"/portfolio/orders/{order_id}", f"/trade-api/v2/portfolio/orders/{order_id}", None),
            ("POST", f"/portfolio/orders/{order_id}/cancel", f"/trade-api/v2/portfolio/orders/{order_id}/cancel", {}),
            ("DELETE", f"/portfolio/orders/{order_id}/cancel", f"/trade-api/v2/portfolio/orders/{order_id}/cancel", None),
            ("POST", "/portfolio/orders/cancel", "/trade-api/v2/portfolio/orders/cancel", {"order_id": order_id}),
            ("POST", "/portfolio/orders/cancel", "/trade-api/v2/portfolio/orders/cancel", {"id": order_id}),
        ]

        last_error = "unknown cancel error"
        for method, url_path, request_path, payload in endpoint_variants:
            transient_retries = 2
            attempt = 0
            while True:
                attempt += 1
                try:
                    if method == "DELETE":
                        response = requests.delete(
                            f"{KALSHI_API_BASE}{url_path}",
                            headers=self._get_signed_headers("DELETE", request_path),
                            timeout=API_TIMEOUT,
                        )
                    else:
                        response = requests.post(
                            f"{KALSHI_API_BASE}{url_path}",
                            json=payload,
                            headers=self._get_signed_headers("POST", request_path),
                            timeout=API_TIMEOUT,
                        )
                    if response.status_code in {200, 201, 202, 204, 409}:
                        return (True, f"ok ({method} {url_path})")
                    body = response.text[:300]
                    last_error = f"{method} {url_path} -> HTTP {response.status_code}: {body}"
                    if _is_transient_http_status(response.status_code) and attempt <= transient_retries:
                        time.sleep(0.25 * attempt)
                        continue
                except requests.exceptions.RequestException as exc:
                    last_error = f"{method} {url_path} -> {exc}"
                    if attempt <= transient_retries:
                        time.sleep(0.25 * attempt)
                        continue
                break
        return (False, last_error)

    def cancel_resting_entry_orders(
        self,
        *,
        client_order_prefix: Optional[str] = None,
        ticker: Optional[str] = None,
        max_orders: int = 20,
    ) -> Tuple[int, str]:
        """
        Cancel resting non-reduce BUY orders, optionally filtered by client_order_id prefix.
        Returns (cancel_count, detail_message).
        """
        orders = self.fetch_resting_orders(ticker=ticker)
        if not orders:
            return (0, "no resting orders")

        candidates: List[Tuple[str, str]] = []
        for order in orders:
            try:
                action = str(order.get("action", "")).strip().lower()
                if action != "buy":
                    continue
                if _to_bool(order.get("reduce_only", False)):
                    continue
                client_order_id = str(order.get("client_order_id", "") or "").strip()
                if client_order_prefix and not client_order_id.startswith(client_order_prefix):
                    continue
                order_id = (
                    order.get("order_id")
                    or order.get("id")
                    or order.get("orderId")
                    or order.get("orderID")
                )
                if order_id is None:
                    continue
                candidates.append((str(order_id), client_order_id))
            except Exception:
                continue

        if not candidates:
            return (0, "no matching resting entry BUY orders")

        canceled = 0
        errors: List[str] = []
        for order_id, _client_order_id in candidates[: max(1, int(max_orders))]:
            ok, reason = self.cancel_order(order_id)
            if ok:
                canceled += 1
            else:
                errors.append(f"{order_id}: {reason}")

        if not errors:
            return (canceled, "ok")
        detail = "; ".join(errors[:2])
        if len(errors) > 2:
            detail += f"; +{len(errors) - 2} more errors"
        return (canceled, detail)

    def place_reduce_only_sell_limit(
        self,
        ticker: str,
        side: str,
        count: int,
        limit_price_cents: int,
        client_order_id: str,
    ) -> Tuple[bool, str]:
        """
        Place a reduce-only limit sell order.

        side must be YES/NO from position perspective.
        """
        if not self.api_key:
            return (False, "missing API key")
        if self._load_private_key() is None:
            return (False, "missing/invalid private key")
        side_l = side.lower()
        if side_l not in {"yes", "no"}:
            return (False, f"invalid side: {side}")
        if count <= 0:
            return (False, "count must be > 0")
        limit_price_cents = max(1, min(99, int(limit_price_cents)))

        request_path = "/trade-api/v2/portfolio/orders"
        base_payload: Dict[str, Any] = {
            "ticker": ticker,
            "action": "sell",
            "side": side_l,
            "type": "limit",
            "count": int(count),
            "reduce_only": True,
            "client_order_id": client_order_id,
        }
        if side_l == "yes":
            base_payload["yes_price"] = limit_price_cents
        else:
            base_payload["no_price"] = limit_price_cents

        payload_variants: List[Tuple[str, Dict[str, Any]]] = [
            (
                "time_in_force=immediate_or_cancel",
                {**base_payload, "time_in_force": "immediate_or_cancel"},
            ),
            (
                "time_in_force=IMMEDIATE_OR_CANCEL",
                {**base_payload, "time_in_force": "IMMEDIATE_OR_CANCEL"},
            ),
            ("time_in_force=ioc", {**base_payload, "time_in_force": "ioc"}),
            ("time_in_force=IOC", {**base_payload, "time_in_force": "IOC"}),
            ("timeInForce=immediate_or_cancel", {**base_payload, "timeInForce": "immediate_or_cancel"}),
            ("timeInForce=ioc", {**base_payload, "timeInForce": "ioc"}),
            ("timeInForce=IOC", {**base_payload, "timeInForce": "IOC"}),
        ]

        last_error = "unknown order error"
        for variant_name, payload in payload_variants:
            transient_retries = 2
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = requests.post(
                        f"{KALSHI_API_BASE}/portfolio/orders",
                        json=payload,
                        headers=self._get_signed_headers("POST", request_path),
                        timeout=API_TIMEOUT,
                    )
                    if response.status_code == 409:
                        # Usually means duplicate client_order_id.
                        logger.info("Order skipped (duplicate client_order_id): %s", client_order_id)
                        return (True, "duplicate client_order_id (already placed)")
                    if response.status_code >= 400:
                        body = response.text[:400]
                        last_error = f"{variant_name} -> HTTP {response.status_code}: {body}"
                        if _is_transient_http_status(response.status_code) and attempt <= transient_retries:
                            time.sleep(0.35 * attempt)
                            continue
                        # Retry across variants on known schema/value order-validation mismatches.
                        if response.status_code == 400 and (
                            ("TimeInForce" in body and "oneof" in body)
                            or ("reduce_only can only be used with IoC orders" in body)
                            or ("invalid_parameters" in body)
                            or ("invalid_order" in body)
                        ):
                            continue
                    response.raise_for_status()
                    return (True, f"ok ({variant_name})")
                except requests.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else "unknown"
                    body = exc.response.text[:400] if exc.response is not None else ""
                    last_error = f"{variant_name} -> HTTP {status}: {body}"
                    if isinstance(status, int) and _is_transient_http_status(status) and attempt <= transient_retries:
                        time.sleep(0.35 * attempt)
                        continue
                    if status == 400:
                        # Keep probing alternate payload shapes for 400 validation errors.
                        continue
                    logger.warning("Failed to place order (%s): %s", ticker, last_error)
                    break
                except requests.exceptions.RequestException as exc:
                    last_error = f"{variant_name} -> {exc}"
                    if attempt <= transient_retries:
                        time.sleep(0.35 * attempt)
                        continue
                    logger.warning("Failed to place order (%s): %s", ticker, exc)
                    break
                break

        return (False, last_error)

    def place_entry_buy_limit(
        self,
        ticker: str,
        side: str,
        count: int,
        limit_price_cents: int,
        client_order_id: str,
        prefer_resting: bool = False,
    ) -> Tuple[bool, str]:
        """Place a limit buy order for entry (non-reduce-only)."""
        if not self.api_key:
            return (False, "missing API key")
        if self._load_private_key() is None:
            return (False, "missing/invalid private key")
        side_l = side.lower()
        if side_l not in {"yes", "no"}:
            return (False, f"invalid side: {side}")
        if count <= 0:
            return (False, "count must be > 0")
        limit_price_cents = max(1, min(99, int(limit_price_cents)))

        request_path = "/trade-api/v2/portfolio/orders"
        base_payload: Dict[str, Any] = {
            "ticker": ticker,
            "action": "buy",
            "side": side_l,
            "type": "limit",
            "count": int(count),
            "client_order_id": client_order_id,
        }
        if side_l == "yes":
            base_payload["yes_price"] = limit_price_cents
        else:
            base_payload["no_price"] = limit_price_cents

        ioc_variants: List[Tuple[str, Dict[str, Any]]] = [
            ("time_in_force=immediate_or_cancel", {**base_payload, "time_in_force": "immediate_or_cancel"}),
            ("time_in_force=IOC", {**base_payload, "time_in_force": "IOC"}),
            ("timeInForce=immediate_or_cancel", {**base_payload, "timeInForce": "immediate_or_cancel"}),
            ("timeInForce=IOC", {**base_payload, "timeInForce": "IOC"}),
        ]
        gtc_variants: List[Tuple[str, Dict[str, Any]]] = [
            ("time_in_force=good_til_cancelled", {**base_payload, "time_in_force": "good_til_cancelled"}),
            ("time_in_force=GTC", {**base_payload, "time_in_force": "GTC"}),
            ("timeInForce=good_til_cancelled", {**base_payload, "timeInForce": "good_til_cancelled"}),
            ("timeInForce=GTC", {**base_payload, "timeInForce": "GTC"}),
        ]
        payload_variants: List[Tuple[str, Dict[str, Any]]] = (
            gtc_variants + ioc_variants if prefer_resting else ioc_variants + gtc_variants
        )

        last_error = "unknown order error"
        for variant_name, payload in payload_variants:
            transient_retries = 2
            attempt = 0
            while True:
                attempt += 1
                try:
                    response = requests.post(
                        f"{KALSHI_API_BASE}/portfolio/orders",
                        json=payload,
                        headers=self._get_signed_headers("POST", request_path),
                        timeout=API_TIMEOUT,
                    )
                    if response.status_code == 409:
                        return (True, "duplicate client_order_id (already placed)")
                    if response.status_code >= 400:
                        body = response.text[:400]
                        last_error = f"{variant_name} -> HTTP {response.status_code}: {body}"
                        if _is_transient_http_status(response.status_code) and attempt <= transient_retries:
                            time.sleep(0.35 * attempt)
                            continue
                        if response.status_code == 400 and (
                            ("TimeInForce" in body and "oneof" in body)
                            or ("invalid_parameters" in body)
                            or ("invalid_order" in body)
                        ):
                            continue
                    response.raise_for_status()
                    return (True, f"ok ({variant_name})")
                except requests.exceptions.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else "unknown"
                    body = exc.response.text[:400] if exc.response is not None else ""
                    last_error = f"{variant_name} -> HTTP {status}: {body}"
                    if isinstance(status, int) and _is_transient_http_status(status) and attempt <= transient_retries:
                        time.sleep(0.35 * attempt)
                        continue
                    if status == 400:
                        continue
                    break
                except requests.exceptions.RequestException as exc:
                    last_error = f"{variant_name} -> {exc}"
                    if attempt <= transient_retries:
                        time.sleep(0.35 * attempt)
                        continue
                    break
                break

        return (False, last_error)

    def has_resting_reduce_like_order(
        self,
        ticker: str,
        side: str,
        price_cents: int,
    ) -> bool:
        """
        Check whether a similar resting sell order already exists.
        """
        orders = self.fetch_resting_orders(ticker=ticker)
        side_l = side.lower()
        for order in orders:
            try:
                if str(order.get("ticker", "")).strip() != ticker:
                    continue
                if str(order.get("action", "")).strip().lower() != "sell":
                    continue
                if str(order.get("side", "")).strip().lower() != side_l:
                    continue
                if not _to_bool(order.get("reduce_only", True)):
                    continue
                existing_price = None
                if side_l == "yes":
                    existing_price = self._to_cents(order.get("yes_price"))
                else:
                    existing_price = self._to_cents(order.get("no_price"))
                if existing_price is None:
                    existing_price = self._to_cents(order.get("price"))
                if existing_price is None:
                    continue
                if int(existing_price) == int(price_cents):
                    return True
            except Exception:
                continue
        return False

    def has_resting_entry_like_order(
        self,
        ticker: str,
        side: str,
        price_cents: int,
    ) -> bool:
        """Check whether a similar resting buy order already exists."""
        orders = self.fetch_resting_orders(ticker=ticker)
        side_l = side.lower()
        for order in orders:
            try:
                if str(order.get("ticker", "")).strip() != ticker:
                    continue
                if str(order.get("action", "")).strip().lower() != "buy":
                    continue
                if str(order.get("side", "")).strip().lower() != side_l:
                    continue
                existing_price = None
                if side_l == "yes":
                    existing_price = self._to_cents(order.get("yes_price"))
                else:
                    existing_price = self._to_cents(order.get("no_price"))
                if existing_price is None:
                    existing_price = self._to_cents(order.get("price"))
                if existing_price is None:
                    continue
                if int(existing_price) == int(price_cents):
                    return True
            except Exception:
                continue
        return False

    def fetch_brackets(self, target_date: str) -> List[MarketBracket]:
        """Fetch all brackets for a target date's temperature market."""
        date_str = format_date_for_ticker(target_date)
        expected_event_ticker = f"{self.series_ticker}-{date_str}"

        # Query the exact event first so we don't rely on series-level page order.
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
    private_key = os.getenv("KALSHI_PRIVATE_KEY", "") or api_secret
    api_key = os.getenv("KALSHI_API_KEY", "") or os.getenv("KALSHI_API_KEY_ID", "")

    return {
        "has_api_key": bool(api_key),
        "api_key_prefix": api_key[:8] if api_key else "",
        "has_api_secret": bool(api_secret),
        "has_private_key": bool(private_key),
        "authenticated_requests_enabled": bool(api_key and private_key and serialization is not None),
        "api_base": KALSHI_API_BASE,
        "markets_url": KALSHI_MARKETS_URL,
    }
