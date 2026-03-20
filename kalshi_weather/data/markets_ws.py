"""
Optional Kalshi websocket quote client.

This is strictly for live advisory quote display. It does not place orders.
If websocket dependencies or auth are unavailable, it fails gracefully so
REST polling can continue.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Generator, Optional

logger = logging.getLogger(__name__)


@dataclass
class LiveQuoteUpdate:
    ticker: str
    yes_bid: Optional[int] = None
    yes_ask: Optional[int] = None
    last_price: Optional[int] = None


class KalshiQuoteWebsocketClient:
    """Best-effort websocket client for streaming quote updates."""

    def __init__(self, ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"):
        self.ws_url = ws_url
        self.api_key = os.getenv("KALSHI_API_KEY", "") or os.getenv("KALSHI_API_KEY_ID", "")
        self.api_secret = os.getenv("KALSHI_API_SECRET", "")

    @property
    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def iter_updates(self) -> Generator[LiveQuoteUpdate, None, None]:
        """
        Yield live quote updates when websocket auth/deps are available.

        On any setup/runtime issue, logs and exits without raising so callers can
        continue using REST polling mode.
        """
        if not self.is_configured:
            logger.info("Kalshi websocket not configured; falling back to REST polling")
            return

        try:
            from websocket import create_connection  # type: ignore
        except Exception:
            logger.info("websocket-client dependency unavailable; using REST polling")
            return

        try:
            ws = create_connection(self.ws_url, timeout=5)
        except Exception as exc:
            logger.warning("Failed to open Kalshi websocket: %s", exc)
            return

        try:
            while True:
                payload = ws.recv()
                if not payload:
                    continue
                try:
                    msg = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                market = msg.get("market") if isinstance(msg, dict) else None
                if not isinstance(market, dict):
                    continue

                ticker = market.get("ticker")
                if not ticker:
                    continue

                update = LiveQuoteUpdate(
                    ticker=ticker,
                    yes_bid=_to_cents(market.get("yes_bid")),
                    yes_ask=_to_cents(market.get("yes_ask")),
                    last_price=_to_cents(market.get("last_price")),
                )
                yield update
        except Exception as exc:
            logger.warning("Kalshi websocket stream ended: %s", exc)
        finally:
            try:
                ws.close()
            except Exception:
                pass


def _to_cents(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return int(round(numeric))
