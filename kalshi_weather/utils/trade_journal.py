"""SQLite-backed trade journal and bot-managed position registry."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Set


class TradeJournal:
    """Persist order events and bot-managed position keys."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS order_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    target_date TEXT,
                    ticker TEXT,
                    side TEXT,
                    qty INTEGER,
                    price_cents INTEGER,
                    status TEXT,
                    reason TEXT,
                    edge REAL,
                    confidence REAL,
                    model_prob REAL,
                    market_prob REAL,
                    client_order_id TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_positions (
                    key TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    side TEXT NOT NULL,
                    active INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def log_order_event(
        self,
        *,
        event_type: str,
        target_date: Optional[str] = None,
        ticker: Optional[str] = None,
        side: Optional[str] = None,
        qty: Optional[int] = None,
        price_cents: Optional[int] = None,
        status: Optional[str] = None,
        reason: Optional[str] = None,
        edge: Optional[float] = None,
        confidence: Optional[float] = None,
        model_prob: Optional[float] = None,
        market_prob: Optional[float] = None,
        client_order_id: Optional[str] = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO order_events (
                    ts, event_type, target_date, ticker, side, qty, price_cents,
                    status, reason, edge, confidence, model_prob, market_prob, client_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    event_type,
                    target_date,
                    ticker,
                    side,
                    qty,
                    price_cents,
                    status,
                    reason,
                    edge,
                    confidence,
                    model_prob,
                    market_prob,
                    client_order_id,
                ),
            )
            conn.commit()

    def set_managed_position(self, *, ticker: str, side: str, active: bool) -> None:
        key = f"{ticker}|{side.upper()}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO managed_positions (key, ticker, side, active, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    active=excluded.active,
                    updated_at=excluded.updated_at
                """,
                (
                    key,
                    ticker,
                    side.upper(),
                    1 if active else 0,
                    datetime.utcnow().isoformat(timespec="seconds"),
                ),
            )
            conn.commit()

    def active_managed_keys(self) -> Set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT key FROM managed_positions WHERE active = 1"
            ).fetchall()
        return {str(r[0]) for r in rows}

    def deactivate_missing(self, open_keys: Iterable[str]) -> None:
        open_set = set(open_keys)
        existing = self.active_managed_keys()
        stale = existing - open_set
        for key in stale:
            ticker, side = key.split("|", 1)
            self.set_managed_position(ticker=ticker, side=side, active=False)

    def estimate_open_entry_price_cents(self, *, ticker: str, side: str) -> Optional[int]:
        """
        Best-effort average entry from recent submitted buy events.

        Used as a fallback when the portfolio positions API omits average entry.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT qty, price_cents
                FROM order_events
                WHERE event_type='buy_attempt'
                  AND status IN ('submitted', 'filled')
                  AND ticker=?
                  AND side=?
                  AND (reason IS NULL OR lower(reason) NOT LIKE '%duplicate client_order_id%')
                  AND qty IS NOT NULL
                  AND qty > 0
                  AND price_cents IS NOT NULL
                ORDER BY id DESC
                LIMIT 32
                """,
                (ticker, side.upper()),
            ).fetchall()
        if not rows:
            return None
        total_qty = 0
        weighted = 0.0
        for qty, price in rows:
            try:
                q = int(qty)
                p = float(price)
            except (TypeError, ValueError):
                continue
            if q <= 0:
                continue
            total_qty += q
            weighted += p * q
        if total_qty <= 0:
            return None
        return int(round(weighted / float(total_qty)))

    def count_order_events_since(
        self,
        *,
        event_type: str,
        since_utc: datetime,
        statuses: Optional[Iterable[str]] = None,
    ) -> int:
        """
        Count order events since a UTC timestamp.

        Uses lexical ISO-8601 comparison (`YYYY-MM-DDTHH:MM:SS`) in SQLite.
        """
        since_iso = since_utc.strftime("%Y-%m-%dT%H:%M:%S")
        sql = "SELECT COUNT(1) FROM order_events WHERE event_type = ? AND ts >= ?"
        params: list[object] = [event_type, since_iso]
        if statuses:
            unique_statuses = [str(s) for s in statuses if s is not None]
            if unique_statuses:
                placeholders = ",".join("?" for _ in unique_statuses)
                sql += f" AND status IN ({placeholders})"
                params.extend(unique_statuses)
        with self._connect() as conn:
            row = conn.execute(sql, tuple(params)).fetchone()
        if not row:
            return 0
        try:
            return int(row[0] or 0)
        except (TypeError, ValueError):
            return 0

    def latest_order_event_id(self) -> int:
        """Return latest order_events row id, or 0 if table is empty."""
        with self._connect() as conn:
            row = conn.execute("SELECT MAX(id) FROM order_events").fetchone()
        if not row:
            return 0
        try:
            return int(row[0] or 0)
        except (TypeError, ValueError):
            return 0

    def compute_paper_totals(
        self,
        *,
        starting_balance_dollars: float,
        mark_price_cents_by_key: Optional[dict[str, int]] = None,
        min_event_id_exclusive: Optional[int] = None,
    ) -> dict[str, float]:
        """
        Reconstruct paper cash/equity by replaying journaled buy/sell attempts.

        This is intended for dry-run monitoring where submitted order attempts are
        treated as immediate paper fills.
        """
        starting_balance = max(0.0, float(starting_balance_dollars))
        cash = float(starting_balance)
        realized_pnl = 0.0

        normalized_marks: dict[str, int] = {}
        for raw_key, raw_mark in (mark_price_cents_by_key or {}).items():
            parts = str(raw_key).split("|", 1)
            if len(parts) != 2:
                continue
            try:
                mark_cents = int(round(float(raw_mark)))
            except (TypeError, ValueError):
                continue
            key = f"{parts[0]}|{parts[1].upper()}"
            normalized_marks[key] = max(0, min(100, mark_cents))

        positions: dict[str, dict[str, float]] = {}
        params: list[object] = []
        sql = """
                SELECT event_type, ticker, side, qty, price_cents
                FROM order_events
                WHERE event_type IN ('buy_attempt', 'sell_attempt')
                  AND status IN ('dry_run', 'submitted', 'filled')
                  AND ticker IS NOT NULL
                  AND side IS NOT NULL
                  AND qty IS NOT NULL
                  AND price_cents IS NOT NULL
                """
        if min_event_id_exclusive is not None:
            try:
                min_id = int(min_event_id_exclusive)
            except (TypeError, ValueError):
                min_id = None
            if min_id is not None and min_id > 0:
                sql += " AND id > ?"
                params.append(min_id)
        sql += " ORDER BY id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()

        for event_type, ticker, side, qty, price_cents in rows:
            try:
                contracts = int(qty)
                px = float(price_cents)
            except (TypeError, ValueError):
                continue
            if contracts <= 0 or px < 0:
                continue

            key = f"{str(ticker)}|{str(side).upper()}"
            state = positions.setdefault(key, {"qty": 0.0, "avg_entry_cents": 0.0})

            if event_type == "buy_attempt":
                prior_qty = int(state["qty"])
                next_qty = prior_qty + contracts
                if next_qty <= 0:
                    continue
                weighted_sum = (state["avg_entry_cents"] * prior_qty) + (px * contracts)
                state["avg_entry_cents"] = weighted_sum / float(next_qty)
                state["qty"] = float(next_qty)
                cash -= (px * contracts) / 100.0
                continue

            if event_type != "sell_attempt":
                continue

            held_qty = int(state["qty"])
            if held_qty <= 0:
                continue
            close_qty = min(held_qty, contracts)
            if close_qty <= 0:
                continue
            avg_entry = float(state["avg_entry_cents"])
            cash += (px * close_qty) / 100.0
            realized_pnl += ((px - avg_entry) * close_qty) / 100.0
            remaining = held_qty - close_qty
            if remaining <= 0:
                positions.pop(key, None)
            else:
                state["qty"] = float(remaining)

        cost_basis = 0.0
        market_value = 0.0
        open_positions = 0
        open_contracts = 0
        for key, state in positions.items():
            qty = int(state["qty"])
            if qty <= 0:
                continue
            avg_entry = float(state["avg_entry_cents"])
            cost_basis += (avg_entry * qty) / 100.0
            mark_cents = normalized_marks.get(key, int(round(avg_entry)))
            market_value += (float(mark_cents) * qty) / 100.0
            open_positions += 1
            open_contracts += qty

        unrealized_pnl = market_value - cost_basis
        equity = cash + market_value
        total_pnl = equity - starting_balance
        return_pct = (total_pnl / starting_balance) if starting_balance > 0 else 0.0

        return {
            "starting_balance_dollars": float(starting_balance),
            "cash_dollars": float(cash),
            "cost_basis_dollars": float(cost_basis),
            "market_value_dollars": float(market_value),
            "equity_dollars": float(equity),
            "realized_pnl_dollars": float(realized_pnl),
            "unrealized_pnl_dollars": float(unrealized_pnl),
            "total_pnl_dollars": float(total_pnl),
            "return_pct": float(return_pct),
            "open_positions": float(open_positions),
            "open_contracts": float(open_contracts),
        }
