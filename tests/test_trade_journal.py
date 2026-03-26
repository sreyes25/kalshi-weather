from pathlib import Path

import pytest

from kalshi_weather.utils.trade_journal import TradeJournal


def test_compute_paper_totals_tracks_cash_equity_and_pnl(tmp_path: Path):
    db_path = tmp_path / "journal.sqlite"
    journal = TradeJournal(str(db_path))

    journal.log_order_event(
        event_type="buy_attempt",
        ticker="KXHIGH-NYC-20260324-B49",
        side="YES",
        qty=10,
        price_cents=40,
        status="dry_run",
    )
    journal.log_order_event(
        event_type="buy_attempt",
        ticker="KXHIGH-NYC-20260324-B49",
        side="YES",
        qty=5,
        price_cents=50,
        status="dry_run",
    )
    journal.log_order_event(
        event_type="sell_attempt",
        ticker="KXHIGH-NYC-20260324-B49",
        side="YES",
        qty=6,
        price_cents=60,
        status="dry_run",
    )

    totals = journal.compute_paper_totals(
        starting_balance_dollars=20.0,
        mark_price_cents_by_key={"KXHIGH-NYC-20260324-B49|YES": 50},
    )

    assert totals["starting_balance_dollars"] == 20.0
    assert totals["cash_dollars"] == pytest.approx(17.1)
    assert totals["cost_basis_dollars"] == pytest.approx(3.9)
    assert totals["market_value_dollars"] == pytest.approx(4.5)
    assert totals["equity_dollars"] == pytest.approx(21.6)
    assert totals["realized_pnl_dollars"] == pytest.approx(1.0)
    assert totals["unrealized_pnl_dollars"] == pytest.approx(0.6)
    assert totals["total_pnl_dollars"] == pytest.approx(1.6)
    assert totals["return_pct"] == pytest.approx(0.08)
    assert totals["open_positions"] == 1.0
    assert totals["open_contracts"] == 9.0


def test_compute_paper_totals_ignores_failed_and_unmatched_sells(tmp_path: Path):
    db_path = tmp_path / "journal.sqlite"
    journal = TradeJournal(str(db_path))

    journal.log_order_event(
        event_type="buy_attempt",
        ticker="KXHIGH-NYC-20260324-B50",
        side="YES",
        qty=10,
        price_cents=42,
        status="failed",
    )
    journal.log_order_event(
        event_type="sell_attempt",
        ticker="KXHIGH-NYC-20260324-B50",
        side="YES",
        qty=4,
        price_cents=55,
        status="dry_run",
    )

    totals = journal.compute_paper_totals(
        starting_balance_dollars=20.0,
        mark_price_cents_by_key={},
    )

    assert totals["cash_dollars"] == pytest.approx(20.0)
    assert totals["market_value_dollars"] == pytest.approx(0.0)
    assert totals["equity_dollars"] == pytest.approx(20.0)
    assert totals["total_pnl_dollars"] == pytest.approx(0.0)
    assert totals["open_positions"] == 0.0
    assert totals["open_contracts"] == 0.0


def test_compute_paper_totals_supports_clean_slate_by_event_id(tmp_path: Path):
    db_path = tmp_path / "journal.sqlite"
    journal = TradeJournal(str(db_path))

    journal.log_order_event(
        event_type="buy_attempt",
        ticker="KXHIGH-NYC-20260324-B49",
        side="YES",
        qty=10,
        price_cents=40,
        status="dry_run",
    )
    baseline_id = journal.latest_order_event_id()

    journal.log_order_event(
        event_type="buy_attempt",
        ticker="KXHIGH-NYC-20260324-B49",
        side="YES",
        qty=2,
        price_cents=50,
        status="dry_run",
    )

    totals = journal.compute_paper_totals(
        starting_balance_dollars=20.0,
        mark_price_cents_by_key={"KXHIGH-NYC-20260324-B49|YES": 50},
        min_event_id_exclusive=baseline_id,
    )

    assert totals["cash_dollars"] == pytest.approx(19.0)
    assert totals["market_value_dollars"] == pytest.approx(1.0)
    assert totals["equity_dollars"] == pytest.approx(20.0)
    assert totals["total_pnl_dollars"] == pytest.approx(0.0)
    assert totals["open_contracts"] == pytest.approx(2.0)
