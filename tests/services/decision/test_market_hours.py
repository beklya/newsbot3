"""Tests for Sprint 6 market hours gate."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.services.decision.market_hours import (
    is_market_open,
    is_market_open_for_ticker,
)


# Helper: 09:00 MSK = 06:00 UTC, 12:00 MSK = 09:00 UTC, etc.
def _utc(year: int, month: int, day: int, hour_msk: int, minute: int = 0) -> datetime:
    """Build UTC datetime that corresponds to the given MSK wall-time.

    Approximates +3h offset; we use fixed-offset rather than a tz lookup because
    Russia has no DST since 2014.
    """
    return datetime(year, month, day, hour_msk - 3, minute, tzinfo=timezone.utc)


# ── Stocks (equity) hours: 06:50 — 23:49:59 MSK ──────────────────────────


def test_stock_weekday_inside_main_session_is_open():
    # Monday 2026-06-01 12:00 MSK
    now = _utc(2026, 6, 1, 12, 0)
    assert now.weekday() == 0
    ok, reason = is_market_open(now, "equity")
    assert ok is True
    assert reason == "ok"


def test_stock_weekday_in_morning_session_is_open():
    # Monday 07:30 MSK — inside utrенняя session (07:00-09:49:59)
    now = _utc(2026, 6, 1, 7, 30)
    ok, reason = is_market_open(now, "equity")
    assert ok is True


def test_stock_weekday_in_evening_session_is_open():
    # Monday 22:00 MSK — inside вечерняя session (19:00:01-23:49:59)
    now = _utc(2026, 6, 1, 22, 0)
    ok, reason = is_market_open(now, "equity")
    assert ok is True


def test_stock_weekday_before_open_rejected():
    # Monday 06:00 MSK — before 06:50 open
    now = _utc(2026, 6, 1, 6, 0)
    ok, reason = is_market_open(now, "equity")
    assert ok is False
    assert reason == "before_open"


def test_stock_weekday_after_close_rejected():
    # Monday 23:55 MSK — after 23:49:59 close
    now = _utc(2026, 6, 1, 23, 55)
    ok, reason = is_market_open(now, "equity")
    assert ok is False
    assert reason == "after_close"


# ── Weekend handling ────────────────────────────────────────────────────


def test_weekend_saturday_rejected():
    # Saturday 2026-06-06 12:00 MSK — middle of "weekend session" but we skip
    now = _utc(2026, 6, 6, 12, 0)
    assert now.weekday() == 5  # Saturday
    ok, reason = is_market_open(now, "equity")
    assert ok is False
    assert reason == "weekend"


def test_weekend_sunday_rejected():
    now = _utc(2026, 6, 7, 14, 0)
    assert now.weekday() == 6
    ok, reason = is_market_open(now, "equity")
    assert ok is False
    assert reason == "weekend"


def test_weekend_when_skip_weekends_false_passes_hour_check():
    # Saturday at midnight MSK — weekend skip OFF, but still before session
    now = _utc(2026, 6, 6, 3, 0)
    ok, reason = is_market_open(now, "equity", skip_weekends=False)
    assert ok is False
    assert reason == "before_open"


def test_weekend_when_skip_weekends_false_inside_session_is_open():
    # Saturday 12:00 MSK with skip_weekends=False → open (would catch weekend session)
    now = _utc(2026, 6, 6, 12, 0)
    ok, reason = is_market_open(now, "equity", skip_weekends=False)
    assert ok is True


# ── Futures hours: 08:50 — 23:49:59 MSK ─────────────────────────────────


def test_futures_weekday_at_open_auction_is_open():
    # Monday 08:55 MSK — inside futures opening auction (08:50-09:00)
    now = _utc(2026, 6, 1, 8, 55)
    ok, reason = is_market_open(now, "futures")
    assert ok is True


def test_futures_weekday_before_open_rejected():
    # Monday 08:00 MSK — futures opens at 08:50
    now = _utc(2026, 6, 1, 8, 0)
    ok, reason = is_market_open(now, "futures")
    assert ok is False
    assert reason == "before_open"


def test_stock_weekday_at_0800_msk_is_open():
    # 08:00 MSK is inside stock morning session (since 06:50)
    now = _utc(2026, 6, 1, 8, 0)
    ok, _ = is_market_open(now, "equity")
    assert ok is True


# ── Asset class routing via ticker lookup ───────────────────────────────


def test_is_market_open_for_ticker_equity():
    # GAZP is equity — Monday 12:00 MSK → open
    now = _utc(2026, 6, 1, 12, 0)
    ok, _ = is_market_open_for_ticker(now, "GAZP")
    assert ok is True


def test_is_market_open_for_ticker_futures():
    # BR is futures — Monday 08:00 MSK → before_open (futures open 08:50)
    now = _utc(2026, 6, 1, 8, 0)
    ok, reason = is_market_open_for_ticker(now, "BR")
    assert ok is False
    assert reason == "before_open"


def test_is_market_open_for_ticker_legacy_name():
    # Legacy "MX" → MIX (futures) — same gate
    now = _utc(2026, 6, 1, 8, 0)
    ok, reason = is_market_open_for_ticker(now, "MX")
    assert ok is False
    assert reason == "before_open"


def test_is_market_open_for_ticker_unknown_raises():
    # Predictor whitelist should already filter — but if it leaks, KeyError surfaces
    now = _utc(2026, 6, 1, 12, 0)
    with pytest.raises(KeyError):
        is_market_open_for_ticker(now, "ZZZZ")


# ── Naive UTC input handling ────────────────────────────────────────────


def test_naive_utc_treated_as_utc():
    # 12:00 UTC = 15:00 MSK Monday → inside main session
    naive_utc = datetime(2026, 6, 1, 12, 0)
    ok, _ = is_market_open(naive_utc, "equity")
    assert ok is True
