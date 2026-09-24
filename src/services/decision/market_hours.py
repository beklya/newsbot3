"""Market hours gate (Sprint 6 — paper trading session filter).

MOEX trading hours by asset class (Moscow time, GMT+3):

  Stocks (Stack T+1):
    Morning auction + continuous:  06:50 — 09:49:59
    Main session:                  09:50 — 18:59:30
    Evening session:               19:00:01 — 23:49:59
    → combined gate: 06:50 ≤ t ≤ 23:49:59

  Futures (FORTS):
    Opening auction:               08:50 — 09:00
    Morning:                       09:00 — 10:00
    Main:                          10:00 — 19:00
    Evening:                       19:00 — 23:50
    → combined gate: 08:50 ≤ t ≤ 23:49:59

  Indices: same as stocks (computed during stock session).

  Currency / commodity: treated same as futures (FORTS hours).

Weekend handling:
  MOEX has weekend sessions (Доп.сессия выходного дня) on calendar
  weekend days that are working days per Russian production calendar
  (~3-4 days/year). Phase 2 backtest did not include weekend session
  data, so we skip Saturday/Sunday entirely by default to avoid
  distribution shift. Configurable via skip_weekends.

Technical clearing break for futures (14:00-14:05) is ignored — only
5 minutes and not worth code complexity.
"""
from __future__ import annotations

from datetime import datetime, time
from typing import Tuple
from zoneinfo import ZoneInfo

from src.contracts.instruments import get_instrument_meta

MSK = ZoneInfo("Europe/Moscow")

# Stocks T+1: pre-open auction + morning + main + evening
STOCK_OPEN = time(6, 50, 0)
STOCK_CLOSE = time(23, 49, 59)

# Futures FORTS: opening auction + morning + main + evening
FUTURES_OPEN = time(8, 50, 0)
FUTURES_CLOSE = time(23, 49, 59)


def _hours_for_asset_class(asset_class: str) -> Tuple[time, time]:
    """Return (open_t, close_t) MSK for the given asset_class.

    Stocks / equity → stock hours.
    Futures / commodity / currency → FORTS hours (FORTS session covers all of these).
    Unknown asset_class falls back to stock hours (conservative — narrower window).
    """
    if asset_class == "equity":
        return STOCK_OPEN, STOCK_CLOSE
    if asset_class in ("futures", "commodity", "currency"):
        return FUTURES_OPEN, FUTURES_CLOSE
    # Unknown — be conservative (assume stock window)
    return STOCK_OPEN, STOCK_CLOSE


def is_market_open(
    now_utc: datetime,
    asset_class: str,
    *,
    skip_weekends: bool = True,
) -> Tuple[bool, str]:
    """Check whether MOEX is open for the given asset_class at now_utc.

    Args:
        now_utc: UTC datetime (tz-aware or naive treated as UTC).
        asset_class: from InstrumentMeta — "equity" / "futures" / "commodity" / "currency".
        skip_weekends: if True (default), Saturday and Sunday return False
            regardless of session window (we intentionally skip weekend sessions
            because Phase 2 backtest didn't cover them).

    Returns:
        (is_open, reason). reason is "ok" when open, otherwise:
          - "weekend"      — Saturday or Sunday with skip_weekends=True
          - "before_open"  — weekday but before session start (MSK)
          - "after_close"  — weekday but after session end (MSK)
    """
    # Ensure tz-aware for MSK conversion
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=ZoneInfo("UTC"))
    now_msk = now_utc.astimezone(MSK)

    # Saturday=5, Sunday=6 (per Python ISO weekday convention)
    if skip_weekends and now_msk.weekday() >= 5:
        return False, "weekend"

    open_t, close_t = _hours_for_asset_class(asset_class)
    t = now_msk.time()

    if t < open_t:
        return False, "before_open"
    if t > close_t:
        return False, "after_close"
    return True, "ok"


def is_market_open_for_ticker(
    now_utc: datetime,
    ticker: str,
    *,
    skip_weekends: bool = True,
) -> Tuple[bool, str]:
    """Resolve asset_class from instruments registry, then check hours.

    Convenience wrapper for pipeline callers that have a ticker but not asset_class.
    Unknown ticker raises KeyError (matches normalize_ticker contract).
    """
    meta = get_instrument_meta(ticker)
    return is_market_open(now_utc, meta.asset_class, skip_weekends=skip_weekends)
