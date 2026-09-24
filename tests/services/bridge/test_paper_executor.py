"""Tests for PaperExecutor — fill simulation + bar-by-bar exit detection."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from src.services.bridge.paper_executor import OpenPosition, PaperExecutor


def test_open_position_uses_next_min_bar(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Fill price = next-minute bar.open + half slippage (BUY → +)."""
    # produced_at = 12:00:00 UTC → 15:00 MSK
    signal_ts_utc = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    # candles starting 14:00 MSK, base 100, no slope — next-min bar at 15:01 MSK has open=100
    cache = candle_cache_factory(ticker="GAZP",
                                 start=pd.Timestamp("2026-05-25 14:00:00"),
                                 close_base=100.0, slope=0.0)
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is not None
    # half slippage GAZP = 0.04/2/100 = 0.0002 → 100 × 1.0002 ≈ 100.02
    assert pos.entry_price == pytest.approx(100.02, abs=0.01)
    assert pos.ticker == "GAZP"
    assert pos.quantity == 10
    assert pos.side == "BUY"


def test_open_position_sell_slippage_negative(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """SELL — fill BELOW market (slippage works against direction)."""
    signal_ts_utc = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="SELL",
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    cache = candle_cache_factory(close_base=100.0)
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is not None
    assert pos.entry_price == pytest.approx(99.98, abs=0.01)


def test_check_exit_tp_buy(bridge_settings, candle_cache_factory):
    """BUY position, TP at 102 — high≥102 на каком-то баре → exit_reason=tp."""
    cache = candle_cache_factory(ticker="GAZP",
                                 start=pd.Timestamp("2026-05-25 14:00:00"),
                                 close_base=100.0, slope=2.0)  # ~2 руб / hour
    pos = OpenPosition(
        signal_event_id="x", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso="2026-05-25T11:00:00.000+00:00",  # 14:00 MSK
        horizon_min=60, notional_rub=1000.0,
    )
    executor = PaperExecutor(bridge_settings, cache)
    # now_msk = 16:00 — far enough that TP at 102 уже достигнут
    outcome = executor.check_exit(pos, now_msk=pd.Timestamp("2026-05-25 16:00:00"))
    assert outcome is not None
    assert outcome.exit_reason == "tp"
    assert outcome.realized_pnl_rub > 0


def test_check_exit_sl_buy(bridge_settings, candle_cache_factory):
    """BUY с очень узким SL — at first bar low → exit_reason=sl."""
    # Custom lows: second bar low=98 < sl=99 (entry-bar at index 0 excluded by window > entry_ts)
    n = 100
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        n_bars=n,
        custom_lows=[99.5, 98.0] + [99.5] * (n - 2),
        custom_highs=[100.5] * n,
    )
    pos = OpenPosition(
        signal_event_id="x", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso="2026-05-25T11:00:00.000+00:00",
        horizon_min=60, notional_rub=1000.0,
    )
    executor = PaperExecutor(bridge_settings, cache)
    outcome = executor.check_exit(pos, now_msk=pd.Timestamp("2026-05-25 16:00:00"))
    assert outcome is not None
    assert outcome.exit_reason == "sl"
    assert outcome.realized_pnl_rub < 0


def test_check_exit_time(bridge_settings, candle_cache_factory):
    """No SL/TP touch + elapsed horizon → exit_reason=time, exit at last close."""
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.5,  # slow drift, не дойдёт до TP=102 за 60 мин
    )
    pos = OpenPosition(
        signal_event_id="x", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso="2026-05-25T11:00:00.000+00:00",  # 14:00 MSK
        horizon_min=60, notional_rub=1000.0,
    )
    executor = PaperExecutor(bridge_settings, cache)
    outcome = executor.check_exit(pos, now_msk=pd.Timestamp("2026-05-25 15:01:00"))
    assert outcome is not None
    assert outcome.exit_reason == "time"


def test_check_exit_returns_none_if_alive(bridge_settings, candle_cache_factory):
    """now < horizon_min, no SL/TP hit → returns None (position alive)."""
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,  # cena стоит
    )
    pos = OpenPosition(
        signal_event_id="x", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso="2026-05-25T11:00:00.000+00:00",  # 14:00 MSK
        horizon_min=60, notional_rub=1000.0,
    )
    executor = PaperExecutor(bridge_settings, cache)
    outcome = executor.check_exit(pos, now_msk=pd.Timestamp("2026-05-25 14:30:00"))
    assert outcome is None


def test_open_position_rejected_on_large_price_drift(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Sprint 6: |bar.open - signal.entry_price| / signal.entry_price > 1% → REJECT.

    Reproduces 2026-06-01 bug: Predictor's last_close was 125.83 (April-20 CSV),
    Bridge fills at 116.32 (today live) — divergence 7.6% >> 1%. With drift gate,
    open_position returns None.
    """
    # Activate drift gate at 1% (conftest defaults disable it for legacy tests)
    bridge_settings.max_entry_drift_pct = 0.01
    # Cache bar open = 100.0 everywhere
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        close_base=100.0, slope=0.0,
    )
    signal_ts_utc = datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc)  # 13:00 MSK
    # Signal claims entry_price=120 (5+ apart from bar open=100) → 20% drift
    signal = execute_signal_factory(
        ticker="GAZP", side="SELL",
        entry_price=120.0, stop_loss=126.0, take_profit=115.0,
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is None  # drift gate rejected


def test_open_position_tolerates_small_price_drift(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Drift of 0.5% (under 1% threshold) → still fills."""
    bridge_settings.max_entry_drift_pct = 0.01
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        close_base=100.0, slope=0.0,
    )
    signal_ts_utc = datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc)
    # entry_price=100.5 → bar open=100.0 → drift 0.5% < 1% → ok
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        entry_price=100.5, stop_loss=99.5, take_profit=102.0,
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is not None


def test_open_position_drift_gate_configurable(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Raise threshold to 25% — even 20% drift fills."""
    bridge_settings.max_entry_drift_pct = 0.25
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        close_base=100.0, slope=0.0,
    )
    signal_ts_utc = datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="SELL",
        entry_price=120.0, stop_loss=126.0, take_profit=115.0,
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is not None  # gate raised → drift allowed


def test_open_position_rejected_when_candles_stale_beyond_threshold(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Sprint 6: gap = signal_time - last_bar > 600s → REJECT (return None).

    Candles end at 21:59 MSK (= 18:59 UTC). Signal at 19:30 UTC (=22:30 MSK).
    next_min hits past last bar → fallback path. gap = 22:30 - 21:59 = 31 min
    > 10 min default threshold → reject.
    """
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        n_bars=600,  # ends at 21:59 MSK
    )
    signal_ts_utc = datetime(2026, 5, 25, 19, 30, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is None  # stale-bar gate triggered


def test_open_position_fallback_within_threshold_still_fills(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Sprint 6: gap = signal_time - last_bar ≤ threshold → fallback to last close.

    Candles end at 21:59 MSK. Signal at 19:05 UTC (=22:05 MSK).
    Fallback path. gap = 22:05 - 21:59 = 6 min < 10 min threshold → fills OK.
    """
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        n_bars=600,
    )
    signal_ts_utc = datetime(2026, 5, 25, 19, 5, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is not None
    assert pos.ticker == "GAZP"


def test_open_position_stale_threshold_configurable(
    bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Lowering threshold to 60s makes 6-min gap also reject."""
    bridge_settings.stale_bar_threshold_sec = 60
    cache = candle_cache_factory(
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 12:00:00"),
        n_bars=600,
    )
    signal_ts_utc = datetime(2026, 5, 25, 19, 5, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        produced_at=signal_ts_utc.isoformat(timespec="milliseconds"),
    )
    executor = PaperExecutor(bridge_settings, cache)
    pos = executor.open_position(signal)
    assert pos is None  # 6 min gap > 60s threshold


def test_open_position_serialize_round_trip():
    """OpenPosition.to_json/from_json — для Redis persistence."""
    pos = OpenPosition(
        signal_event_id="x", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso="2026-05-25T11:00:00.000+00:00",
        horizon_min=60, notional_rub=1000.0,
    )
    restored = OpenPosition.from_json(pos.to_json())
    assert restored.signal_event_id == pos.signal_event_id
    assert restored.entry_price == pos.entry_price
    assert restored.notional_rub == pos.notional_rub
