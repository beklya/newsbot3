"""Shared fixtures for Bridge tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import fakeredis.aioredis

from src.contracts.trade_signal import (
    TradeSignalEvent,
    TradeSignalPayload,
)
from src.infra.candles import CandleCache
from src.services.bridge.config import BridgeSettings


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


@pytest.fixture
def bridge_settings(tmp_path: Path) -> BridgeSettings:
    """Settings with prices_dir stub — uses production Sprint-6 defaults.

    Tests that need cache prices to drift from signal.entry_price drive the
    drift gate explicitly: pass entry_price aligned to bar.open (Phase-2
    convention: bar.open = close_base + linear slope at index 0), or override
    max_entry_drift_pct per-test.
    """
    prices_dir = tmp_path / "prices"
    prices_dir.mkdir()
    # touch fake CSV (validator only checks dir existence)
    (prices_dir / "prices_GAZP.csv").write_text("dummy")
    return BridgeSettings(
        prices_dir=prices_dir,
        tracker_poll_interval_sec=0.05,  # fast polling в тестах
    )


def _build_candles_for(ticker: str, start: pd.Timestamp, *,
                       n_bars: int = 600, close_base: float = 100.0,
                       slope: float = 0.0) -> pd.DataFrame:
    idx = pd.date_range(start, periods=n_bars, freq="1min")
    closes = np.linspace(close_base, close_base + slope * n_bars / 60.0, n_bars)
    df = pd.DataFrame({
        "open": closes,
        "high": closes + 0.5,
        "low": closes - 0.5,
        "close": closes,
        "volume": np.full(n_bars, 1000.0),
    }, index=idx)
    df.index.name = "ts"
    return df


@pytest.fixture
def candle_cache_factory():
    """Build CandleCache с конкретным slope для targeted TP/SL hit scenarios."""

    def _factory(*, ticker: str = "GAZP",
                 start: pd.Timestamp = pd.Timestamp("2026-05-25 12:00:00"),
                 close_base: float = 100.0, slope: float = 0.0,
                 high_offset: float = 0.5, low_offset: float = -0.5,
                 n_bars: int = 600,
                 custom_highs: list[float] | None = None,
                 custom_lows: list[float] | None = None) -> CandleCache:
        cache = CandleCache(Path("nowhere"))
        df = _build_candles_for(ticker, start, n_bars=n_bars,
                                close_base=close_base, slope=slope)
        if custom_highs is not None:
            df["high"] = pd.Series(custom_highs, index=df.index[:len(custom_highs)])
        if custom_lows is not None:
            df["low"] = pd.Series(custom_lows, index=df.index[:len(custom_lows)])
        cache._candles[ticker] = df
        return cache

    return _factory


def _utc_now_minus(seconds: int = 0) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat(timespec="milliseconds")


@pytest.fixture
def execute_signal_factory():
    """Build TradeSignalEvent action=EXECUTE."""

    def _factory(
        *,
        event_id: str = "01JEVENT00000SIGNAL00001",
        prediction_event_id: str = "01JEVENT00000PREDICT0001",
        ticker: str = "GAZP",
        side: str = "BUY",
        entry_price: float = 100.0,
        stop_loss: float = 99.0,
        take_profit: float = 102.0,
        quantity: int = 10,
        produced_at: str | None = None,
    ) -> TradeSignalEvent:
        payload = TradeSignalPayload(
            prediction_event_id=prediction_event_id,
            action="EXECUTE",
            reject_reason="",
            ticker=ticker,
            side=side,  # type: ignore[arg-type]
            horizon="60m",
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            quantity=quantity,
            risk_rub=1000.0,
            expected_pnl_rub=2000.0,
            rr_ratio=2.0,
            open_positions=0,
            daily_pnl_pct=0.0,
            cooldown_active=False,
        )
        kwargs = {"event_id": event_id, "payload": payload}
        if produced_at:
            kwargs["produced_at"] = produced_at
        return TradeSignalEvent(**kwargs)

    return _factory


@pytest.fixture
def reject_signal_factory():
    def _factory(
        *,
        event_id: str = "01JEVENT00000SIGNAL00002",
        reason: str = "test reject",
    ) -> TradeSignalEvent:
        payload = TradeSignalPayload(
            prediction_event_id="01JEVENT00000PREDICT0002",
            action="REJECT",
            reject_reason=reason,
            ticker="GAZP",
            open_positions=0,
            daily_pnl_pct=0.0,
            cooldown_active=False,
        )
        return TradeSignalEvent(event_id=event_id, payload=payload)

    return _factory
