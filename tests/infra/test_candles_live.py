"""Тесты live-update API в CandleCache (Sprint 5.8).

Покрытие:
  - add_bar: append > last_ts (fast path)
  - add_bar: overwrite на duplicate ts (idempotent)
  - add_bar: out-of-order insert + re-sort
  - add_bar: cold start (ticker без historical CSV)
  - add_bar: legacy ticker normalize (Si → SI)
  - last_bar_time getter
  - subscribe_redis_stream: consumes от candles:1m stream
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import pytest
import fakeredis.aioredis

from src.infra.candles import CandleCache


@pytest.fixture
def empty_cache(tmp_path: Path) -> CandleCache:
    return CandleCache(tmp_path)


# --- add_bar ---

def test_add_bar_cold_start_creates_dataframe(empty_cache: CandleCache):
    ts = datetime(2026, 5, 29, 14, 37)
    added = empty_cache.add_bar("SBER", ts, 295.0, 295.5, 294.8, 295.3, 10000)
    assert added is True
    df = empty_cache.get("SBER")
    assert df is not None
    assert len(df) == 1


def test_add_bar_appends_fast_path(empty_cache: CandleCache):
    empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 37),
                        295.0, 295.5, 294.8, 295.3, 10000)
    added = empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 38),
                                295.3, 295.8, 295.0, 295.6, 9500)
    assert added is True
    df = empty_cache.get("SBER")
    assert len(df) == 2
    assert df["close"].iloc[-1] == 295.6


def test_add_bar_overwrite_on_duplicate_ts(empty_cache: CandleCache):
    ts = datetime(2026, 5, 29, 14, 37)
    empty_cache.add_bar("SBER", ts, 295.0, 295.5, 294.8, 295.3, 10000)
    added = empty_cache.add_bar("SBER", ts, 295.1, 295.5, 294.8, 295.4, 11000)
    assert added is False  # not a new bar
    df = empty_cache.get("SBER")
    assert len(df) == 1
    # latest write wins
    assert df["close"].iloc[-1] == 295.4


def test_add_bar_out_of_order_sorts(empty_cache: CandleCache):
    empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 38),
                        295.3, 295.8, 295.0, 295.6, 9500)
    empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 37),
                        295.0, 295.5, 294.8, 295.3, 10000)
    df = empty_cache.get("SBER")
    assert len(df) == 2
    # sorted by ts asc
    assert df.index[0] < df.index[1]
    assert df["close"].iloc[0] == 295.3
    assert df["close"].iloc[1] == 295.6


def test_add_bar_normalizes_legacy_ticker(empty_cache: CandleCache):
    """Si → SI via instruments registry."""
    empty_cache.add_bar("Si", datetime(2026, 5, 29, 14, 37),
                        80.5, 80.7, 80.3, 80.6, 1500)
    assert empty_cache.has("Si")
    assert empty_cache.has("SI")  # canonical access works
    # Both lookups return the same DF (canonical key in dict)
    assert empty_cache.get("Si") is empty_cache.get("SI")


# --- last_bar_time ---

def test_last_bar_time_returns_latest_ts(empty_cache: CandleCache):
    assert empty_cache.last_bar_time("SBER") is None
    empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 37),
                        295.0, 295.5, 294.8, 295.3, 0)
    empty_cache.add_bar("SBER", datetime(2026, 5, 29, 14, 38),
                        295.3, 295.8, 295.0, 295.6, 0)
    ts = empty_cache.last_bar_time("SBER")
    assert ts is not None
    assert ts.to_pydatetime() == datetime(2026, 5, 29, 14, 38)


# --- subscribe_redis_stream ---

@pytest.mark.asyncio
async def test_subscribe_redis_stream_consumes_bars(empty_cache: CandleCache):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    shutdown = asyncio.Event()

    # Запускаем subscriber в background
    task = asyncio.create_task(
        empty_cache.subscribe_redis_stream(redis, "candles:1m", shutdown, block_ms=100),
    )

    # publish bars (после старта subscriber, чтобы $ cursor поймал их)
    await asyncio.sleep(0.05)
    await redis.xadd("candles:1m", {
        "ticker": "SBER",
        "ts": "2026-05-29T14:37:00",
        "open": "295.0", "high": "295.5", "low": "294.8",
        "close": "295.3", "volume": "10000",
    })
    await redis.xadd("candles:1m", {
        "ticker": "GAZP",
        "ts": "2026-05-29T14:37:00",
        "open": "150.0", "high": "150.5", "low": "149.8",
        "close": "150.3", "volume": "5000",
    })

    # Даём subscriber'у время прочитать
    await asyncio.sleep(0.3)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2)

    assert empty_cache.has("SBER")
    assert empty_cache.has("GAZP")
    sber = empty_cache.get("SBER")
    assert sber["close"].iloc[-1] == pytest.approx(295.3)

    await redis.aclose()


@pytest.mark.asyncio
async def test_subscribe_redis_stream_tolerates_malformed_messages(empty_cache: CandleCache):
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    shutdown = asyncio.Event()

    task = asyncio.create_task(
        empty_cache.subscribe_redis_stream(redis, "candles:1m", shutdown, block_ms=100),
    )
    await asyncio.sleep(0.05)

    # Malformed: missing fields
    await redis.xadd("candles:1m", {"ticker": "SBER", "ts": "2026-05-29T14:37:00"})
    # Malformed: bad ts
    await redis.xadd("candles:1m", {
        "ticker": "SBER", "ts": "not-a-date",
        "open": "1", "high": "1", "low": "1", "close": "1", "volume": "0",
    })
    # Valid
    await redis.xadd("candles:1m", {
        "ticker": "SBER", "ts": "2026-05-29T14:37:00",
        "open": "295.0", "high": "295.5", "low": "294.8",
        "close": "295.3", "volume": "0",
    })

    await asyncio.sleep(0.3)
    shutdown.set()
    await asyncio.wait_for(task, timeout=2)

    assert empty_cache.has("SBER")
    # Только 1 valid bar (malformed skipped)
    assert len(empty_cache.get("SBER")) == 1

    await redis.aclose()
