"""Тесты QuikFeeder (Sprint 5.8)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import fakeredis.aioredis

from src.services.quik_feed.config import QuikFeedSettings
from src.services.quik_feed.feeder import QuikFeeder
from src.services.quik_feed.metrics import QuikFeedMetrics
from src.services.quik_feed.readers import CSVTailReader


def _make_settings(tmp_path: Path, csv_path: Path) -> QuikFeedSettings:
    """Construct settings for tests without .env."""
    return QuikFeedSettings(
        redis_url="redis://localhost:6379",
        candles_stream="candles:1m",
        quik_feed_source_path=csv_path,
        quik_feed_poll_sec=1,
        bootstrap_mode="all",
        accepted_tickers=["SBER", "GAZP", "Si", "SI", "MX", "MIX"],
    )


def _write_csv(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.mark.asyncio
async def test_feeder_publishes_new_bars_to_redis(tmp_path: Path):
    csv_path = tmp_path / "candles.csv"
    _write_csv(csv_path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000",
        "GAZP,2026-05-29 14:38:00,150.5,151.0,150.2,150.8,8000",
    ])
    settings = _make_settings(tmp_path, csv_path)
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    reader = CSVTailReader(csv_path, bootstrap_mode="all")
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(redis=redis, reader=reader, settings=settings, metrics=metrics)

    n = await feeder.poll_once()
    assert n == 2

    # Verify they're in the stream
    entries = await redis.xrange("candles:1m")
    assert len(entries) == 2
    tickers = {e[1][b"ticker"].decode() for e in entries}
    assert tickers == {"SBER", "GAZP"}

    await redis.aclose()


@pytest.mark.asyncio
async def test_feeder_skips_off_whitelist_tickers(tmp_path: Path):
    csv_path = tmp_path / "candles.csv"
    _write_csv(csv_path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000",
        "UNKNOWN_TKR,2026-05-29 14:37:00,100,101,99,100.5,500",
    ])
    settings = QuikFeedSettings(
        quik_feed_source_path=csv_path,
        accepted_tickers=["SBER"],
        bootstrap_mode="all",
    )
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    reader = CSVTailReader(csv_path, bootstrap_mode="all")
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(redis=redis, reader=reader, settings=settings, metrics=metrics)

    await feeder.poll_once()
    entries = await redis.xrange("candles:1m")
    assert len(entries) == 1
    assert entries[0][1][b"ticker"] == b"SBER"
    snap = metrics.snapshot()
    assert snap.get("bars_published") == 1
    assert snap.get("bars_skipped_off_whitelist") == 1
    await redis.aclose()


@pytest.mark.asyncio
async def test_feeder_normalizes_legacy_ticker_to_canonical(tmp_path: Path):
    """Si → SI (canonical via instruments registry)."""
    csv_path = tmp_path / "candles.csv"
    _write_csv(csv_path, [
        "ticker,ts,open,high,low,close,volume",
        "Si,2026-05-29 14:37:00,80.5,80.7,80.3,80.6,1500",
    ])
    settings = QuikFeedSettings(
        quik_feed_source_path=csv_path,
        accepted_tickers=["SI"],  # only canonical
        bootstrap_mode="all",
    )
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    reader = CSVTailReader(csv_path, bootstrap_mode="all")
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(redis=redis, reader=reader, settings=settings, metrics=metrics)

    await feeder.poll_once()
    entries = await redis.xrange("candles:1m")
    assert len(entries) == 1
    # Published as canonical (SI), not legacy (Si)
    assert entries[0][1][b"ticker"] == b"SI"
    await redis.aclose()


@pytest.mark.asyncio
async def test_feeder_bootstrap_from_redis_resumes_state(tmp_path: Path):
    csv_path = tmp_path / "candles.csv"
    # свежие ts в пределах окна сверки screener'а (reconcile_lookback_days)
    _msk = timezone(timedelta(hours=3))
    base = datetime.now(_msk).replace(tzinfo=None, second=0, microsecond=0) - timedelta(hours=2)
    t0, t1 = base, base + timedelta(minutes=1)
    _write_csv(csv_path, [
        "ticker,ts,open,high,low,close,volume",
        f"SBER,{t0.strftime('%Y-%m-%d %H:%M:%S')},295.0,295.5,294.8,295.3,10000",
        f"SBER,{t1.strftime('%Y-%m-%d %H:%M:%S')},295.3,295.8,295.0,295.6,9500",
    ])
    settings = QuikFeedSettings(
        quik_feed_source_path=csv_path,
        accepted_tickers=["SBER"],
        bootstrap_mode="all",
    )
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    # Pre-existing entry в stream (имитация предыдущего runa)
    await redis.xadd("candles:1m", {
        "ticker": "SBER", "ts": t0.isoformat(timespec="seconds"),
        "open": "295.0", "high": "295.5", "low": "294.8", "close": "295.3", "volume": "10000",
    })

    reader = CSVTailReader(csv_path, bootstrap_mode="all")
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(redis=redis, reader=reader, settings=settings, metrics=metrics)
    await feeder.bootstrap_from_redis()
    # last_ts должен быть установлен из существующих entries
    assert feeder._last_ts.get("SBER") == t0

    # При очередном poll первая строка (14:37) пропускается, публикуется только 14:38
    n = await feeder.poll_once()
    assert n == 1
    entries = await redis.xrange("candles:1m")
    # 1 pre-existing + 1 свежий
    assert len(entries) == 2

    await redis.aclose()


@pytest.mark.asyncio
async def test_feeder_handles_missing_source_file(tmp_path: Path):
    csv_path = tmp_path / "does_not_exist.csv"
    settings = QuikFeedSettings(quik_feed_source_path=csv_path, bootstrap_mode="all")
    redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
    reader = CSVTailReader(csv_path, bootstrap_mode="all")
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(redis=redis, reader=reader, settings=settings, metrics=metrics)

    n = await feeder.poll_once()
    assert n == 0
    snap = metrics.snapshot()
    assert snap.get("polls_source_missing") == 1
    await redis.aclose()
