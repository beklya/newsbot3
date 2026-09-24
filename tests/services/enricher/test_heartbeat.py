"""Tests for HeartbeatPublisher using fakeredis."""
from __future__ import annotations

import asyncio

import pytest

from src.infra.heartbeat import HeartbeatPublisher


@pytest.mark.asyncio
async def test_heartbeat_publishes_on_start(fake_redis):
    """start() immediately publishes one heartbeat."""
    snapshot = {"events_in": 5, "uptime_sec": 10}
    hb = HeartbeatPublisher(
        redis=fake_redis,
        stream="system:heartbeats",
        producer="enricher",
        interval_sec=60,
        snapshot_fn=lambda: snapshot,
    )
    hb.start()
    # Give the task a chance to publish.
    await asyncio.sleep(0.05)
    await hb.stop()

    # At least one heartbeat in the stream
    n = await fake_redis.xlen("system:heartbeats")
    assert n >= 1
    msgs = await fake_redis.xrange("system:heartbeats")
    _, fields = msgs[0]
    assert fields[b"service"].decode() == "enricher"
    assert fields[b"events_in"].decode() == "5"


@pytest.mark.asyncio
async def test_heartbeat_repeats_on_interval(fake_redis):
    """Multiple publishes happen at the configured interval."""
    snapshot = {"events_in": 0}
    hb = HeartbeatPublisher(
        redis=fake_redis,
        stream="system:heartbeats",
        producer="enricher",
        interval_sec=1,  # 1 sec for the test
        snapshot_fn=lambda: snapshot,
    )
    hb.start()
    # Wait ~2.5 sec — should see ~3 heartbeats (initial + 2 intervals)
    await asyncio.sleep(2.5)
    await hb.stop()

    n = await fake_redis.xlen("system:heartbeats")
    assert n >= 3, f"expected ≥3 heartbeats, got {n}"


@pytest.mark.asyncio
async def test_heartbeat_continues_after_snapshot_error(fake_redis, caplog):
    """If snapshot_fn raises one time, the loop continues."""
    import logging
    caplog.set_level(logging.WARNING)
    call_count = {"n": 0}

    def flaky_snapshot():
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("transient")
        return {"x": call_count["n"]}

    hb = HeartbeatPublisher(
        redis=fake_redis,
        stream="system:heartbeats",
        producer="enricher",
        interval_sec=1,
        snapshot_fn=flaky_snapshot,
    )
    hb.start()
    await asyncio.sleep(2.5)
    await hb.stop()

    # Even though snapshot #2 raised, snapshot #1 and #3 should succeed.
    n = await fake_redis.xlen("system:heartbeats")
    assert n >= 2  # at least initial + post-error recovery


@pytest.mark.asyncio
async def test_heartbeat_stop_is_idempotent(fake_redis):
    """stop() called twice does not raise."""
    hb = HeartbeatPublisher(
        redis=fake_redis,
        stream="system:heartbeats",
        producer="enricher",
        interval_sec=60,
        snapshot_fn=lambda: {},
    )
    hb.start()
    await asyncio.sleep(0.05)
    await hb.stop()
    await hb.stop()  # no-op


@pytest.mark.asyncio
async def test_heartbeat_double_start_is_safe(fake_redis, caplog):
    """start() called when already running logs a warning, doesn't create another task."""
    import logging
    caplog.set_level(logging.WARNING)
    hb = HeartbeatPublisher(
        redis=fake_redis,
        stream="system:heartbeats",
        producer="enricher",
        interval_sec=60,
        snapshot_fn=lambda: {},
    )
    hb.start()
    hb.start()
    await asyncio.sleep(0.05)
    await hb.stop()
    assert any("already running" in rec.message for rec in caplog.records)
