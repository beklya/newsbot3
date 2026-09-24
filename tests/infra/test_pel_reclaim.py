"""Tests for PelReclaimer (Sprint 6.2 ext).

Cover:
  - happy path: a message stuck in PEL gets reclaimed and handled successfully
  - retry path: handler keeps failing → message stays in PEL across ticks
  - terminal DLQ: after max_deliveries, message routed to DLQ + xack'd
  - shutdown event stops the loop promptly
  - snapshot() exposes correct counters
"""
from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.infra.consumer import StreamConsumer
from src.infra.pel_reclaim import PelReclaimer
from src.infra.publisher import StreamPublisher


def _make_event(message_id: int = 1, text: str = "hello") -> RawNewsEvent:
    return RawNewsEvent(payload=RawNewsPayload(
        channel="@test",
        message_id=message_id,
        text=text,
        tg_published_at="2026-06-09T00:00:00.000+00:00",
        received_at="2026-06-09T00:00:00.000+00:00",
        text_hash="a" * 64,
        has_media=False,
        is_reply=False,
        is_forward=False,
    ))


async def _seed_stuck_message(redis, stream, group, consumer_name, event):
    """Publish an event to the stream and ensure it's in `consumer_name`'s PEL
    (delivered to a consumer that never acked)."""
    pub = StreamPublisher(redis=redis, stream=stream)
    await pub.publish(event)
    # Ensure group exists
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        pass
    # Deliver the message to consumer_name without acking → PEL entry
    delivered = await redis.xreadgroup(
        group, consumer_name,
        streams={stream: ">"}, count=10, block=100,
    )
    assert delivered, "expected at least one delivered message"


@pytest.mark.asyncio
async def test_reclaim_happy_path(monkeypatch):
    """Stuck PEL message gets reclaimed and successfully handled."""
    redis = fakeredis.aioredis.FakeRedis()
    stream = "news:raw"
    group = "test-group"
    stuck_consumer = "stuck-1"
    reclaimer_consumer = "reclaimer-1"

    await _seed_stuck_message(redis, stream, group, stuck_consumer, _make_event(1))

    handled: list[RawNewsEvent] = []

    async def handler(ev):
        handled.append(ev)

    reclaimer = PelReclaimer(
        redis=redis,
        stream=stream,
        group=group,
        consumer_name=reclaimer_consumer,
        event_type=RawNewsEvent,
        handler=handler,
        min_idle_ms=0,  # accept everything immediately for tests
        poll_interval_sec=1,
        max_deliveries=10,
    )
    shutdown = asyncio.Event()

    async def stopper():
        # Let one tick run, then stop.
        await asyncio.sleep(0.5)
        shutdown.set()

    await asyncio.gather(reclaimer.run(shutdown), stopper())

    assert len(handled) == 1, f"expected reclaim+handle, got handled={len(handled)}"
    assert reclaimer.n_reclaimed == 1
    assert reclaimer.n_recovered_ok == 1
    assert reclaimer.n_recovered_fail == 0
    assert reclaimer.n_dlq_terminal == 0
    await redis.aclose()


@pytest.mark.asyncio
async def test_reclaim_persistent_failure_then_dlq():
    """Handler keeps raising → after max_deliveries, terminal DLQ + ack."""
    redis = fakeredis.aioredis.FakeRedis()
    stream = "news:raw"
    group = "test-group"
    stuck_consumer = "stuck-1"

    await _seed_stuck_message(redis, stream, group, stuck_consumer, _make_event(2))

    async def always_fail(ev):
        raise ValueError("simulated invalid_json from LLM")

    dlq_calls: list[tuple] = []

    async def dlq_handler(msg_id, fields, reason):
        dlq_calls.append((msg_id, reason))

    # max_deliveries=2 → initial delivery (1) + 1 reclaim attempt = 2 total,
    # next reclaim hits the limit and goes to DLQ.
    reclaimer = PelReclaimer(
        redis=redis,
        stream=stream,
        group=group,
        consumer_name="reclaimer-1",
        event_type=RawNewsEvent,
        handler=always_fail,
        min_idle_ms=0,
        poll_interval_sec=0.1,  # short ticks to drive multiple attempts
        max_deliveries=2,
        dlq_handler=dlq_handler,
    )
    shutdown = asyncio.Event()

    async def stopper():
        # Let several ticks run.
        await asyncio.sleep(1.2)
        shutdown.set()

    await asyncio.gather(reclaimer.run(shutdown), stopper())

    assert reclaimer.n_dlq_terminal >= 1, (
        f"expected terminal DLQ to fire, dlq_terminal={reclaimer.n_dlq_terminal} "
        f"ok={reclaimer.n_recovered_ok} fail={reclaimer.n_recovered_fail}"
    )
    assert dlq_calls, "expected dlq_handler to be called"
    assert "max_deliveries_exceeded" in dlq_calls[-1][1]
    # The terminal message should no longer be in PEL (ack'd).
    pending = await redis.xpending(stream, group)
    # xpending returns a dict in fakeredis with various shapes; we just want 0 pending
    if isinstance(pending, dict):
        pending_count = pending.get(b"pending") or pending.get("pending") or 0
    else:
        pending_count = pending[0] if pending else 0
    assert pending_count == 0, f"expected 0 pending after terminal DLQ, got {pending_count}"
    await redis.aclose()


@pytest.mark.asyncio
async def test_reclaim_shutdown_stops_loop():
    """Setting shutdown causes the reclaimer to exit promptly."""
    redis = fakeredis.aioredis.FakeRedis()
    reclaimer = PelReclaimer(
        redis=redis,
        stream="empty-stream",
        group="g",
        consumer_name="reclaimer-1",
        event_type=RawNewsEvent,
        handler=lambda ev: asyncio.sleep(0),  # type: ignore[arg-type]
        min_idle_ms=0,
        poll_interval_sec=10,  # long
        max_deliveries=1,
    )
    shutdown = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.2)
        shutdown.set()

    # Should return within the stopper window despite 10s poll cadence.
    await asyncio.wait_for(
        asyncio.gather(reclaimer.run(shutdown), stop_soon()),
        timeout=2.0,
    )
    await redis.aclose()


def test_snapshot_shape():
    """snapshot() returns the expected counter keys."""
    redis = fakeredis.aioredis.FakeRedis()
    reclaimer = PelReclaimer(
        redis=redis,
        stream="s",
        group="g",
        consumer_name="c",
        event_type=RawNewsEvent,
        handler=lambda ev: asyncio.sleep(0),  # type: ignore[arg-type]
    )
    snap = reclaimer.snapshot()
    assert set(snap.keys()) == {
        "pel_reclaim_n", "pel_reclaim_ok",
        "pel_reclaim_fail", "pel_reclaim_dlq",
    }
    assert all(isinstance(v, int) for v in snap.values())


@pytest.mark.asyncio
async def test_reclaim_alongside_stream_consumer():
    """End-to-end: a healthy StreamConsumer runs in parallel with a reclaimer
    over the same group. The reclaimer rescues a message stuck in a dead
    consumer's PEL while the main loop continues servicing fresh messages."""
    redis = fakeredis.aioredis.FakeRedis()
    stream = "news:raw"
    group = "enricher"

    # Seed a stuck message (delivered to dead-consumer, never acked)
    await _seed_stuck_message(redis, stream, group, "dead-1", _make_event(99))

    pub = StreamPublisher(redis=redis, stream=stream)
    # Publish a fresh message that the live consumer will pick up
    await pub.publish(_make_event(100))

    received_by_main: list[RawNewsEvent] = []
    received_by_reclaimer: list[RawNewsEvent] = []

    async def main_handler(ev):
        received_by_main.append(ev)

    async def reclaim_handler(ev):
        received_by_reclaimer.append(ev)

    consumer = StreamConsumer(
        redis=redis,
        stream=stream,
        group=group,
        consumer_name="live-1",
        event_type=RawNewsEvent,
        block_ms=50,
    )
    reclaimer = PelReclaimer(
        redis=redis,
        stream=stream,
        group=group,
        consumer_name="reclaimer-1",
        event_type=RawNewsEvent,
        handler=reclaim_handler,
        min_idle_ms=0,
        poll_interval_sec=0.2,
        max_deliveries=10,
    )
    shutdown = asyncio.Event()

    async def stopper():
        # Wait until both have processed at least one message OR timeout
        for _ in range(50):
            if received_by_main and received_by_reclaimer:
                break
            await asyncio.sleep(0.1)
        shutdown.set()

    await asyncio.gather(
        consumer.run(main_handler, shutdown),
        reclaimer.run(shutdown),
        stopper(),
    )

    assert received_by_main, "live consumer should have processed the fresh message"
    assert received_by_reclaimer, "reclaimer should have rescued the stuck message"
    # No overlap — they handled different messages
    main_ids = {e.payload.message_id for e in received_by_main}
    reclaim_ids = {e.payload.message_id for e in received_by_reclaimer}
    assert main_ids.isdisjoint(reclaim_ids), (
        f"overlap between main and reclaimer: main={main_ids} reclaim={reclaim_ids}"
    )
    await redis.aclose()
