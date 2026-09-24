"""Tests for StreamConsumer — PEL recovery and new-message handling."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.infra.consumer import StreamConsumer
from src.infra.publisher import StreamPublisher


STREAM = "test:raw"
GROUP = "test_group"
CONSUMER_NAME = "test_consumer"


def _make_raw_event(text: str) -> RawNewsEvent:
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    payload = RawNewsPayload(
        channel="@test",
        message_id=1,
        text=text,
        tg_published_at=now,
        received_at=now,
        text_hash=hashlib.sha256(text.encode()).hexdigest(),
    )
    return RawNewsEvent(payload=payload)


@pytest_asyncio.fixture
async def publisher_consumer(fake_redis):
    """Publisher writes to STREAM, Consumer reads from same with group.

    block_ms=100 — короткий timeout, чтобы тесты не висели на пустых стримах.
    """
    pub = StreamPublisher(fake_redis, stream=STREAM, maxlen=1_000)
    consumer = StreamConsumer(
        redis=fake_redis,
        stream=STREAM,
        group=GROUP,
        consumer_name=CONSUMER_NAME,
        event_type=RawNewsEvent,
        block_ms=100,
    )
    await consumer.ensure_group()
    return pub, consumer, fake_redis


@pytest.mark.asyncio
async def test_consumer_reads_new_messages(publisher_consumer):
    """Happy path: 3 messages published, all consumed and ack'ed."""
    pub, consumer, redis = publisher_consumer

    # Publish 3 messages
    events = [_make_raw_event(f"news {i}") for i in range(3)]
    for e in events:
        await pub.publish(e)

    received: list[RawNewsEvent] = []

    async def handler(ev):
        received.append(ev)

    shutdown = asyncio.Event()
    task = asyncio.create_task(consumer.run(handler, shutdown))
    # Give consumer a moment to read
    await asyncio.sleep(0.3)
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    assert len(received) == 3
    # All ack'ed → 0 pending
    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 0


@pytest.mark.asyncio
async def test_consumer_keeps_failed_in_pending(publisher_consumer):
    """If handler raises, message stays in PEL (no xack)."""
    pub, consumer, redis = publisher_consumer

    e1 = _make_raw_event("news 1")
    e2 = _make_raw_event("news 2")
    await pub.publish(e1)
    await pub.publish(e2)

    call_count = {"n": 0}

    async def flaky_handler(ev):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated failure")
        # Second one succeeds.

    shutdown = asyncio.Event()
    task = asyncio.create_task(consumer.run(flaky_handler, shutdown))
    await asyncio.sleep(0.3)
    shutdown.set()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()

    # Handler called twice — once failed, once succeeded
    assert call_count["n"] == 2
    # One message remains pending (the one that failed)
    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 1


@pytest.mark.asyncio
async def test_consumer_recovers_pel_on_restart(publisher_consumer):
    """KEY TEST: after restart, consumer picks up its own PEL entries.

    Scenario:
      1. Consumer-1 fails handling msg1 (raises).
      2. Process "restarts" — new Consumer instance with SAME name.
      3. New instance should read msg1 from PEL on start.
    """
    pub, consumer1, redis = publisher_consumer

    e1 = _make_raw_event("will fail first time")
    await pub.publish(e1)

    fail_first = {"failed": False}

    async def fail_then_succeed(ev):
        if not fail_first["failed"]:
            fail_first["failed"] = True
            raise RuntimeError("first attempt fails")
        # Recovered run

    # First run: failure
    shutdown1 = asyncio.Event()
    task1 = asyncio.create_task(consumer1.run(fail_then_succeed, shutdown1))
    await asyncio.sleep(0.3)
    shutdown1.set()
    try:
        await asyncio.wait_for(task1, timeout=2.0)
    except asyncio.TimeoutError:
        task1.cancel()

    # Verify msg is in PEL
    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 1

    # --- Simulate restart: new Consumer with SAME consumer_name ---
    consumer2 = StreamConsumer(
        redis=redis,
        stream=STREAM,
        group=GROUP,
        consumer_name=CONSUMER_NAME,  # ← same name!
        event_type=RawNewsEvent,
        block_ms=100,
    )
    received_on_recovery: list[RawNewsEvent] = []

    async def recovery_handler(ev):
        received_on_recovery.append(ev)

    shutdown2 = asyncio.Event()
    task2 = asyncio.create_task(consumer2.run(recovery_handler, shutdown2))
    await asyncio.sleep(0.3)
    shutdown2.set()
    try:
        await asyncio.wait_for(task2, timeout=2.0)
    except asyncio.TimeoutError:
        task2.cancel()

    # The PEL message should have been re-delivered and processed
    assert len(received_on_recovery) == 1
    assert received_on_recovery[0].event_id == e1.event_id

    # PEL drained
    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 0


@pytest.mark.asyncio
async def test_consumer_processes_pel_then_new_messages(publisher_consumer):
    """Mix scenario: PEL has 1 msg, then 2 new msgs published — all 3 processed."""
    pub, consumer1, redis = publisher_consumer

    # Setup PEL: publish + fail
    e0 = _make_raw_event("pending one")
    await pub.publish(e0)
    fail_once = {"done": False}

    async def fail_handler(ev):
        if not fail_once["done"]:
            fail_once["done"] = True
            raise RuntimeError("fail")

    sh1 = asyncio.Event()
    t1 = asyncio.create_task(consumer1.run(fail_handler, sh1))
    await asyncio.sleep(0.3)
    sh1.set()
    try:
        await asyncio.wait_for(t1, timeout=2.0)
    except asyncio.TimeoutError:
        t1.cancel()

    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 1

    # Now publish 2 new messages and restart consumer
    e1 = _make_raw_event("new one")
    e2 = _make_raw_event("new two")
    await pub.publish(e1)
    await pub.publish(e2)

    received: list[RawNewsEvent] = []

    async def ok_handler(ev):
        received.append(ev)

    consumer2 = StreamConsumer(
        redis=redis, stream=STREAM, group=GROUP,
        consumer_name=CONSUMER_NAME, event_type=RawNewsEvent,
        block_ms=100,
    )
    sh2 = asyncio.Event()
    t2 = asyncio.create_task(consumer2.run(ok_handler, sh2))
    await asyncio.sleep(0.5)
    sh2.set()
    try:
        await asyncio.wait_for(t2, timeout=2.0)
    except asyncio.TimeoutError:
        t2.cancel()

    # All 3 events processed (1 from PEL + 2 new)
    received_ids = {e.event_id for e in received}
    assert e0.event_id in received_ids
    assert e1.event_id in received_ids
    assert e2.event_id in received_ids
    assert len(received) == 3

    # No more pending
    summary = await redis.xpending(STREAM, GROUP)
    assert summary.get("pending", 0) == 0
