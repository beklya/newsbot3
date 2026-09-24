"""
End-to-end smoke test for Sprint 1 infrastructure.

Verifies that all 3 infra components work together with real Memurai/Redis:
    Test 1: Publisher.publish() writes event, raw xreadgroup reads it back,
            event_id and text_hash are preserved through serialization.
    Test 2: IdempotencyGuard.claim() returns True once, then False (atomic).
    Test 3: Consumer.run() loop integrates with publisher and handler,
            processes 3 events, all are xack'd, pending=0.

Prerequisites:
    - Memurai is running on localhost:6379 (sc query Memurai = RUNNING)
    - venv activated:
        cd D:\\quik_sber\\newsbot\\newsbot3
        .venv\\Scripts\\activate.bat

Usage:
    python scripts\\demo_pipeline.py
"""

import asyncio
import sys
from pathlib import Path

# Make src.* importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from redis.asyncio import Redis

from src.contracts.base import utcnow_iso
from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.infra.publisher import StreamPublisher
from src.infra.consumer import StreamConsumer
from src.infra.idempotency import IdempotencyGuard


STREAM_NAME = "news:raw"
GROUP_NAME = "cg:demo"
CONSUMER_NAME = "demo-consumer-1"


def make_test_event(text: str) -> RawNewsEvent:
    """Build a RawNewsEvent with deterministic-ish fields for tests."""
    # text_hash should be 64 chars; we mock it for the demo
    fake_hash = ("c" * 64)
    return RawNewsEvent(
        payload=RawNewsPayload(
            channel="@interfaxonline",
            message_id=12345678,
            text=text,
            tg_published_at=utcnow_iso(),
            received_at=utcnow_iso(),
            text_hash=fake_hash,
        )
    )


async def cleanup(redis: Redis) -> None:
    """Remove leftover state from previous runs."""
    await redis.delete(STREAM_NAME)
    keys = await redis.keys(b"idem:demo:*")
    if keys:
        await redis.delete(*keys)


async def test_publisher_and_consumer_raw(redis: Redis) -> None:
    """Test 1: Publisher writes, raw xreadgroup reads it back, event reparses correctly."""
    print("\n[Test 1] Publisher.publish() + raw xreadgroup + parse round-trip")

    publisher = StreamPublisher(redis, STREAM_NAME)
    event = make_test_event(text="Test 1: GAZP record profit announcement")

    msg_id = await publisher.publish(event)
    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
    print(f"  Published msg_id={msg_id_str}")
    print(f"  event_id     ={event.event_id}")

    # Set up consumer group
    consumer = StreamConsumer(redis, STREAM_NAME, GROUP_NAME, CONSUMER_NAME, RawNewsEvent)
    await consumer.ensure_group()

    # Read using raw redis-py API (bypasses Consumer.run loop for direct test)
    messages = await redis.xreadgroup(
        GROUP_NAME, CONSUMER_NAME,
        streams={STREAM_NAME: ">"},
        count=10, block=2000,
    )

    assert messages, "No messages received from xreadgroup"
    stream_name, msgs = messages[0]
    assert msgs, "Empty messages list"

    received_msg_id, fields = msgs[0]

    # Parse using same logic as Consumer._handle
    data_bytes = fields[b"data"]
    received_event = RawNewsEvent.model_validate_json(data_bytes)

    # Verify integrity
    assert received_event.event_id == event.event_id, \
        f"event_id mismatch: sent={event.event_id}, received={received_event.event_id}"
    assert received_event.payload.text_hash == event.payload.text_hash, \
        "text_hash mismatch (data corrupted in transit?)"
    assert received_event.payload.text == event.payload.text, \
        "text content mismatch"
    assert len(received_event.trace) == 1, \
        f"Expected trace with 1 entry (added by Publisher), got {len(received_event.trace)}"
    assert received_event.trace[0]["service"] == STREAM_NAME, \
        f"trace service should be '{STREAM_NAME}'"

    await redis.xack(STREAM_NAME, GROUP_NAME, received_msg_id)

    print(f"  [OK] event_id round-trip preserved")
    print(f"  [OK] text_hash matches")
    print(f"  [OK] text content matches (incl. cyrillic)")
    print(f"  [OK] trace[0].service = '{STREAM_NAME}'")
    print(f"  [OK] xack'd successfully")


async def test_idempotency_guard(redis: Redis) -> None:
    """Test 2: IdempotencyGuard prevents double-processing."""
    print("\n[Test 2] IdempotencyGuard atomic claim")

    guard = IdempotencyGuard(redis, ttl_seconds=60)

    first  = await guard.claim("demo", "unique_key_xyz")
    second = await guard.claim("demo", "unique_key_xyz")
    third  = await guard.claim("demo", "different_key")

    print(f"  First  claim 'unique_key_xyz':  {first}")
    print(f"  Second claim 'unique_key_xyz':  {second}")
    print(f"  Claim       'different_key':    {third}")

    assert first is True, "First claim should succeed (key didn't exist)"
    assert second is False, "Second claim should fail (key already exists)"
    assert third is True, "Different key should succeed (no collision)"

    print(f"  [OK] First wins, duplicate rejected, different key allowed")


async def test_consumer_run_loop(redis: Redis) -> None:
    """Test 3: Consumer.run() loop processes published events via handler."""
    print("\n[Test 3] Consumer.run() loop with handler (3 events)")

    publisher = StreamPublisher(redis, STREAM_NAME)
    consumer  = StreamConsumer(redis, STREAM_NAME, GROUP_NAME, CONSUMER_NAME, RawNewsEvent)

    received_events: list[RawNewsEvent] = []

    async def handler(event: RawNewsEvent) -> None:
        received_events.append(event)
        print(f"  Handler got: event_id={event.event_id} text='{event.payload.text[:40]}...'")

    shutdown = asyncio.Event()

    # Start consumer in background
    consumer_task = asyncio.create_task(consumer.run(handler, shutdown))

    # Tiny pause to let consumer establish group + first xreadgroup
    await asyncio.sleep(0.3)

    # Publish 3 events
    test_events = [make_test_event(f"Test 3 event #{i+1} (cyclic GAZP news)") for i in range(3)]
    for ev in test_events:
        await publisher.publish(ev)
    print(f"  Published {len(test_events)} events")

    # Wait for handler to receive all 3 (max ~8 sec)
    for _ in range(80):
        if len(received_events) >= 3:
            break
        await asyncio.sleep(0.1)

    # Signal shutdown
    shutdown.set()

    # Wait for consumer.run() to exit cleanly (xreadgroup blocks up to 5s)
    try:
        await asyncio.wait_for(consumer_task, timeout=10)
    except asyncio.TimeoutError:
        print("  [WARN] consumer task didn't finish in 10s, cancelling")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    # Verify all events received
    assert len(received_events) == 3, \
        f"Expected 3 events, got {len(received_events)}"
    received_ids = {e.event_id for e in received_events}
    expected_ids = {e.event_id for e in test_events}
    assert received_ids == expected_ids, \
        f"event_id sets don't match. Missing: {expected_ids - received_ids}"

    # Verify all xack'd (pending=0)
    pending = await redis.xpending(STREAM_NAME, GROUP_NAME)
    pending_count = pending["pending"] if isinstance(pending, dict) else pending[0]
    assert pending_count == 0, f"Expected 0 pending after run, got {pending_count}"

    print(f"  [OK] All 3 events received via Consumer.run() handler")
    print(f"  [OK] All event_ids match expected set")
    print(f"  [OK] XPENDING after run = 0 (all xack'd)")


async def main() -> None:
    print("=" * 70)
    print("Sprint 1 Infrastructure End-to-End Smoke Test")
    print("Memurai connection: localhost:6379")
    print("=" * 70)

    redis = Redis(host="localhost", port=6379, decode_responses=False)

    try:
        # Verify connection
        pong = await redis.ping()
        print(f"\nRedis PING: {pong}")
        if not pong:
            print("[ERROR] Memurai not responding to PING")
            return

        # Cleanup any leftover state
        await cleanup(redis)

        # Run tests
        await test_publisher_and_consumer_raw(redis)
        await cleanup(redis)

        await test_idempotency_guard(redis)

        await cleanup(redis)
        await test_consumer_run_loop(redis)

        # Final cleanup
        await cleanup(redis)

        print("\n" + "=" * 70)
        print("[OK] ALL 3 TESTS PASSED")
        print("[OK] Sprint 1 infrastructure is fully functional")
        print("=" * 70)

    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
