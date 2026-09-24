"""Sprint 6.2 — Tests for redis_retry helpers + StreamConsumer reconnect path.

Cover:
  - is_connection_error classification matrix
  - ReconnectBackoff schedule (1→2→4→8→8…), reset behavior
  - reset_pool best-effort semantics
  - StreamConsumer.run() survives a transient ConnectionError on xreadgroup
    and resumes processing once the underlying client recovers
"""
from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.exceptions import ResponseError

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.infra.consumer import StreamConsumer
from src.infra.publisher import StreamPublisher
from src.infra.redis_retry import (
    ReconnectBackoff,
    is_connection_error,
    reset_pool,
)


# -----------------------------------------------------------------------------
# is_connection_error

@pytest.mark.parametrize("exc", [
    RedisConnectionError("dead"),
    RedisTimeoutError("timeout"),
    ConnectionResetError("WinError 64"),
    ConnectionRefusedError("WinError 1225"),
    ConnectionAbortedError("aborted"),
    BrokenPipeError("pipe"),
    asyncio.TimeoutError(),
    OSError(22, "WinError 22"),
])
def test_is_connection_error_true(exc):
    assert is_connection_error(exc) is True


@pytest.mark.parametrize("exc", [
    ValueError("not a connection issue"),
    KeyError("missing"),
    ResponseError("WRONGTYPE"),
    RuntimeError("application bug"),
])
def test_is_connection_error_false(exc):
    assert is_connection_error(exc) is False


# -----------------------------------------------------------------------------
# ReconnectBackoff

@pytest.mark.asyncio
async def test_backoff_progression(monkeypatch):
    """Schedule advances 1→2→4→8→8 and resets to 1 on reset()."""
    sleeps: list[float] = []

    async def _capture(coro_or_event, timeout=None):
        sleeps.append(timeout)
        # Close the inner coroutine to avoid "coroutine never awaited" warnings
        if hasattr(coro_or_event, "close"):
            coro_or_event.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(asyncio, "wait_for", _capture)
    shutdown = asyncio.Event()
    bo = ReconnectBackoff(initial_sec=1.0, max_sec=8.0, multiplier=2.0)

    for expected in [1.0, 2.0, 4.0, 8.0, 8.0]:
        await bo.sleep(shutdown)
        assert sleeps[-1] == expected

    bo.reset()
    await bo.sleep(shutdown)
    assert sleeps[-1] == 1.0


@pytest.mark.asyncio
async def test_backoff_attempts_counter():
    bo = ReconnectBackoff(initial_sec=0.001, max_sec=0.001)
    assert bo.attempts == 0
    shutdown = asyncio.Event()
    await bo.sleep(shutdown)
    await bo.sleep(shutdown)
    assert bo.attempts == 2
    bo.reset()
    assert bo.attempts == 0


# -----------------------------------------------------------------------------
# reset_pool — best-effort, never raises

@pytest.mark.asyncio
async def test_reset_pool_on_fakeredis_is_noop():
    r = fakeredis.aioredis.FakeRedis()
    await reset_pool(r, where="test")  # should not raise
    # Connection still usable afterwards
    await r.set("x", "1")
    assert await r.get("x") == b"1"
    await r.aclose()


@pytest.mark.asyncio
async def test_reset_pool_handles_missing_attr():
    """If we somehow receive a Redis-like object without connection_pool,
    reset_pool just logs and returns."""
    class FakeNoPool:
        pass
    await reset_pool(FakeNoPool(), where="missing-attr")  # no raise


# -----------------------------------------------------------------------------
# StreamConsumer — resilience under transient ConnectionError

class FlakyRedis:
    """Wraps fakeredis and injects N ConnectionErrors on the first xreadgroup
    calls. Counts pool.disconnect() invocations."""

    def __init__(self, inner, fail_count: int = 2):
        self._inner = inner
        self._fail_count = fail_count
        self._calls = 0
        self.pool_resets = 0
        # Stub connection_pool attribute so reset_pool() can disconnect.
        class _Pool:
            async def disconnect(_self, inuse_connections: bool = True):
                self.pool_resets += 1
        self.connection_pool = _Pool()

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def xreadgroup(self, *args, **kwargs):
        self._calls += 1
        if self._calls <= self._fail_count:
            raise RedisConnectionError(
                f"Error 22 connecting to 127.0.0.1:6380 (injected #{self._calls})"
            )
        return await self._inner.xreadgroup(*args, **kwargs)


@pytest.mark.asyncio
async def test_consumer_recovers_from_connection_error(monkeypatch):
    """The main loop survives 2 injected ConnectionErrors and processes the
    next message once the inner client is healthy again."""
    # Speed up the backoff so the test stays under a second.
    monkeypatch.setattr(
        "src.infra.consumer.ReconnectBackoff",
        lambda *a, **kw: ReconnectBackoff(initial_sec=0.01, max_sec=0.02),
    )

    inner = fakeredis.aioredis.FakeRedis()
    flaky = FlakyRedis(inner, fail_count=2)

    pub = StreamPublisher(redis=inner, stream="news:raw")
    sample = RawNewsEvent(payload=RawNewsPayload(
        channel="@test", message_id=1, text="hello",
        tg_published_at="2026-06-07T00:00:00.000+00:00",
        received_at="2026-06-07T00:00:00.000+00:00",
        text_hash="a" * 64, has_media=False, is_reply=False, is_forward=False,
    ))
    await pub.publish(sample)

    consumer = StreamConsumer(
        redis=flaky,
        stream="news:raw",
        group="test-group",
        consumer_name="test-1",
        event_type=RawNewsEvent,
        block_ms=10,
    )

    received: list[RawNewsEvent] = []

    async def handler(ev):
        received.append(ev)

    shutdown = asyncio.Event()

    async def stop_when_received():
        for _ in range(200):
            if received:
                shutdown.set()
                return
            await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(
        consumer.run(handler, shutdown),
        stop_when_received(),
    )

    assert len(received) == 1, "consumer should process message after recovery"
    assert flaky.pool_resets >= 2, (
        f"pool should be reset on each connection error "
        f"(got {flaky.pool_resets} resets for 2 injected failures)"
    )
    await inner.aclose()


@pytest.mark.asyncio
async def test_consumer_handler_error_does_not_reset_pool(monkeypatch):
    """A handler-level exception (e.g. validation) must NOT reset the pool —
    that's a hot-path side effect we only want on transport failures."""
    monkeypatch.setattr(
        "src.infra.consumer.ReconnectBackoff",
        lambda *a, **kw: ReconnectBackoff(initial_sec=0.01, max_sec=0.02),
    )

    inner = fakeredis.aioredis.FakeRedis()

    class CountingRedis:
        def __init__(self, wrapped):
            self._wrapped = wrapped
            self.pool_resets = 0
            class _Pool:
                async def disconnect(_self, inuse_connections: bool = True):
                    self.pool_resets += 1
            self.connection_pool = _Pool()

        def __getattr__(self, name):
            return getattr(self._wrapped, name)

    counting = CountingRedis(inner)
    pub = StreamPublisher(redis=inner, stream="news:raw")
    ev = RawNewsEvent(payload=RawNewsPayload(
        channel="@test", message_id=1, text="hello",
        tg_published_at="2026-06-07T00:00:00.000+00:00",
        received_at="2026-06-07T00:00:00.000+00:00",
        text_hash="a" * 64, has_media=False, is_reply=False, is_forward=False,
    ))
    await pub.publish(ev)

    consumer = StreamConsumer(
        redis=counting, stream="news:raw", group="g2",
        consumer_name="c2", event_type=RawNewsEvent, block_ms=10,
    )

    calls = {"n": 0}

    async def handler(ev):
        calls["n"] += 1
        raise ValueError("handler bug — not a connection error")

    shutdown = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.3)
        shutdown.set()

    await asyncio.gather(consumer.run(handler, shutdown), stop_after())

    assert calls["n"] >= 1, "handler should have been called at least once"
    assert counting.pool_resets == 0, (
        f"handler-level errors must not trigger pool reset "
        f"(got {counting.pool_resets} resets)"
    )
    await inner.aclose()
