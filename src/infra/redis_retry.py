"""Sprint 6.2 — Redis connection-loss recovery helpers.

Background
----------
Production setup runs services on local Windows box connected to VPS Redis
via SSH tunnel `127.0.0.1:6380 → vps:6379`. Tunnel drops happen for several
network-level reasons (CGNAT timeouts, route flaps, sshd keepalive misses).

Empirically `redis-py` async client does NOT auto-recover from these drops:
- Broken sockets stay in the ConnectionPool and the next xreadgroup/xrange
  raises `ConnectionError` immediately or hangs.
- The existing `try/except → sleep(1) → continue` pattern in
  `StreamConsumer.run()` and friends is not enough — the pool needs to be
  explicitly drained, otherwise subsequent retries may re-use the same dead
  connection.

This module provides:
- `is_connection_error(exc)` — recognize transport-level Redis failures
  vs. handler-level exceptions (which we want to keep PEL'ing as before)
- `reset_pool(redis)` — force-disconnect all pooled connections, so the
  next call opens a fresh socket through the (recovered) tunnel
- `ReconnectBackoff` — exponential backoff helper, resetting on success
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Classification

def is_connection_error(exc: BaseException) -> bool:
    """True if exception indicates a transport-level failure where the pool
    should be reset before retrying.

    Covers:
    - `redis.ConnectionError` / `redis.TimeoutError` — most common
    - `ConnectionResetError` (Windows: WinError 64 "Указанное сетевое имя
      более недоступно", WinError 10054)
    - `ConnectionRefusedError` (Windows: WinError 1225) — tunnel down,
      target port not listening
    - `ConnectionAbortedError`
    - `OSError` with the above errno aliases (raw socket errors before
      redis-py wraps them)
    - `asyncio.TimeoutError` — block_ms timeout combined with a stalled
      socket can surface as this if we wrap a read in wait_for
    """
    if isinstance(exc, (RedisConnectionError, RedisTimeoutError)):
        return True
    if isinstance(exc, (
        ConnectionResetError, ConnectionRefusedError, ConnectionAbortedError,
        BrokenPipeError, asyncio.TimeoutError,
    )):
        return True
    # Some redis-py paths raise plain OSError when the underlying transport
    # dies mid-flight. Treat any OSError as connection-level for our purposes
    # — handler-level code paths don't produce OSError directly.
    if isinstance(exc, OSError):
        return True
    return False


# -----------------------------------------------------------------------------
# Pool reset

async def reset_pool(redis: Redis, *, where: str = "") -> None:
    """Force-disconnect every connection currently pooled in `redis`.

    `redis-py` async ConnectionPool keeps a free-list and an in-use set.
    When a connection's socket dies, redis-py marks it but does NOT proactively
    drop it from the pool — the next `get_connection()` may still return it,
    and the dead socket's TCP state has to be discovered all over again.

    Calling `pool.disconnect(inuse_connections=True)` walks both lists and
    closes every socket, so the next `get_connection()` opens a fresh one
    (which finally succeeds once the tunnel is back).

    This is safe to call even after a successful operation — pool re-creates
    connections on demand. We only do it after we've observed a connection
    failure, to keep the latency hit out of the hot path.
    """
    try:
        pool = getattr(redis, "connection_pool", None)
        if pool is None:
            return
        await pool.disconnect(inuse_connections=True)
        log.info("redis pool reset where=%s", where or "(unspecified)")
    except Exception as exc:
        # Pool reset is best-effort. If it itself errors (rare), log and let
        # the retry loop try again.
        log.warning("redis pool reset failed where=%s err=%s", where, exc)


# -----------------------------------------------------------------------------
# Exponential backoff

class ReconnectBackoff:
    """Stateful exponential backoff with `reset()` on success.

    Default schedule: 1s → 2s → 4s → 8s → 8s … (cap), reset to 1s after the
    next successful operation.

    Usage:
        backoff = ReconnectBackoff()
        while not shutdown.is_set():
            try:
                await do_work()
                backoff.reset()
            except Exception as e:
                if is_connection_error(e):
                    await reset_pool(redis)
                    await backoff.sleep(shutdown)
                else:
                    log.exception("handler error")
                    await asyncio.sleep(1)
    """

    def __init__(
        self,
        initial_sec: float = 1.0,
        max_sec: float = 8.0,
        multiplier: float = 2.0,
    ) -> None:
        self.initial = initial_sec
        self.max = max_sec
        self.mult = multiplier
        self._current = initial_sec
        self._attempts = 0

    def reset(self) -> None:
        if self._attempts > 0:
            log.info("redis reconnect backoff reset after %d failed attempt(s)",
                     self._attempts)
        self._current = self.initial
        self._attempts = 0

    async def sleep(self, shutdown: Optional[asyncio.Event] = None) -> None:
        """Wait current delay, then advance. Cancellable via shutdown event."""
        self._attempts += 1
        delay = self._current
        # advance for next call
        self._current = min(self._current * self.mult, self.max)
        if shutdown is not None:
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(delay)

    @property
    def attempts(self) -> int:
        return self._attempts
