# src/infra/consumer.py
import asyncio
import logging
from typing import Callable, Awaitable, Type
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from src.contracts.base import MessageEnvelope
from src.infra.redis_retry import (
    ReconnectBackoff,
    is_connection_error,
    reset_pool,
)

log = logging.getLogger(__name__)

DEFAULT_BLOCK_MS = 5_000


class StreamConsumer:
    """Reads from a Redis Stream consumer group.

    Behavior on start:
    1. First reads its own PEL (Pending Entries List) — messages that were
       delivered to THIS consumer name but never xack'ed (e.g. crashed mid-handling
       or threw retryable Exception). This recovers in-flight work after a restart.
    2. Once PEL is drained, switches to '>' to read new messages.
    3. If a handler still raises on a PEL replay, the message stays in PEL — the
       retry count (xpending delivered) increments.

    Special read ID semantics in Redis XREADGROUP:
        ">"  → only undelivered messages (new ones from the stream)

    Note on PEL recovery: we use XPENDING + XCLAIM (not XREADGROUP with "0") —
    this is more explicit and works consistently across Redis implementations
    including fakeredis.
    """

    _NEW_CURSOR = ">"

    def __init__(
        self,
        redis: Redis,
        stream: str,
        group: str,
        consumer_name: str,
        event_type: Type[MessageEnvelope],
        block_ms: int = DEFAULT_BLOCK_MS,
    ):
        self.redis = redis
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self.event_type = event_type
        self.block_ms = block_ms

    async def ensure_group(self):
        try:
            await self.redis.xgroup_create(
                self.stream, self.group, id="0", mkstream=True
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _read(self, cursor: str, block_ms: int) -> list:
        """Read a batch from xreadgroup. Returns list of (msg_id, fields) tuples."""
        messages = await self.redis.xreadgroup(
            self.group, self.consumer_name,
            streams={self.stream: cursor},
            count=10,
            block=block_ms,
        )
        if not messages:
            return []
        # Format: [(stream_name, [(msg_id, fields), ...])]
        for _stream_name, msgs in messages:
            if msgs:
                return msgs
        return []

    async def _recover_pel(self, handler, shutdown: asyncio.Event) -> None:
        """Drain this consumer's PEL on startup using XPENDING + XCLAIM.

        Note: we don't use xreadgroup with cursor "0" — that approach has subtle
        issues across Redis implementations (fakeredis returns empty results).
        Instead, we explicitly look up PEL entries via xpending_range and reclaim
        them via xclaim. This is the same effect: we get to (re)process our own
        previously-undelivered messages.

        Sprint 6.2 — on a ConnectionError (tunnel down on cold start) we reset
        the pool and retry with backoff instead of giving up. Previously we
        broke out, leaving messages stranded in PEL until the next restart.
        """
        log.info(
            "PEL recovery starting stream=%s consumer=%s",
            self.stream, self.consumer_name,
        )
        recovered = 0
        backoff = ReconnectBackoff()
        # Loop in case PEL is large; batch through 100 at a time.
        while not shutdown.is_set():
            try:
                pending = await self.redis.xpending_range(
                    self.stream, self.group,
                    min="-", max="+", count=100,
                    consumername=self.consumer_name,
                )
            except Exception as exc:
                if is_connection_error(exc):
                    log.warning(
                        "PEL xpending_range connection error (attempt %d): %s",
                        backoff.attempts + 1, exc,
                    )
                    await reset_pool(self.redis, where=f"pel/{self.stream}")
                    await backoff.sleep(shutdown)
                    continue
                log.exception("PEL xpending_range error")
                break

            backoff.reset()

            if not pending:
                break

            # Collect message IDs for this consumer's PEL.
            ids = []
            for entry in pending:
                # Tolerant decode — entry может быть dict со str или bytes ключами
                if isinstance(entry, dict):
                    mid = entry.get("message_id") or entry.get(b"message_id")
                else:
                    mid = entry[0] if len(entry) > 0 else None
                if mid is not None:
                    ids.append(mid)

            if not ids:
                break

            # xclaim re-delivers these messages to us with fresh idle counter.
            # min_idle_time=0 — claim immediately without waiting.
            try:
                claimed = await self.redis.xclaim(
                    self.stream, self.group, self.consumer_name,
                    min_idle_time=0,
                    message_ids=ids,
                )
            except Exception as exc:
                if is_connection_error(exc):
                    log.warning(
                        "PEL xclaim connection error (attempt %d): %s",
                        backoff.attempts + 1, exc,
                    )
                    await reset_pool(self.redis, where=f"pel-claim/{self.stream}")
                    await backoff.sleep(shutdown)
                    continue
                log.exception("PEL xclaim error")
                break

            if not claimed:
                break

            for entry in claimed:
                if shutdown.is_set():
                    return
                # claimed entry is (msg_id, fields)
                if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    msg_id, fields = entry[0], entry[1]
                    await self._handle(handler, msg_id, fields)
                    recovered += 1

            # If we got fewer than the requested batch size, we've drained.
            if len(claimed) < 100:
                break

        log.info(
            "PEL recovery done stream=%s consumer=%s recovered=%d",
            self.stream, self.consumer_name, recovered,
        )

    async def _ensure_group_with_retry(self, shutdown: asyncio.Event) -> None:
        """ensure_group, surviving cold-start tunnel downs.

        Without this, the very first xgroup_create call hits a dead tunnel and
        the service crashes before `run()` reaches its retry loop. We use the
        same backoff machinery as the main loop.
        """
        backoff = ReconnectBackoff()
        while not shutdown.is_set():
            try:
                await self.ensure_group()
                return
            except Exception as exc:
                if is_connection_error(exc):
                    log.warning(
                        "ensure_group connection error (attempt %d) stream=%s: %s",
                        backoff.attempts + 1, self.stream, exc,
                    )
                    await reset_pool(self.redis, where=f"ensure_group/{self.stream}")
                    await backoff.sleep(shutdown)
                    continue
                raise

    async def run(
        self,
        handler: Callable[[MessageEnvelope], Awaitable[None]],
        shutdown: asyncio.Event,
    ):
        await self._ensure_group_with_retry(shutdown)
        log.info(
            "consumer started stream=%s group=%s consumer=%s block_ms=%d",
            self.stream, self.group, self.consumer_name, self.block_ms,
        )
        # Phase 1: recover own PEL
        await self._recover_pel(handler, shutdown)

        # Phase 2: read new messages until shutdown.
        # Sprint 6.2 — differentiate connection-level failures (force pool
        # disconnect + exponential backoff, so we don't burn cycles on a stale
        # socket) from handler-level errors (keep the legacy 1-second pause).
        backoff = ReconnectBackoff()
        while not shutdown.is_set():
            try:
                msgs = await self._read(self._NEW_CURSOR, block_ms=self.block_ms)
                backoff.reset()
            except Exception as exc:
                if is_connection_error(exc):
                    log.warning(
                        "Consumer connection error stream=%s (attempt %d): %s",
                        self.stream, backoff.attempts + 1, exc,
                    )
                    await reset_pool(self.redis, where=f"consumer/{self.stream}")
                    await backoff.sleep(shutdown)
                    continue
                log.exception("Consumer error")
                await asyncio.sleep(1)
                continue
            if not msgs:
                # Empty result — either real Redis returned after block timeout
                # or fakeredis returned immediately. Yield control to event loop.
                await asyncio.sleep(0.01)
                continue
            for msg_id, fields in msgs:
                if shutdown.is_set():
                    break
                await self._handle(handler, msg_id, fields)

        log.info("consumer stopped stream=%s consumer=%s", self.stream, self.consumer_name)

    async def _handle(self, handler, msg_id, fields):
        try:
            data = fields[b"data"].decode("utf-8")
            event = self.event_type.model_validate_json(data)
            await handler(event)
            await self.redis.xack(self.stream, self.group, msg_id)
        except Exception:
            log.exception("Handler failed for %s", msg_id)
            # NO xack → message stays in PEL for next reclaim/retry
