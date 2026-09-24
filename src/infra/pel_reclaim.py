"""Sprint 6.2 — PEL Reclaim Daemon.

Background
----------
StreamConsumer recovers its own PEL only on startup (cold path).  During
steady-state operation, if a handler raises an exception, the message stays
in PEL forever — the main `xreadgroup` loop only requests new (`>`) messages,
never PEL'd ones.

Observed 2026-06-09 on enricher with 6 parallel workers:
- 633 messages stuck in PEL (invalid_json errors from DI 70B under load)
- All 6 consumers idle ~3.4 hours with these PEL items
- Throughput dropped to ~1 ev/min because workers were "holding" failed work
  in PEL while servicing only fresh messages

Solution
--------
Spawn one PelReclaimer per service.  It runs in parallel with the regular
StreamConsumer workers and periodically:

1. XAUTOCLAIM stream group reclaimer-N <min-idle-ms> <cursor> COUNT 100
2. For each claimed message, check `times_delivered` via XPENDING
3. If `times_delivered > max_deliveries`, route to DLQ + xack (give up)
4. Otherwise call the same handler the main consumers use
5. On success: xack.  On exception: leave in PEL — it will be reclaimed again
   on the next tick (with `times_delivered + 1`).

Design notes
------------
- `min_idle_ms` MUST exceed the longest expected handler latency so we never
  steal in-flight work from a still-running consumer.  For enricher with DI
  70B p95 ~36s + transient retries up to ~80s, we default to 120_000ms.
- `poll_interval_sec` cadence (default 30s) controls how fast we retry stuck
  items.  Don't lower below min_idle_ms / 4 — wastes XAUTOCLAIM cycles.
- The reclaimer uses its OWN consumer_name so XAUTOCLAIM transfers ownership
  cleanly.  Original consumer's PEL count drops, reclaimer's PEL count grows
  only briefly between claim and ack.
- Connection-error resilience reuses src.infra.redis_retry (Task #10).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional, Type

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from src.contracts.base import MessageEnvelope
from src.infra.redis_retry import (
    ReconnectBackoff,
    is_connection_error,
    reset_pool,
)

log = logging.getLogger(__name__)


# Type aliases
HandlerFn = Callable[[MessageEnvelope], Awaitable[None]]
DlqHandlerFn = Callable[[bytes, dict, str], Awaitable[None]]
# DlqHandlerFn signature: (msg_id, fields, reason) -> None
# Called when max_deliveries exceeded; implementation publishes wherever it wants.


class PelReclaimer:
    """Periodic XAUTOCLAIM-based PEL drainer for a single consumer group.

    Lives alongside regular StreamConsumer workers in the same process. Each
    tick claims PEL messages idle longer than `min_idle_ms` and retries them
    via the supplied handler. Messages exceeding `max_deliveries` are
    optionally routed to a caller-supplied DLQ handler before being xack'd
    (terminal give-up).
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        group: str,
        consumer_name: str,
        event_type: Type[MessageEnvelope],
        handler: HandlerFn,
        *,
        min_idle_ms: int = 120_000,
        poll_interval_sec: int = 30,
        max_deliveries: int = 4,  # 1 initial + 3 reclaim attempts
        dlq_handler: Optional[DlqHandlerFn] = None,
        batch_count: int = 100,
    ) -> None:
        self.redis = redis
        self.stream = stream
        self.group = group
        self.consumer_name = consumer_name
        self.event_type = event_type
        self.handler = handler
        self.min_idle_ms = min_idle_ms
        self.poll_interval_sec = poll_interval_sec
        self.max_deliveries = max_deliveries
        self.dlq_handler = dlq_handler
        self.batch_count = batch_count
        # Counters for visibility (heartbeat snapshot integration)
        self.n_reclaimed = 0
        self.n_recovered_ok = 0
        self.n_recovered_fail = 0
        self.n_dlq_terminal = 0

    async def run(self, shutdown: asyncio.Event) -> None:
        log.info(
            "PEL reclaimer started stream=%s group=%s consumer=%s "
            "min_idle_ms=%d poll=%ds max_deliveries=%d",
            self.stream, self.group, self.consumer_name,
            self.min_idle_ms, self.poll_interval_sec, self.max_deliveries,
        )
        backoff = ReconnectBackoff()
        # Run a tick immediately on startup — at production min_idle_ms (120s)
        # we won't steal in-flight work because nothing is idle that long yet.
        # This also makes the reclaimer reactive: on service restart, we begin
        # draining PEL right away rather than waiting one interval.
        while not shutdown.is_set():
            try:
                await self._tick(shutdown)
                backoff.reset()
            except Exception as exc:
                if is_connection_error(exc):
                    log.warning(
                        "PEL reclaimer connection error stream=%s (attempt %d): %s",
                        self.stream, backoff.attempts + 1, exc,
                    )
                    await reset_pool(self.redis, where=f"pel_reclaim/{self.stream}")
                    await backoff.sleep(shutdown)
                    continue
                log.exception("PEL reclaimer tick error")
            await self._sleep_interval(shutdown)
        log.info(
            "PEL reclaimer stopped stream=%s reclaimed=%d ok=%d fail=%d dlq=%d",
            self.stream, self.n_reclaimed,
            self.n_recovered_ok, self.n_recovered_fail, self.n_dlq_terminal,
        )

    async def _sleep_interval(self, shutdown: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(
                shutdown.wait(), timeout=self.poll_interval_sec,
            )
        except asyncio.TimeoutError:
            pass

    async def _tick(self, shutdown: asyncio.Event) -> None:
        """Drain all PEL items eligible right now (idle >= min_idle_ms).

        XAUTOCLAIM pages through eligible messages. We loop until the cursor
        returns to "0-0" (no more eligible) or until shutdown.
        """
        cursor: Any = "0-0"
        claimed_total_this_tick = 0
        while not shutdown.is_set():
            try:
                # redis-py async exposes xautoclaim as a method.
                response = await self.redis.xautoclaim(
                    self.stream,
                    self.group,
                    self.consumer_name,
                    min_idle_time=self.min_idle_ms,
                    start_id=cursor,
                    count=self.batch_count,
                )
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    log.warning(
                        "PEL reclaim NOGROUP %s/%s — group not yet created, skipping tick",
                        self.stream, self.group,
                    )
                    return
                raise
            # redis-py returns (next_cursor, claimed_msgs, deleted_ids) for Redis 6.2+
            # or (next_cursor, claimed_msgs) for older.
            if isinstance(response, tuple) or isinstance(response, list):
                if len(response) >= 2:
                    cursor = response[0]
                    claimed = response[1]
                else:
                    log.warning("Unexpected xautoclaim response shape: %s", response)
                    return
            else:
                log.warning("Unexpected xautoclaim response type: %s", type(response))
                return

            if not claimed:
                break

            if isinstance(cursor, bytes):
                cursor_str = cursor.decode()
            else:
                cursor_str = str(cursor)

            for entry in claimed:
                if shutdown.is_set():
                    return
                msg_id, fields = self._unpack_entry(entry)
                if msg_id is None:
                    continue
                self.n_reclaimed += 1
                claimed_total_this_tick += 1
                await self._process_one(msg_id, fields)

            if cursor_str in ("0-0", "0", ""):
                break

        if claimed_total_this_tick > 0:
            log.info(
                "PEL reclaim tick stream=%s reclaimed=%d (ok=%d fail=%d dlq=%d total)",
                self.stream, claimed_total_this_tick,
                self.n_recovered_ok, self.n_recovered_fail, self.n_dlq_terminal,
            )

    @staticmethod
    def _unpack_entry(entry: Any) -> tuple[Optional[bytes], Optional[dict]]:
        """Extract (msg_id, fields) from a claimed entry — tolerant of shape."""
        if isinstance(entry, (list, tuple)) and len(entry) >= 2:
            return entry[0], entry[1]
        return None, None

    async def _process_one(self, msg_id: bytes, fields: dict) -> None:
        """Check delivery count, then either DLQ or re-handle."""
        delivery_count = await self._get_delivery_count(msg_id)
        if (
            self.max_deliveries > 0
            and delivery_count > self.max_deliveries
        ):
            log.warning(
                "PEL terminal: msg_id=%s delivery_count=%d > max=%d — sending to DLQ",
                msg_id, delivery_count, self.max_deliveries,
            )
            try:
                if self.dlq_handler is not None:
                    await self.dlq_handler(
                        msg_id, fields,
                        f"max_deliveries_exceeded (count={delivery_count})",
                    )
            except Exception:
                log.exception("DLQ handler raised for terminal msg_id=%s", msg_id)
            # ack regardless to release the slot
            try:
                await self.redis.xack(self.stream, self.group, msg_id)
            except Exception:
                log.exception("xack failed for terminal msg_id=%s", msg_id)
            self.n_dlq_terminal += 1
            return

        # Re-handle via the same handler the main workers use.
        try:
            raw = fields[b"data"] if b"data" in fields else fields.get("data")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            event = self.event_type.model_validate_json(raw)
            await self.handler(event)
            await self.redis.xack(self.stream, self.group, msg_id)
            self.n_recovered_ok += 1
        except Exception:
            log.exception(
                "PEL reclaim retry failed msg_id=%s delivery_count_was=%d "
                "(left in PEL, will retry on next tick)",
                msg_id, delivery_count,
            )
            self.n_recovered_fail += 1
            # NO xack — message stays in PEL, will reclaim again

    async def _get_delivery_count(self, msg_id: bytes) -> int:
        """Look up `times_delivered` for a specific message id.

        After XAUTOCLAIM, redis-py increments the count internally. So when we
        observe the count here it reflects the count AFTER this claim. For our
        bookkeeping we treat that as the current delivery attempt number.
        """
        try:
            pending = await self.redis.xpending_range(
                self.stream, self.group,
                min=msg_id, max=msg_id, count=1,
            )
        except Exception:
            return 0
        if not pending:
            return 0
        entry = pending[0]
        if isinstance(entry, dict):
            for key in ("times_delivered", b"times_delivered"):
                if key in entry:
                    val = entry[key]
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        return 0
        # Tuple shape (XPENDING <stream> <group> <start> <end> N): (id, consumer, idle, times_delivered)
        if isinstance(entry, (list, tuple)) and len(entry) >= 4:
            try:
                return int(entry[3])
            except (ValueError, TypeError):
                return 0
        return 0

    def snapshot(self) -> dict[str, int]:
        """Counters for heartbeat publisher."""
        return {
            "pel_reclaim_n": self.n_reclaimed,
            "pel_reclaim_ok": self.n_recovered_ok,
            "pel_reclaim_fail": self.n_recovered_fail,
            "pel_reclaim_dlq": self.n_dlq_terminal,
        }
