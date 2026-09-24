"""Shared HeartbeatPublisher (Sprint 5 / Commit 5.0).

Periodic heartbeat publisher used by all services (receiver, enricher,
predictor, decision, bridge, monitor). Writes a small snapshot to
`system:heartbeats` stream every N seconds. If the gap exceeds N+threshold,
the service is presumed stuck or dead.

Previously duplicated в `services/receiver/heartbeat.py` и
`services/enricher/heartbeat.py` — слиты сюда (DRY).

Snapshot semantics:
- `snapshot_fn` возвращает dict[str, Any] произвольных counter'ов / gauge'ов
  специфичных для сервиса. Все значения сериализуются как str (требование
  Redis Streams).
- Поля `service` и `at` (ISO timestamp) добавляются автоматически.

Errors-eat-and-continue: never crashes the caller's main loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, Optional

from redis.asyncio import Redis

from src.contracts.base import utcnow_iso
from src.infra.redis_retry import is_connection_error, reset_pool

log = logging.getLogger(__name__)

SnapshotFn = Callable[[], Dict[str, Any]]


class HeartbeatPublisher:
    """Background task that XADDs a heartbeat to a Redis stream periodically.

    Designed to be cheap (one xadd per interval) and never to crash the
    main pipeline — all errors are logged and the loop continues.
    """

    def __init__(
        self,
        redis: Redis,
        stream: str,
        producer: str,
        interval_sec: int,
        snapshot_fn: SnapshotFn,
        maxlen: int = 5_000,
    ) -> None:
        self.redis = redis
        self.stream = stream
        self.producer = producer
        self.interval_sec = interval_sec
        self.snapshot_fn = snapshot_fn
        self.maxlen = maxlen
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._task is not None:
            log.warning("heartbeat: start() called but task already running")
            return
        self._task = asyncio.create_task(self._run(), name=f"{self.producer}.heartbeat")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                log.warning("heartbeat: did not stop in 2s, cancelling")
                self._task.cancel()
            self._task = None

    async def _publish_one(self) -> None:
        snap = self.snapshot_fn()
        fields: Dict[str, str] = {
            "service": self.producer,
            "at": utcnow_iso(),
        }
        for key, val in snap.items():
            fields[key] = str(val)
        await self.redis.xadd(
            self.stream,
            fields=fields,
            maxlen=self.maxlen,
            approximate=True,
        )

    async def _run(self) -> None:
        log.info(
            "heartbeat: started producer=%s interval=%ds stream=%s",
            self.producer, self.interval_sec, self.stream,
        )
        try:
            await self._publish_one()
        except Exception as e:
            log.warning("heartbeat: initial publish failed: %s", e)
            if is_connection_error(e):
                await reset_pool(self.redis, where=f"heartbeat/{self.producer}")

        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_sec)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                break
            try:
                await self._publish_one()
            except Exception as e:
                log.warning("heartbeat: publish failed: %s", e)
                # Sprint 6.2 — proactively reset the pool when xadd fails for
                # connection reasons. Without this the heartbeat keeps stamping
                # the same dead socket every interval_sec until the service
                # restarts, and stale heartbeat events confuse Monitor.
                if is_connection_error(e):
                    await reset_pool(self.redis, where=f"heartbeat/{self.producer}")

        log.info("heartbeat: stopped producer=%s", self.producer)
