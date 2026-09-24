"""HeartbeatAggregator — read `system:heartbeats`, track per-service last-seen.

Tail mode: на каждой итерации pollloop'а XRANGE с last-seen-id вперёд, парсим
все новые entries. Возвращаем dict[service → last_seen_dt + sample counters]
для alert rules.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional

from redis.asyncio import Redis

from src.infra.redis_retry import is_connection_error, reset_pool

log = logging.getLogger(__name__)


@dataclass
class ServiceHeartbeatState:
    service: str
    last_seen_utc: Optional[datetime] = None
    last_entry_id: Optional[str] = None
    last_snapshot: Dict[str, str] = field(default_factory=dict)


class HeartbeatAggregator:
    def __init__(self, redis: Redis, stream: str) -> None:
        self.redis = redis
        self.stream = stream
        self._state: Dict[str, ServiceHeartbeatState] = {}
        # Cursor across all consumers of the stream — start from "0" on bootstrap, then "$"
        self._last_id: str = "0-0"

    async def tick(self) -> Dict[str, ServiceHeartbeatState]:
        """Read all new heartbeats since last tick, update per-service state.

        Sprint 5.11: exclusive-range syntax `(ID` requires Redis 6.2+. VPS Redis
        is 6.0.16 (Ubuntu 22.04 default), so we increment seq by 1 instead —
        works on any Redis version that supports streams.
        """
        try:
            next_start = self._next_id_after(self._last_id)
            entries = await self.redis.xrange(self.stream, min=next_start, max="+", count=10_000)
        except Exception as exc:
            # Sprint 6.2 — on connection-level failures (tunnel drop), force the
            # pool to discard dead sockets so the next tick reconnects cleanly.
            # tick() is called from a 30s poll loop, so we don't need backoff
            # here — the natural interval is already conservative.
            if is_connection_error(exc):
                log.warning("aggregator connection error stream=%s: %s",
                            self.stream, exc)
                await reset_pool(self.redis, where=f"aggregator/{self.stream}")
                return self._state
            log.exception("aggregator_xrange_failed stream=%s", self.stream)
            return self._state

        for msg_id, fields in entries:
            msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            self._last_id = msg_id_str
            self._absorb_entry(fields)

        return self._state

    @staticmethod
    def _next_id_after(stream_id: str) -> str:
        """Compute the smallest valid stream ID strictly greater than `stream_id`.

        Redis stream IDs are `ms-seq`. For 6.0 compatibility we can't use the
        exclusive `(ms-seq` syntax, so bump seq by 1 instead (gives the next
        possible ID, no in-between values exist).
        """
        if "-" in stream_id:
            ms_part, seq_part = stream_id.split("-", 1)
            try:
                return f"{ms_part}-{int(seq_part) + 1}"
            except ValueError:
                return stream_id
        # No seq → assume seq=0; bump to seq=1
        return f"{stream_id}-1"

    def _absorb_entry(self, fields: dict) -> None:
        # Decode bytes if needed
        decoded: Dict[str, str] = {}
        for k, v in fields.items():
            ks = k.decode() if isinstance(k, bytes) else k
            vs = v.decode() if isinstance(v, bytes) else v
            decoded[ks] = vs

        service = decoded.get("service")
        if not service:
            return
        at = decoded.get("at")
        last_seen = None
        if at and at != "final":
            try:
                last_seen = datetime.fromisoformat(at)
                if last_seen.tzinfo is None:
                    last_seen = last_seen.replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        if service not in self._state:
            self._state[service] = ServiceHeartbeatState(service=service)
        st = self._state[service]
        if last_seen is not None:
            st.last_seen_utc = last_seen
        st.last_snapshot = decoded

    @property
    def state(self) -> Dict[str, ServiceHeartbeatState]:
        return self._state
