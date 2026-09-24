"""EnrichmentCache — Redis-backed lookup of EnrichedNewsEvent by event_id.

Enricher (Sprint 5.4) populates this cache via SETEX `enriched:<event_id>`
JSON TTL=300s parallel to news:enriched stream publish. Decision reads
on each ml:predictions arrival.

Cache miss is normal (event aged out — 5min TTL, slow predictor lag, etc.).
Decision treats miss as skip + log; не DLQ.
"""
from __future__ import annotations

import logging
from typing import Optional

from redis.asyncio import Redis

from src.contracts.enriched_news import EnrichedNewsEvent

log = logging.getLogger(__name__)


class EnrichmentCache:
    def __init__(self, redis: Redis, key_prefix: str = "enriched:") -> None:
        self.redis = redis
        self.key_prefix = key_prefix

    async def get(self, event_id: str) -> Optional[EnrichedNewsEvent]:
        """Lookup by enriched event_id. Returns None on miss or parse error."""
        key = f"{self.key_prefix}{event_id}"
        try:
            raw = await self.redis.get(key)
        except Exception:
            log.exception("enrichment_cache_redis_error key=%s", key)
            return None
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return EnrichedNewsEvent.model_validate_json(raw)
        except Exception:
            log.exception("enrichment_cache_parse_error key=%s", key)
            return None
