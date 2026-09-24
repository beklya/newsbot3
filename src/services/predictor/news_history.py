"""NewsHistory — per-ticker rolling 24h history of EnrichedNewsEvent.

Used by feature_builder.py для computing 3 features:
- news_count_24h
- cum_sentiment_24h
- time_since_last_min

Bootstrap on startup: XREAD news:enriched от начала истории, filter
по timestamp ≥ now-24h, заполнить per-ticker deque'ами. На каждое
свежее news:enriched event — append.

Plan Open Q1 resolved: 50k events/day легко влезает в RAM как
dict[ticker, deque[EnrichedNewsEvent]]. Sorted set Redis не нужен.
"""
from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, Optional

from redis.asyncio import Redis

from src.contracts.enriched_news import EnrichedNewsEvent

log = logging.getLogger(__name__)


class NewsHistory:
    """Rolling 24h per-ticker history."""

    def __init__(self, lookback_hours: int = 24, per_ticker_maxlen: int = 50):
        self._lookback = timedelta(hours=lookback_hours)
        self._maxlen = per_ticker_maxlen
        self._by_ticker: Dict[str, Deque[EnrichedNewsEvent]] = defaultdict(
            lambda: deque(maxlen=per_ticker_maxlen),
        )

    def append(self, event: EnrichedNewsEvent) -> None:
        """Add event to per-ticker deque'ы для каждого ticker'а в tickers[]."""
        for t in event.payload.tickers:
            self._by_ticker[t.ticker].append(event)

    def for_ticker(self, ticker: str, now: Optional[datetime] = None) -> list[EnrichedNewsEvent]:
        """Return events for ticker within lookback window. Most recent last."""
        if ticker not in self._by_ticker:
            return []
        cutoff = (now or datetime.now(timezone.utc)) - self._lookback
        result = []
        for ev in self._by_ticker[ticker]:
            ev_ts = datetime.fromisoformat(ev.produced_at)
            if ev_ts >= cutoff:
                result.append(ev)
        return result

    async def bootstrap(self, redis: Redis, stream: str) -> int:
        """Load last 24h of news:enriched into memory on startup.

        Uses XRANGE с MIN timestamp. Returns count of events loaded.
        """
        cutoff = datetime.now(timezone.utc) - self._lookback
        # Redis Stream IDs are milliseconds; convert cutoff to that base.
        min_id = f"{int(cutoff.timestamp() * 1000)}-0"
        log.info("Bootstrapping news history from %s (XRANGE %s..+, stream=%s)",
                 cutoff.isoformat(), min_id, stream)

        count = 0
        try:
            entries = await redis.xrange(stream, min=min_id, max="+", count=100_000)
        except Exception:
            log.exception("bootstrap_xrange_failed stream=%s", stream)
            return 0

        for _msg_id, fields in entries:
            try:
                raw = fields.get(b"data") or fields.get("data")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                ev = EnrichedNewsEvent.model_validate_json(raw)
                self.append(ev)
                count += 1
            except Exception as e:
                # Stale/corrupt entries shouldn't kill the bootstrap.
                log.warning("bootstrap_skip_entry err=%s", e)

        n_tickers = len(self._by_ticker)
        log.info("NewsHistory bootstrap done: %d events across %d tickers", count, n_tickers)
        return count

    def stats(self) -> Dict[str, int]:
        """Counter snapshot for heartbeats."""
        return {
            "news_history_tickers": len(self._by_ticker),
            "news_history_events_total": sum(len(d) for d in self._by_ticker.values()),
        }
