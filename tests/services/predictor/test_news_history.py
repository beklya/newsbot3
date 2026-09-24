"""Tests for NewsHistory append + for_ticker + bootstrap."""
from __future__ import annotations

import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone

from src.contracts.enriched_news import EnrichedNewsEvent
from src.services.predictor.news_history import NewsHistory


def test_append_separates_tickers(enriched_event_factory):
    h = NewsHistory(lookback_hours=24, per_ticker_maxlen=50)
    from src.contracts.enriched_news import TickerImpact
    multi = enriched_event_factory(tickers=[
        TickerImpact(ticker="GAZP", direction="long", sentiment="positive",
                     confidence=0.7, impact_strength=0.5, rationale=""),
        TickerImpact(ticker="LKOH", direction="short", sentiment="negative",
                     confidence=0.6, impact_strength=0.4, rationale=""),
    ])
    h.append(multi)

    assert len(h.for_ticker("GAZP")) == 1
    assert len(h.for_ticker("LKOH")) == 1
    assert len(h.for_ticker("SBER")) == 0


def test_lookback_window(enriched_event_factory):
    h = NewsHistory(lookback_hours=24, per_ticker_maxlen=50)
    now = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    fresh = enriched_event_factory(produced_at=(now - timedelta(hours=1)).isoformat(timespec="milliseconds"))
    stale = enriched_event_factory(produced_at=(now - timedelta(hours=48)).isoformat(timespec="milliseconds"))
    h.append(fresh)
    h.append(stale)

    result = h.for_ticker("GAZP", now=now)
    assert len(result) == 1
    assert result[0].event_id == fresh.event_id


@pytest.mark.asyncio
async def test_bootstrap_loads_from_redis(fake_redis, enriched_event_factory):
    """XRANGE should re-hydrate per-ticker deques on startup."""
    ev = enriched_event_factory(ticker="GAZP")
    await fake_redis.xadd("news:enriched", {"data": ev.model_dump_json()})

    h = NewsHistory(lookback_hours=24)
    count = await h.bootstrap(fake_redis, "news:enriched")
    assert count == 1
    assert len(h.for_ticker("GAZP")) == 1


def test_stats_returns_aggregate_counts(enriched_event_factory):
    h = NewsHistory()
    h.append(enriched_event_factory(ticker="GAZP"))
    h.append(enriched_event_factory(ticker="LKOH"))
    stats = h.stats()
    assert stats["news_history_tickers"] == 2
    assert stats["news_history_events_total"] == 2
