"""Tests for HeartbeatAggregator XRANGE polling."""
from __future__ import annotations

import pytest

from src.services.monitor.aggregator import HeartbeatAggregator


@pytest.mark.asyncio
async def test_tick_empty_stream(fake_redis):
    agg = HeartbeatAggregator(fake_redis, "system:heartbeats")
    state = await agg.tick()
    assert state == {}


@pytest.mark.asyncio
async def test_tick_absorbs_entries(fake_redis):
    await fake_redis.xadd("system:heartbeats", {
        "service": "receiver",
        "at": "2026-05-25T12:00:00.000+00:00",
        "events_in": "5",
    })
    agg = HeartbeatAggregator(fake_redis, "system:heartbeats")
    state = await agg.tick()
    assert "receiver" in state
    assert state["receiver"].last_seen_utc is not None
    assert state["receiver"].last_snapshot["events_in"] == "5"


@pytest.mark.asyncio
async def test_tick_ignores_already_seen(fake_redis):
    """Two ticks с одним и тем же entry → не дубль."""
    await fake_redis.xadd("system:heartbeats", {
        "service": "enricher",
        "at": "2026-05-25T12:00:00.000+00:00",
    })
    agg = HeartbeatAggregator(fake_redis, "system:heartbeats")
    await agg.tick()
    # Запоминаем last_id; на втором tick — без новых entries
    first_last = agg._last_id
    await agg.tick()
    assert agg._last_id == first_last


@pytest.mark.asyncio
async def test_tick_updates_on_new_entry(fake_redis):
    await fake_redis.xadd("system:heartbeats", {
        "service": "enricher",
        "at": "2026-05-25T12:00:00.000+00:00",
    })
    agg = HeartbeatAggregator(fake_redis, "system:heartbeats")
    await agg.tick()
    # Новая запись
    await fake_redis.xadd("system:heartbeats", {
        "service": "enricher",
        "at": "2026-05-25T12:00:30.000+00:00",
    })
    await agg.tick()
    assert agg.state["enricher"].last_seen_utc.isoformat().startswith("2026-05-25T12:00:30")


@pytest.mark.asyncio
async def test_tick_skips_entry_without_service_field(fake_redis):
    """Malformed entry без service field — silent skip."""
    await fake_redis.xadd("system:heartbeats", {"weird": "value"})
    agg = HeartbeatAggregator(fake_redis, "system:heartbeats")
    state = await agg.tick()
    assert state == {}
