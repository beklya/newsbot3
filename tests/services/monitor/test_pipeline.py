"""Integration tests for MonitorPipeline."""
from __future__ import annotations

import pytest

from src.services.monitor.aggregator import HeartbeatAggregator
from src.services.monitor.metrics import MonitorMetrics
from src.services.monitor.pipeline import MonitorPipeline


def _build_pipeline(fake_redis, monitor_settings):
    aggregator = HeartbeatAggregator(fake_redis, monitor_settings.heartbeat_stream)
    metrics = MonitorMetrics()
    return MonitorPipeline(
        settings=monitor_settings,
        aggregator=aggregator,
        redis=fake_redis,
        metrics=metrics,
    ), aggregator, metrics


@pytest.mark.asyncio
async def test_first_tick_alerts_no_heartbeats(fake_redis, monitor_settings):
    """All tracked services missing → один alert на каждый tracked service."""
    pipeline, _, metrics = _build_pipeline(fake_redis, monitor_settings)
    alerts = await pipeline.tick()
    # Кол-во alerts == кол-ву tracked_services (после Sprint 5.8: +quik_feed)
    assert len(alerts) == len(monitor_settings.tracked_services)
    assert all(a.rule == "missing_heartbeat" for a in alerts)


@pytest.mark.asyncio
async def test_startup_grace_suppresses_missing_heartbeats(fake_redis):
    """В течение startup_grace_sec missing_heartbeat alerts подавляются."""
    from src.services.monitor.config import MonitorSettings
    settings = MonitorSettings(
        startup_grace_sec=600,  # достаточно долго чтобы tick попал в grace
        missing_heartbeat_threshold_sec=90,
    )
    pipeline, _, metrics = _build_pipeline(fake_redis, settings)
    alerts = await pipeline.tick()
    # Никаких missing_heartbeat alerts во время grace
    assert all(a.rule != "missing_heartbeat" for a in alerts)
    snap = metrics.snapshot()
    assert snap.get("alerts.grace_suppressed.missing_heartbeat", 0) >= 1


@pytest.mark.asyncio
async def test_alert_dedup_suppresses_repeats(fake_redis, monitor_settings):
    """Same alert не должен повториться в следующие 10 tick'ов."""
    pipeline, _, _ = _build_pipeline(fake_redis, monitor_settings)
    first = await pipeline.tick()
    second = await pipeline.tick()
    assert len(first) > 0
    assert second == []  # suppressed


@pytest.mark.asyncio
async def test_daily_pnl_kill_emits_alert(fake_redis, monitor_settings):
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await fake_redis.set(
        f"{monitor_settings.risk_daily_pnl_key_prefix}{today}",
        "-15000",
    )
    pipeline, _, _ = _build_pipeline(fake_redis, monitor_settings)
    alerts = await pipeline.tick()
    pnl_alerts = [a for a in alerts if a.rule == "daily_pnl_kill"]
    assert len(pnl_alerts) == 1
    assert pnl_alerts[0].severity == "crit"
