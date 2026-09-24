"""Tests for EnricherMetrics."""
from __future__ import annotations

import time

from src.services.enricher.metrics import EnricherMetrics


def test_metrics_starts_empty():
    m = EnricherMetrics()
    snap = m.snapshot()
    # Only the core fields when empty.
    assert snap["uptime_sec"] >= 0
    assert snap["latency_p50_ms"] == 0
    assert snap["latency_p95_ms"] == 0
    assert snap["latency_samples"] == 0


def test_metrics_increment_counters():
    m = EnricherMetrics()
    m.inc("events_in")
    m.inc("events_in", 4)
    m.inc("events_out")
    m.inc("errors.invalid_json", 2)

    snap = m.snapshot()
    assert snap["events_in"] == 5
    assert snap["events_out"] == 1
    assert snap["errors.invalid_json"] == 2


def test_metrics_latency_percentiles():
    m = EnricherMetrics()
    # Insert 100 samples: 1ms, 2ms, ..., 100ms
    for i in range(1, 101):
        m.record_latency_ms(float(i))

    snap = m.snapshot()
    assert snap["latency_samples"] == 100
    # p50 is around 50, p95 around 95
    assert 45 <= snap["latency_p50_ms"] <= 55
    assert 92 <= snap["latency_p95_ms"] <= 98


def test_metrics_latency_buffer_caps_at_1000():
    m = EnricherMetrics()
    for i in range(2000):
        m.record_latency_ms(float(i))
    # Only last 1000 retained
    assert m.snapshot()["latency_samples"] == 1000


def test_metrics_latency_ignores_negative():
    m = EnricherMetrics()
    m.record_latency_ms(-1.0)
    m.record_latency_ms(100.0)
    assert m.snapshot()["latency_samples"] == 1


def test_metrics_snapshot_returns_flat_dict():
    """Snapshot values must be int/float for heartbeat stringification."""
    m = EnricherMetrics()
    m.inc("events_in")
    m.record_latency_ms(50.0)
    snap = m.snapshot()
    for k, v in snap.items():
        assert isinstance(v, (int, float)), f"{k}={v} is {type(v)}"


def test_metrics_get_returns_counter():
    m = EnricherMetrics()
    m.inc("custom_metric", 42)
    assert m.get("custom_metric") == 42
    # missing → 0 (Counter behavior)
    assert m.get("never_incremented") == 0
