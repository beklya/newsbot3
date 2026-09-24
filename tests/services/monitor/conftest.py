"""Shared fixtures for Monitor tests."""
from __future__ import annotations

import pytest
import pytest_asyncio
import fakeredis.aioredis

from src.services.monitor.config import MonitorSettings


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


@pytest.fixture
def monitor_settings() -> MonitorSettings:
    return MonitorSettings(
        missing_heartbeat_threshold_sec=90,
        dlq_spike_threshold=5,
        dlq_window_sec=300,
        # Тесты эмулируют установившийся pipeline без cold start grace.
        # Grace period проверяется отдельным test'ом ниже в test_pipeline.py.
        startup_grace_sec=0,
    )
