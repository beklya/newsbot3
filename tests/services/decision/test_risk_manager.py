"""Tests for RiskManager Redis-backed gates."""
from __future__ import annotations

import pytest

from src.services.decision.risk_manager import RiskManager


def _build(redis, *, max_open=3, kill_pct=0.02, equity=500_000.0):
    return RiskManager(
        redis=redis,
        open_positions_key="risk:open_positions",
        daily_pnl_key_prefix="risk:daily_pnl:",
        cooldown_key_prefix="risk:cooldown:",
        max_open_positions=max_open,
        daily_kill_pct=kill_pct,
        initial_equity_rub=equity,
    )


@pytest.mark.asyncio
async def test_open_positions_count_empty(fake_redis):
    rm = _build(fake_redis)
    assert await rm.open_positions_count() == 0


@pytest.mark.asyncio
async def test_open_positions_count_filled(fake_redis):
    rm = _build(fake_redis)
    await fake_redis.sadd("risk:open_positions", b"GAZP", b"LKOH")
    assert await rm.open_positions_count() == 2


@pytest.mark.asyncio
async def test_cooldown_inactive(fake_redis):
    rm = _build(fake_redis)
    assert await rm.is_cooldown_active("GAZP") is False


@pytest.mark.asyncio
async def test_cooldown_active(fake_redis):
    rm = _build(fake_redis)
    await fake_redis.set("risk:cooldown:GAZP", b"1", ex=60)
    assert await rm.is_cooldown_active("GAZP") is True


@pytest.mark.asyncio
async def test_daily_pnl_zero_default(fake_redis):
    rm = _build(fake_redis)
    assert await rm.daily_pnl_rub() == 0.0


@pytest.mark.asyncio
async def test_daily_kill_below_threshold(fake_redis):
    rm = _build(fake_redis, kill_pct=0.02, equity=500_000.0)
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await fake_redis.set(f"risk:daily_pnl:{today}", b"-5000")  # -1%
    assert await rm.is_daily_kill_triggered() is False


@pytest.mark.asyncio
async def test_daily_kill_triggered(fake_redis):
    rm = _build(fake_redis, kill_pct=0.02, equity=500_000.0)
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await fake_redis.set(f"risk:daily_pnl:{today}", b"-10000")  # -2% = на грани
    assert await rm.is_daily_kill_triggered() is True


@pytest.mark.asyncio
async def test_daily_kill_triggers_on_positive_too(fake_redis):
    """abs(daily_pnl_pct) ≥ kill_pct — kill срабатывает и на огромном плюсе."""
    rm = _build(fake_redis, kill_pct=0.02, equity=500_000.0)
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await fake_redis.set(f"risk:daily_pnl:{today}", b"15000")  # +3%
    assert await rm.is_daily_kill_triggered() is True
