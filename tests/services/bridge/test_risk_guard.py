"""RiskGuard — offline-тесты safety-слоя (без QUIK, без Redis где можно)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.services.bridge.config import BridgeSettings
from src.services.bridge.risk_guard import RiskGuard, MSK


@pytest.fixture
def guard(tmp_path):
    s = BridgeSettings(prices_dir=tmp_path, max_qty_per_order=1, max_orders_per_min=3,
                       max_entry_drift_pct=0.01, kill_switch_key="/nonexistent/kill")
    return RiskGuard(s, redis=None)


def test_qty_cap(guard):
    assert guard.clamp_qty(1) == (1, False)
    assert guard.clamp_qty(5) == (1, True)   # жёсткий потолок 1 лот


def test_price_collar(guard):
    assert guard.check_price_collar(250.0, 250.5) is None          # 0.2% ok
    assert guard.check_price_collar(250.0, 260.0) is not None      # 4% reject
    assert guard.check_price_collar(None, 260.0) is None           # нет reference


def test_rate_limit(guard):
    t = 1000.0
    for _ in range(3):
        assert guard.check_rate_limit(t) is None
        guard.register_sent(t)
    assert guard.check_rate_limit(t) is not None                   # 4-я за минуту
    assert guard.check_rate_limit(t + 61) is None                  # окно сдвинулось


def test_trading_hours(guard):
    wed_open = datetime(2026, 5, 27, 12, 0, tzinfo=MSK)            # среда 12:00
    sat = datetime(2026, 5, 30, 12, 0, tzinfo=MSK)                 # суббота
    night = datetime(2026, 5, 27, 23, 0, tzinfo=MSK)
    assert guard.check_trading_hours(wed_open) is None
    assert guard.check_trading_hours(sat) is not None
    assert guard.check_trading_hours(night) is not None


@pytest.mark.asyncio
async def test_kill_switch_file(tmp_path):
    kill = tmp_path / "kill"
    s = BridgeSettings(prices_dir=tmp_path, kill_switch_key=str(kill))
    g = RiskGuard(s, redis=None)
    assert await g.check_kill_switch() is None
    kill.write_text("stop")
    assert await g.check_kill_switch() is not None


@pytest.mark.asyncio
async def test_gate_combined_ok(guard):
    wed = datetime(2026, 5, 27, 12, 0, tzinfo=MSK)
    reason = await guard.gate(ref_price=250.0, current_price=250.3, qty=1,
                              now=0.0, now_msk=wed)
    assert reason is None


@pytest.mark.asyncio
async def test_gate_rejects_drift(guard):
    wed = datetime(2026, 5, 27, 12, 0, tzinfo=MSK)
    reason = await guard.gate(ref_price=250.0, current_price=300.0, qty=1,
                              now=0.0, now_msk=wed)
    assert reason and "price_collar" in reason
