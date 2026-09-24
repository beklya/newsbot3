"""Tests for GroqKeyPool — round-robin and cooldown logic."""
from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from src.services.enricher.key_pool import GroqKeyPool, DEFAULT_COOLDOWN_SEC


@pytest.fixture
def pool(monkeypatch):
    """Pool with 3 fake AsyncGroq clients."""
    real_clients = [MagicMock(name=f"c{i}") for i in range(3)]
    # Принимаем любые kwargs (api_key, timeout, max_retries, ...)
    monkeypatch.setattr(
        "src.services.enricher.key_pool.AsyncGroq",
        lambda **kwargs: real_clients.pop(0),
    )
    p = GroqKeyPool(["k1", "k2", "k3"], timeout_sec=5.0)
    return p


@pytest.mark.asyncio
async def test_pool_round_robin(pool):
    """Acquire должен возвращать клиентов по кругу."""
    c1 = await pool.acquire()
    c2 = await pool.acquire()
    c3 = await pool.acquire()
    c4 = await pool.acquire()  # round-robin back to first
    assert c1 is not c2
    assert c2 is not c3
    assert c4 is c1


@pytest.mark.asyncio
async def test_pool_skips_cooling_down_keys(pool):
    """Если ключ на cooldown — acquire берёт следующий."""
    c1 = await pool.acquire()
    pool.mark_rate_limited(c1, retry_after_sec=10.0)

    # Следующий acquire должен дать не c1
    c2 = await pool.acquire()
    assert c2 is not c1


@pytest.mark.asyncio
async def test_pool_waits_when_all_cooling_down(pool, monkeypatch):
    """Если все на cooldown — ждём, пока освободится первый."""
    # Все 3 ключа помечаем как cooling down с маленьким сроком
    clients = []
    for _ in range(3):
        c = await pool.acquire()
        clients.append(c)
        pool.mark_rate_limited(c, retry_after_sec=0.1)

    # asyncio.sleep — патчим, чтобы не ждать реально
    sleep_calls = []
    real_sleep = asyncio.sleep

    async def fake_sleep(s):
        sleep_calls.append(s)
        # На самом деле ждём — но коротко, чтобы cooldown истёк
        await real_sleep(s)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    start = time.monotonic()
    c = await pool.acquire()
    elapsed = time.monotonic() - start
    # Должны были подождать хотя бы 0.1 сек
    assert elapsed >= 0.05
    assert len(sleep_calls) >= 1
    assert c is not None


def test_pool_requires_at_least_one_key():
    with pytest.raises(ValueError, match="at least one"):
        GroqKeyPool([])


@pytest.mark.asyncio
async def test_mark_rate_limited_uses_default_cooldown(pool):
    """Без retry_after_sec используется DEFAULT_COOLDOWN_SEC."""
    c = await pool.acquire()
    pool.mark_rate_limited(c, retry_after_sec=None)
    # Внутреннее состояние: cooldown_until > now + 50 сек
    slot = next(s for s in pool._slots if s.client is c)
    assert slot.cooldown_until > time.monotonic() + DEFAULT_COOLDOWN_SEC - 1


@pytest.mark.asyncio
async def test_mark_rate_limited_for_unknown_client_safe(pool, caplog):
    """Если передали клиента не из пула — логируем error, не падаем."""
    import logging
    caplog.set_level(logging.ERROR)
    fake = MagicMock()
    pool.mark_rate_limited(fake, retry_after_sec=5.0)
    assert any("unknown client" in rec.message.lower() for rec in caplog.records)


def test_pool_size_property():
    real_clients = [MagicMock() for _ in range(2)]
    import src.services.enricher.key_pool as kp
    # Простейшее построение без monkeypatch
    pool = kp.GroqKeyPool.__new__(kp.GroqKeyPool)
    from src.services.enricher.key_pool import _KeySlot
    pool._slots = [
        _KeySlot(key_id=0, client=real_clients[0]),
        _KeySlot(key_id=1, client=real_clients[1]),
    ]
    pool._cursor = 0
    assert pool.size == 2


@pytest.mark.asyncio
async def test_pool_stats_reflects_cooldown(pool):
    """stats() показывает n_ready / n_cooldown корректно."""
    s0 = pool.stats()
    assert s0["n_total"] == 3
    assert s0["n_ready"] == 3
    assert s0["n_cooldown"] == 0
    assert s0["earliest_ready_in_sec"] == 0.0

    # Помечаем один ключ как cooling down
    c = await pool.acquire()
    pool.mark_rate_limited(c, retry_after_sec=10.0)

    s1 = pool.stats()
    assert s1["n_ready"] == 2
    assert s1["n_cooldown"] == 1
    assert 0.0 < s1["earliest_ready_in_sec"] <= 10.0
