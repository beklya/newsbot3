"""Integration tests for BridgePipeline + PositionTracker + Redis state."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.contracts.execution_result import ExecutionResultEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.bridge.metrics import BridgeMetrics
from src.services.bridge.paper_executor import PaperExecutor
from src.services.bridge.pipeline import BridgePipeline
from src.services.bridge.position_tracker import PositionTracker


async def _read_stream(redis, stream: str) -> list[ExecutionResultEvent]:
    entries = await redis.xrange(stream, min="-", max="+")
    out = []
    for _mid, fields in entries:
        data = fields.get(b"data") or fields.get("data")
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        out.append(ExecutionResultEvent.model_validate_json(data))
    return out


async def _build_pipeline(fake_redis, bridge_settings, candle_cache_factory, **kw):
    cache = candle_cache_factory(**kw)
    publisher = StreamPublisher(fake_redis, stream=bridge_settings.trade_executions_stream)
    metrics = BridgeMetrics()
    executor = PaperExecutor(bridge_settings, cache)
    tracker = PositionTracker(
        settings=bridge_settings,
        executor=executor,
        redis=fake_redis,
        publisher=publisher,
        producer_name="bridge",
    )
    idem = IdempotencyGuard(fake_redis, ttl_seconds=300)
    return BridgePipeline(
        settings=bridge_settings, executor=executor,
        tracker=tracker, idem=idem, publisher=publisher, metrics=metrics,
    ), tracker, metrics


def _flat_open_with_tp_spike(n_bars: int = 600, spike_after_idx: int = 70,
                              spike_high: float = 102.5) -> list[float]:
    """Build custom_highs so all bars sit flat at 100.5 until spike_after_idx,
    then rise to spike_high to hit TP=102. With slope=0 in the factory, this
    keeps every bar.open at 100 (matching signal.entry_price=100, drift=0)
    while still triggering TP exit in PositionTracker."""
    return [100.5] * spike_after_idx + [spike_high] * (n_bars - spike_after_idx)


@pytest.mark.asyncio
async def test_execute_publishes_open_and_close(
    fake_redis, bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Happy path: signal → OPEN published, position tracked, CLOSE published."""
    signal_ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        entry_price=100.0, stop_loss=99.0, take_profit=102.0, quantity=10,
        produced_at=signal_ts.isoformat(timespec="milliseconds"),
    )
    pipeline, tracker, metrics = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        custom_highs=_flat_open_with_tp_spike(),  # entry bar opens at 100; later bar high=102.5 → TP hit
    )

    await pipeline.process(signal)
    # Дать tracker'у пару тиков чтобы поймать exit
    for _ in range(20):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break

    events = await _read_stream(fake_redis, bridge_settings.trade_executions_stream)
    statuses = [(e.payload.status, e.payload.exit_reason) for e in events]
    # Expect one OPEN (exit_reason=None) and one CLOSE (exit_reason set)
    assert any(s == ("FILLED", None) for s in statuses), f"missing OPEN: {statuses}"
    assert any(s[1] is not None for s in statuses), f"missing CLOSE: {statuses}"

    # Risk state cleaned up after close
    assert await fake_redis.scard(bridge_settings.risk_open_positions_key) == 0


@pytest.mark.asyncio
async def test_reject_signal_no_fill(
    fake_redis, bridge_settings, reject_signal_factory, candle_cache_factory,
):
    """REJECT signal → counter only, no execution_result published."""
    pipeline, _, metrics = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP", start=pd.Timestamp("2026-05-25 14:00:00"),
    )
    signal = reject_signal_factory(reason="test")
    await pipeline.process(signal)

    events = await _read_stream(fake_redis, bridge_settings.trade_executions_stream)
    assert events == []
    assert metrics.snapshot().get("rejects_seen", 0) == 1


@pytest.mark.asyncio
async def test_pnl_writeback(
    fake_redis, bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """После CLOSE — daily_pnl atomically INCRBYFLOAT."""
    signal_ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        entry_price=100.0, stop_loss=99.0, take_profit=102.0, quantity=10,
        produced_at=signal_ts.isoformat(timespec="milliseconds"),
    )
    pipeline, tracker, _ = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        custom_highs=_flat_open_with_tp_spike(),
    )

    await pipeline.process(signal)
    for _ in range(20):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw = await fake_redis.get(f"{bridge_settings.risk_daily_pnl_key_prefix}{today}")
    assert raw is not None
    pnl_val = float(raw.decode() if isinstance(raw, bytes) else raw)
    # TP hit на BUY с stretch ~2 руб × 10 lots × 10 qty = ~200 минус costs
    assert pnl_val != 0  # Should have moved


@pytest.mark.asyncio
async def test_cooldown_set_on_close(
    fake_redis, bridge_settings, execute_signal_factory, candle_cache_factory,
):
    pipeline, tracker, _ = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        custom_highs=_flat_open_with_tp_spike(),
    )
    signal = execute_signal_factory(
        ticker="GAZP", side="BUY",
        produced_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
                        .isoformat(timespec="milliseconds"),
    )
    await pipeline.process(signal)
    for _ in range(20):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break
    # cooldown set
    cooldown = await fake_redis.exists(
        f"{bridge_settings.risk_cooldown_key_prefix}GAZP",
    )
    assert cooldown == 1


@pytest.mark.asyncio
async def test_idempotency_skips_duplicate(
    fake_redis, bridge_settings, execute_signal_factory, candle_cache_factory,
):
    signal = execute_signal_factory(
        produced_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
                        .isoformat(timespec="milliseconds"),
    )
    pipeline, tracker, metrics = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        custom_highs=_flat_open_with_tp_spike(),
    )
    await pipeline.process(signal)
    await pipeline.process(signal)  # replay
    for _ in range(20):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break
    # Only one position should have been opened
    assert metrics.snapshot().get("opens_published", 0) == 1
    assert metrics.snapshot().get("events_skipped_idem", 0) == 1


@pytest.mark.asyncio
async def test_recovery_resumes_open_position(
    fake_redis, bridge_settings, execute_signal_factory, candle_cache_factory,
):
    """Persisted bridge:open_positions:* keys → tracker resumes on startup."""
    from src.services.bridge.paper_executor import OpenPosition

    pos = OpenPosition(
        signal_event_id="recovered-1", ticker="GAZP", side="BUY",
        entry_price=100.0, sl=99.0, tp=102.0, quantity=10,
        entry_ts_iso=datetime(2026, 5, 25, 11, 0, 0, tzinfo=timezone.utc)
            .isoformat(timespec="milliseconds"),
        horizon_min=60, notional_rub=1000.0,
    )
    # Pre-seed
    await fake_redis.set(
        f"{bridge_settings.bridge_open_position_prefix}{pos.signal_event_id}",
        pos.to_json(),
    )

    pipeline, tracker, _ = await _build_pipeline(
        fake_redis, bridge_settings, candle_cache_factory,
        ticker="GAZP",
        start=pd.Timestamp("2026-05-25 14:00:00"),
        close_base=100.0, slope=0.0,
        custom_highs=_flat_open_with_tp_spike(),
    )

    recovered = await tracker.recover_from_redis(parent_trace=[])
    assert recovered == 1
    assert tracker.active_count() == 1

    # Pump until close
    for _ in range(20):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break

    events = await _read_stream(fake_redis, bridge_settings.trade_executions_stream)
    assert any(e.payload.exit_reason is not None for e in events)
