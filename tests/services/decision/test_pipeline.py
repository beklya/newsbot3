"""Integration tests for DecisionPipeline на fakeredis."""
from __future__ import annotations

import pytest

from src.contracts.trade_signal import TradeSignalEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.decision.enrichment_cache import EnrichmentCache
from src.services.decision.metrics import DecisionMetrics
from src.services.decision.pipeline import DecisionPipeline
from src.services.decision.risk_manager import RiskManager

from .conftest import seed_enriched_cache


async def _read_stream(redis, stream: str) -> list[TradeSignalEvent]:
    entries = await redis.xrange(stream, min="-", max="+")
    out = []
    for _msg_id, fields in entries:
        data = fields.get(b"data") or fields.get("data")
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        out.append(TradeSignalEvent.model_validate_json(data))
    return out


def _build_pipeline(fake_redis, decision_settings):
    cache = EnrichmentCache(fake_redis, key_prefix=decision_settings.enrichment_cache_key_prefix)
    risk = RiskManager(
        redis=fake_redis,
        open_positions_key=decision_settings.risk_open_positions_key,
        daily_pnl_key_prefix=decision_settings.risk_daily_pnl_key_prefix,
        cooldown_key_prefix=decision_settings.risk_cooldown_key_prefix,
        max_open_positions=decision_settings.max_open_positions,
        daily_kill_pct=decision_settings.daily_kill_pct,
        initial_equity_rub=decision_settings.initial_equity_rub,
    )
    return DecisionPipeline(
        settings=decision_settings,
        enrichment_cache=cache,
        risk_manager=risk,
        idem=IdempotencyGuard(fake_redis, ttl_seconds=300),
        publisher=StreamPublisher(fake_redis, stream=decision_settings.trade_signals_stream),
        metrics=DecisionMetrics(),
    )


@pytest.mark.asyncio
async def test_execute_path_happy(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    """LLM long + prediction long + risk clear → EXECUTE."""
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,  # rr=2.5
        mfe_short_60m=0.1, mae_short_60m=0.3,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)

    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)

    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.payload.action == "EXECUTE"
    assert sig.payload.side == "BUY"
    assert sig.payload.ticker == "GAZP"
    assert sig.payload.rr_ratio is not None
    assert sig.payload.rr_ratio >= 2.0
    assert sig.payload.quantity is not None and sig.payload.quantity >= 1


@pytest.mark.asyncio
async def test_reject_direction_mismatch(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    """LLM short, but R:R picks BUY → REJECT (direction mismatch)."""
    enriched = enriched_event_factory(direction="short", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,  # rr_long=2.5
        mfe_short_60m=0.1, mae_short_60m=0.3,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)

    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)

    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert len(signals) == 1
    assert signals[0].payload.action == "REJECT"
    assert "direction" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_reject_confidence_low(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    enriched = enriched_event_factory(direction="long", confidence=0.4)  # < 0.5
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals[0].payload.action == "REJECT"
    assert "confidence" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_reject_rr_below_threshold(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.05, mae_long_60m=0.20,  # Sprint 6.2: rr=0.25 < 1.0 default
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals[0].payload.action == "REJECT"
    assert "R:R" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_cache_miss_skips_no_publish(
    fake_redis, decision_settings, prediction_event_factory,
):
    """Decision не публикует ничего если EnrichmentCache miss (race condition)."""
    pred = prediction_event_factory()  # cache не заполнен
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals == []
    assert pipeline.metrics.snapshot().get("errors.enrichment_missing", 0) == 1


@pytest.mark.asyncio
async def test_reject_max_open_positions(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    # Pre-fill open positions
    await fake_redis.sadd(
        decision_settings.risk_open_positions_key, b"LKOH", b"ROSN", b"NVTK",
    )
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals[0].payload.action == "REJECT"
    assert "max_open" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_reject_cooldown(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    await fake_redis.set(
        f"{decision_settings.risk_cooldown_key_prefix}GAZP", b"1", ex=60,
    )
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals[0].payload.action == "REJECT"
    assert "cooldown" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_reject_daily_kill(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    await fake_redis.set(
        f"{decision_settings.risk_daily_pnl_key_prefix}{today}", b"-15000",  # -3%
    )
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert signals[0].payload.action == "REJECT"
    assert "daily_kill" in signals[0].payload.reject_reason


@pytest.mark.asyncio
async def test_idempotency_dedup(
    fake_redis, decision_settings, enriched_event_factory, prediction_event_factory,
):
    """Replay same MLPredictionEvent → не дубль."""
    enriched = enriched_event_factory(direction="long", confidence=0.7)
    pred = prediction_event_factory(
        enriched_event_id=enriched.event_id,
        mfe_long_60m=0.5, mae_long_60m=0.2,
    )
    await seed_enriched_cache(fake_redis, decision_settings, enriched)
    pipeline = _build_pipeline(fake_redis, decision_settings)
    await pipeline.process(pred)
    await pipeline.process(pred)  # replay
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream)
    assert len(signals) == 1
