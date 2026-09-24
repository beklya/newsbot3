"""Sprint 6: tests for Decision stale_features REJECT gate."""
from __future__ import annotations

import pytest

from src.contracts.trade_signal import TradeSignalEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.decision.config import DecisionSettings
from src.services.decision.enrichment_cache import EnrichmentCache
from src.services.decision.metrics import DecisionMetrics
from src.services.decision.pipeline import DecisionPipeline
from src.services.decision.risk_manager import RiskManager
from .conftest import seed_enriched_cache


async def _read_signals(redis, settings: DecisionSettings) -> list[TradeSignalEvent]:
    entries = await redis.xrange(settings.trade_signals_stream, min="-", max="+")
    out = []
    for _msg_id, fields in entries:
        d = fields.get(b"data") or fields.get("data")
        if isinstance(d, bytes):
            d = d.decode("utf-8")
        out.append(TradeSignalEvent.model_validate_json(d))
    return out


def _build_pipeline(fake_redis, settings) -> DecisionPipeline:
    return DecisionPipeline(
        settings=settings,
        enrichment_cache=EnrichmentCache(
            fake_redis, key_prefix=settings.enrichment_cache_key_prefix,
        ),
        risk_manager=RiskManager(
            fake_redis,
            open_positions_key=settings.risk_open_positions_key,
            daily_pnl_key_prefix=settings.risk_daily_pnl_key_prefix,
            cooldown_key_prefix=settings.risk_cooldown_key_prefix,
            max_open_positions=settings.max_open_positions,
            daily_kill_pct=settings.daily_kill_pct,
            initial_equity_rub=settings.initial_equity_rub,
        ),
        idem=IdempotencyGuard(fake_redis, ttl_seconds=300),
        publisher=StreamPublisher(fake_redis, settings.trade_signals_stream),
        metrics=DecisionMetrics(),
    )


@pytest.mark.asyncio
async def test_stale_features_rejected_when_last_bar_too_old(
    fake_redis, decision_settings,
    enriched_event_factory, prediction_event_factory,
):
    """news_time = Monday 12:00 MSK (factory default), last_bar_time =
    April 20 (prod-bug shape). Gap ≈ 41 days >> 30 min → REJECT.
    """
    pipeline = _build_pipeline(fake_redis, decision_settings)
    ev_enriched = enriched_event_factory(
        event_id="01JEV1STALE000000ENRICHED",
        ticker="GAZP", direction="short", confidence=0.7,
    )
    await seed_enriched_cache(fake_redis, decision_settings, ev_enriched)
    ev_pred = prediction_event_factory(
        enriched_event_id=ev_enriched.event_id,
        ticker="GAZP",
        last_bar_time="2026-04-20T23:49:00",  # naive MSK, 6+ weeks before news_time
    )

    await pipeline.process(ev_pred)

    signals = await _read_signals(fake_redis, decision_settings)
    assert len(signals) == 1
    s = signals[0]
    assert s.payload.action == "REJECT"
    assert "stale_features" in s.payload.reject_reason
    snap = pipeline.metrics.snapshot()
    assert snap.get("rejects.stale_features", 0) == 1


@pytest.mark.asyncio
async def test_fresh_features_pass_stale_gate(
    fake_redis, decision_settings,
    enriched_event_factory, prediction_event_factory,
):
    """Factory defaults align news_time and last_bar_time → gap = 0 → pass."""
    pipeline = _build_pipeline(fake_redis, decision_settings)
    ev_enriched = enriched_event_factory(
        event_id="01JEV2FRESH00000ENRICHED",
        ticker="GAZP", direction="short", confidence=0.7,
    )
    await seed_enriched_cache(fake_redis, decision_settings, ev_enriched)
    ev_pred = prediction_event_factory(
        enriched_event_id=ev_enriched.event_id,
        ticker="GAZP",
        mfe_short_60m=0.5, mae_short_60m=0.2,
    )

    await pipeline.process(ev_pred)

    # Should NOT be REJECT(stale_features); could be EXECUTE or REJECT for
    # other reasons (B-filter etc.). Assert just the stale counter is 0.
    snap = pipeline.metrics.snapshot()
    assert snap.get("rejects.stale_features", 0) == 0


@pytest.mark.asyncio
async def test_stale_features_gate_off_when_threshold_zero(
    fake_redis,
    enriched_event_factory, prediction_event_factory,
):
    """stale_features_max_gap_sec=0 disables the check entirely."""
    settings = DecisionSettings(stale_features_max_gap_sec=0)
    pipeline = _build_pipeline(fake_redis, settings)
    ev_enriched = enriched_event_factory(
        event_id="01JEV3OFF0000000ENRICHED",
        ticker="GAZP", direction="short", confidence=0.7,
    )
    await seed_enriched_cache(fake_redis, settings, ev_enriched)
    ev_pred = prediction_event_factory(
        enriched_event_id=ev_enriched.event_id,
        ticker="GAZP",
        last_bar_time="2024-01-01T10:00:00",  # very stale — would normally trip
    )

    await pipeline.process(ev_pred)

    snap = pipeline.metrics.snapshot()
    assert snap.get("rejects.stale_features", 0) == 0
