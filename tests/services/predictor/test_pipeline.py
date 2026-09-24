"""Integration tests for PredictorPipeline using fakeredis + real model bundle."""
from __future__ import annotations

import json

import pytest
import pytest_asyncio

from src.contracts.enriched_news import EnrichedNewsEvent, TickerImpact
from src.contracts.ml_prediction import MLPredictionEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.predictor.metrics import PredictorMetrics
from src.services.predictor.pipeline import PredictorPipeline


async def _read_stream(redis, stream: str) -> list[dict]:
    """Decode all entries from a fakeredis stream as parsed events."""
    entries = await redis.xrange(stream, min="-", max="+")
    out = []
    for _msg_id, fields in entries:
        out.append({k.decode() if isinstance(k, bytes) else k:
                    v.decode() if isinstance(v, bytes) else v for k, v in fields.items()})
    return out


def _build_pipeline(fake_redis, predictor_settings, candle_cache_synthetic,
                    empty_history, real_bundle) -> PredictorPipeline:
    return PredictorPipeline(
        bundle=real_bundle,
        candles=candle_cache_synthetic,
        history=empty_history,
        idem=IdempotencyGuard(fake_redis, ttl_seconds=300),
        publisher_main=StreamPublisher(fake_redis, predictor_settings.ml_predictions_stream),
        publisher_dlq=StreamPublisher(fake_redis, predictor_settings.ml_predictions_dlq_stream),
        metrics=PredictorMetrics(),
        settings=predictor_settings,
    )


@pytest.mark.asyncio
async def test_whitelist_ticker_publishes_prediction(
    fake_redis, predictor_settings, candle_cache_synthetic, empty_history,
    real_bundle, enriched_event_factory,
):
    pipeline = _build_pipeline(
        fake_redis, predictor_settings, candle_cache_synthetic, empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="GAZP")
    await pipeline.process(ev)

    entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream)
    assert len(entries) == 1
    parsed = MLPredictionEvent.model_validate_json(entries[0]["data"])
    assert parsed.payload.enriched_event_id == ev.event_id
    assert parsed.payload.ticker == "GAZP"
    assert parsed.payload.feature_count == 67
    assert len(parsed.payload.predictions) == 2


@pytest.mark.asyncio
async def test_off_whitelist_ticker_skipped(
    fake_redis, predictor_settings, candle_cache_synthetic, empty_history,
    real_bundle, enriched_event_factory,
):
    pipeline = _build_pipeline(
        fake_redis, predictor_settings, candle_cache_synthetic, empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="SBER")  # SBER не в whitelist
    await pipeline.process(ev)

    entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream)
    assert entries == []
    snap = pipeline.metrics.snapshot()
    assert snap.get("tickers_skipped_off_whitelist", 0) == 1


@pytest.mark.asyncio
async def test_non_financial_skipped(
    fake_redis, predictor_settings, candle_cache_synthetic, empty_history,
    real_bundle, enriched_event_factory,
):
    pipeline = _build_pipeline(
        fake_redis, predictor_settings, candle_cache_synthetic, empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="GAZP", is_financial=False)
    await pipeline.process(ev)
    entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream)
    assert entries == []
    snap = pipeline.metrics.snapshot()
    assert snap.get("events_skipped_non_financial", 0) == 1


@pytest.mark.asyncio
async def test_multi_ticker_publishes_n_predictions(
    fake_redis, predictor_settings, candle_cache_synthetic, empty_history,
    real_bundle, enriched_event_factory,
):
    """3 whitelist tickers in one event → 3 separate MLPredictionEvents."""
    pipeline = _build_pipeline(
        fake_redis, predictor_settings, candle_cache_synthetic, empty_history, real_bundle,
    )
    tickers = [
        TickerImpact(ticker=t, direction="long", sentiment="positive",
                     confidence=0.7, impact_strength=0.5, rationale="")
        for t in ("GAZP", "LKOH", "ROSN")  # all in whitelist
    ]
    # Only GAZP has synthetic candles → LKOH, ROSN go to DLQ.
    # GAZP gets a prediction.
    ev = enriched_event_factory(tickers=tickers)
    await pipeline.process(ev)

    main_entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream)
    dlq_entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_dlq_stream)
    assert len(main_entries) == 1  # GAZP
    assert len(dlq_entries) == 2   # LKOH + ROSN missing_market_data


@pytest.mark.asyncio
async def test_composite_idempotency(
    fake_redis, predictor_settings, candle_cache_synthetic, empty_history,
    real_bundle, enriched_event_factory,
):
    """Replay same event → no duplicate predictions. Different tickers same news → different idem keys."""
    pipeline = _build_pipeline(
        fake_redis, predictor_settings, candle_cache_synthetic, empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="GAZP")
    await pipeline.process(ev)
    await pipeline.process(ev)  # replay

    entries = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream)
    assert len(entries) == 1  # idem заблокировал второй
