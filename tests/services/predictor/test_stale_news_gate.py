"""Sprint 6: tests for Predictor stale_candles_at_news_time gate.

Reproduces 2026-06-01 production bug: news arrived during candle-cache gap
(historical CSV ended April 20, live data started today 12:31, news_time
fell at 12:28 today). Predictor used April-20 close as last_close while
Bridge filled on today's bar => catastrophic SL/TP inversion.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from src.contracts.ml_prediction import MLPredictionEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.predictor.candle_cache import CandleCache
from src.services.predictor.config import PredictorSettings
from src.services.predictor.metrics import PredictorMetrics
from src.services.predictor.pipeline import PredictorPipeline


def _cache_ending_n_hours_ago(hours_before_now: float) -> CandleCache:
    """Build a CandleCache whose last bar lies `hours_before_now` hours
    before wall-clock UTC now. Used to reproduce the 2026-06-01 bug shape
    where Predictor's last bar at news_time is far older than news_time."""
    cache = CandleCache(Path("nowhere"))
    now_utc = datetime.now(timezone.utc).replace(microsecond=0, second=0, tzinfo=None)
    # MSK naive = UTC + 3h
    end_msk = pd.Timestamp(now_utc) + pd.Timedelta(hours=3) - pd.Timedelta(hours=hours_before_now)
    start = (end_msk - pd.Timedelta(hours=10)).floor("min")
    n = 600
    closes = np.full(n, 100.0)
    df = pd.DataFrame({
        "open": closes, "high": closes + 0.5, "low": closes - 0.5,
        "close": closes, "volume": np.full(n, 1000.0),
    }, index=pd.date_range(start, periods=n, freq="1min"))
    df.index.name = "ts"
    cache._candles["GAZP"] = df
    return cache


async def _read_stream(redis, stream: str) -> list[dict]:
    entries = await redis.xrange(stream, min="-", max="+")
    out = []
    for _msg_id, fields in entries:
        out.append({k.decode() if isinstance(k, bytes) else k:
                    v.decode() if isinstance(v, bytes) else v for k, v in fields.items()})
    return out


def _pipeline_with_gate(
    fake_redis, predictor_settings_with_gate, candle_cache_synthetic,
    empty_history, real_bundle,
) -> PredictorPipeline:
    return PredictorPipeline(
        bundle=real_bundle,
        candles=candle_cache_synthetic,
        history=empty_history,
        idem=IdempotencyGuard(fake_redis, ttl_seconds=300),
        publisher_main=StreamPublisher(
            fake_redis, predictor_settings_with_gate.ml_predictions_stream,
        ),
        publisher_dlq=StreamPublisher(
            fake_redis, predictor_settings_with_gate.ml_predictions_dlq_stream,
        ),
        metrics=PredictorMetrics(),
        settings=predictor_settings_with_gate,
    )


@pytest.fixture
def predictor_settings_with_gate(predictor_settings) -> PredictorSettings:
    """Re-enable the Sprint 6 stale-news gate (default conftest disables it)."""
    return predictor_settings.model_copy(update={"max_news_to_last_bar_gap_sec": 1800})


@pytest.mark.asyncio
async def test_stale_news_gate_skips_prediction_when_gap_too_large(
    fake_redis, predictor_settings_with_gate,
    empty_history, real_bundle, enriched_event_factory,
):
    """Reproduce 2026-06-01 bug shape: cache ends 2h before news_time.

    Default event produced_at = utcnow. We build a cache whose last bar
    is 2 hours older than now → gap = 7200s >> 1800s threshold → skip.
    """
    stale_cache = _cache_ending_n_hours_ago(hours_before_now=2.0)
    pipeline = _pipeline_with_gate(
        fake_redis, predictor_settings_with_gate, stale_cache,
        empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="GAZP")  # default produced_at=utcnow
    await pipeline.process(ev)
    entries = await _read_stream(
        fake_redis, predictor_settings_with_gate.ml_predictions_stream,
    )
    assert entries == []  # no prediction published
    snap = pipeline.metrics.snapshot()
    assert snap.get("errors.stale_candles_at_news_time", 0) == 1


@pytest.mark.asyncio
async def test_fresh_news_passes_gate_normally(
    fake_redis, predictor_settings_with_gate, candle_cache_synthetic,
    empty_history, real_bundle, enriched_event_factory,
):
    """Default factory uses produced_at=utcnow; cache is anchored to NOW too
    → gap ≈ 0 → gate passes, prediction published."""
    pipeline = _pipeline_with_gate(
        fake_redis, predictor_settings_with_gate, candle_cache_synthetic,
        empty_history, real_bundle,
    )
    ev = enriched_event_factory(ticker="GAZP")  # default produced_at=utcnow
    await pipeline.process(ev)
    entries = await _read_stream(
        fake_redis, predictor_settings_with_gate.ml_predictions_stream,
    )
    assert len(entries) == 1
    parsed = MLPredictionEvent.model_validate_json(entries[0]["data"])
    assert parsed.payload.ticker == "GAZP"


@pytest.mark.asyncio
async def test_stale_news_gate_disabled_with_huge_threshold(
    fake_redis, predictor_settings, candle_cache_synthetic,
    empty_history, real_bundle, enriched_event_factory,
):
    """With a huge max_gap_sec, even far-past events go through."""
    settings_disabled = predictor_settings.model_copy(update={
        "max_news_to_last_bar_gap_sec": 10**9,
    })
    # Same stale cache shape as the trigger test, but gate disabled.
    stale_cache = _cache_ending_n_hours_ago(hours_before_now=2.0)
    pipeline = PredictorPipeline(
        bundle=real_bundle,
        candles=stale_cache,
        history=empty_history,
        idem=IdempotencyGuard(fake_redis, ttl_seconds=300),
        publisher_main=StreamPublisher(
            fake_redis, settings_disabled.ml_predictions_stream,
        ),
        publisher_dlq=StreamPublisher(
            fake_redis, settings_disabled.ml_predictions_dlq_stream,
        ),
        metrics=PredictorMetrics(),
        settings=settings_disabled,
    )
    ev = enriched_event_factory(ticker="GAZP")
    await pipeline.process(ev)
    entries = await _read_stream(
        fake_redis, settings_disabled.ml_predictions_stream,
    )
    assert len(entries) == 1  # gate skipped, prediction published as before
