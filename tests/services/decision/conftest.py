"""Shared fixtures for Decision tests.

Time anchor: every event in these tests is anchored to **Monday 2026-06-01
09:00 UTC = 12:00 MSK** — a weekday inside MOEX trading hours, so that the
market-hours gate passes. last_bar_time on predictions is set to the same
moment (MSK naive) so the stale-features gate passes too. Tests that
exercise the gates explicitly override these.
"""
from __future__ import annotations

import pytest
import pytest_asyncio
import fakeredis.aioredis

# Anchor — Monday 12:00 MSK (= 09:00 UTC), middle of main MOEX session.
_DEFAULT_PRODUCED_AT_UTC = "2026-06-01T09:00:00.000+00:00"
# Naive MSK matching the anchor moment (UTC + 3h).
_DEFAULT_LAST_BAR_TIME_MSK = "2026-06-01T12:00:00"

from src.contracts.enriched_news import (
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)
from src.contracts.ml_prediction import (
    MLPredictionEvent,
    MLPredictionPayload,
    MLPredictionPerHorizon,
)
from src.services.decision.config import DecisionSettings


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


@pytest.fixture
def decision_settings() -> DecisionSettings:
    """Default settings — Phase 2 winning config + production Sprint-6 gates.

    Tests get production-realistic gates ON. To make this work, fixture
    factories anchor every event timestamp to Monday 2026-06-01 09:00 UTC
    (= 12:00 MSK), inside MOEX trading hours, and the prediction factory
    sets last_bar_time to match — so default gate checks pass naturally.
    """
    return DecisionSettings()


@pytest.fixture
def enriched_event_factory():
    def _factory(
        *,
        event_id: str = "01JEVENT00000ENRICHED01",
        ticker: str = "GAZP",
        direction: str = "long",
        sentiment: str = "positive",
        confidence: float = 0.7,
        tickers: list[TickerImpact] | None = None,
        produced_at: str = _DEFAULT_PRODUCED_AT_UTC,
    ) -> EnrichedNewsEvent:
        if tickers is None:
            tickers = [TickerImpact(
                ticker=ticker, direction=direction, sentiment=sentiment,
                confidence=confidence, impact_strength=0.5, rationale="",
            )]
        payload = EnrichedNewsPayload(
            raw_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
            llm_provider="groq",
            llm_model="llama-3.3-70b-versatile",
            llm_latency_ms=500.0,
            llm_input_tokens=100,
            llm_output_tokens=50,
            prompt_version="1.0.0",
            is_financial=True,
            tickers=tickers,
            summary="x",
            expected_timeframe="medium",
            urgency="medium",
            category="corporate",
            is_actionable=True,
            llm_raw_response="{}",
        )
        return EnrichedNewsEvent(
            event_id=event_id, produced_at=produced_at, payload=payload,
        )

    return _factory


@pytest.fixture
def prediction_event_factory():
    """Build MLPredictionEvent с конкретными MFE/MAE на каждый horizon."""

    def _factory(
        *,
        event_id: str = "01JEVENT00000PREDICT0001",
        enriched_event_id: str = "01JEVENT00000ENRICHED01",
        ticker: str = "GAZP",
        last_close: float = 100.0,
        mfe_long_60m: float = 0.5,  # %
        mae_long_60m: float = 0.2,
        mfe_short_60m: float = 0.2,
        mae_short_60m: float = 0.5,
        mfe_long_30m: float = 0.4,
        mae_long_30m: float = 0.15,
        mfe_short_30m: float = 0.15,
        mae_short_30m: float = 0.4,
        produced_at: str = _DEFAULT_PRODUCED_AT_UTC,
        news_time: str | None = _DEFAULT_PRODUCED_AT_UTC,
        last_bar_time: str = _DEFAULT_LAST_BAR_TIME_MSK,
    ) -> MLPredictionEvent:
        preds = [
            MLPredictionPerHorizon(
                horizon="30m",
                predicted_mfe_long_pct=mfe_long_30m,
                predicted_mae_long_pct=mae_long_30m,
                predicted_mfe_short_pct=mfe_short_30m,
                predicted_mae_short_pct=mae_short_30m,
                rr_long=mfe_long_30m / max(mae_long_30m, 0.05),
                rr_short=mfe_short_30m / max(mae_short_30m, 0.05),
            ),
            MLPredictionPerHorizon(
                horizon="60m",
                predicted_mfe_long_pct=mfe_long_60m,
                predicted_mae_long_pct=mae_long_60m,
                predicted_mfe_short_pct=mfe_short_60m,
                predicted_mae_short_pct=mae_short_60m,
                rr_long=mfe_long_60m / max(mae_long_60m, 0.05),
                rr_short=mfe_short_60m / max(mae_short_60m, 0.05),
            ),
        ]
        payload = MLPredictionPayload(
            enriched_event_id=enriched_event_id,
            ticker=ticker,
            features_built_at=produced_at,
            features_hash="0" * 64,
            feature_count=67,
            news_time=news_time,
            last_bar_time=last_bar_time,
            last_close=last_close,
            predictions=preds,
            inference_latency_ms=12.0,
            model_version="test-model",
        )
        return MLPredictionEvent(
            event_id=event_id, produced_at=produced_at, payload=payload,
        )

    return _factory


async def seed_enriched_cache(redis, settings: DecisionSettings, event: EnrichedNewsEvent) -> None:
    """Helper: pretend the Enricher SETEX'd this event into the cache."""
    key = f"{settings.enrichment_cache_key_prefix}{event.event_id}"
    await redis.set(key, event.model_dump_json(), ex=settings.enrichment_cache_ttl_sec)
