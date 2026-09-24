"""Sprint 5.4 Enricher upgrades — SETEX cache side effect + 403 fallback model."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from src.contracts.enriched_news import EnrichedNewsEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.enricher.config import EnricherSettings
from src.services.enricher.metrics import EnricherMetrics
from src.services.enricher.pipeline import EnrichmentPipeline


def test_default_groq_model_is_70b():
    """Sprint 5.4 code default switched 8b → 70b (Sprint 4.10 winner).

    Проверяем default'ы класса, не fixture instance (там .env переопределяет
    значения). .env update — отдельный 5.5 pre-flight шаг.
    """
    fields = EnricherSettings.model_fields
    assert fields["groq_model"].default == "llama-3.3-70b-versatile"
    assert fields["groq_fallback_model"].default == "llama-3.1-8b-instant"


def test_enrichment_cache_settings_defaults():
    """SETEX side effect config: prefix + TTL."""
    fields = EnricherSettings.model_fields
    assert fields["enrichment_cache_key_prefix"].default == "enriched:"
    assert fields["enrichment_cache_ttl_sec"].default == 300


@pytest.mark.asyncio
async def test_setex_cache_side_effect(fake_redis, enricher_settings, raw_event_factory, mock_llm_ok):
    """After publish enriched, SETEX enriched:<event_id> JSON в Redis."""
    idem = IdempotencyGuard(fake_redis, ttl_seconds=60)
    pub_main = StreamPublisher(fake_redis, stream=enricher_settings.enriched_news_stream)
    pub_dlq = StreamPublisher(fake_redis, stream=enricher_settings.enriched_news_dlq_stream)
    llm = AsyncMock()
    llm.enrich = AsyncMock(return_value=mock_llm_ok())
    pipeline = EnrichmentPipeline(
        llm=llm, idem=idem,
        publisher_main=pub_main, publisher_dlq=pub_dlq,
        metrics=EnricherMetrics(), settings=enricher_settings,
    )
    raw = raw_event_factory(text="x")
    await pipeline.process(raw)

    # Cache key должен существовать
    cache_key = f"{enricher_settings.enrichment_cache_key_prefix}{raw.event_id}"
    cached_raw = await fake_redis.get(cache_key)
    assert cached_raw is not None
    # Парсится в EnrichedNewsEvent
    if isinstance(cached_raw, bytes):
        cached_raw = cached_raw.decode("utf-8")
    parsed = EnrichedNewsEvent.model_validate_json(cached_raw)
    assert parsed.event_id == raw.event_id

    # TTL установлен
    ttl = await fake_redis.ttl(cache_key)
    assert 0 < ttl <= enricher_settings.enrichment_cache_ttl_sec
