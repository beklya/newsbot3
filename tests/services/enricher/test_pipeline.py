"""Integration tests for EnrichmentPipeline using fakeredis."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from src.contracts.enriched_news import EnrichedNewsEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.enricher.llm_client import EnrichErrorKind
from src.services.enricher.metrics import EnricherMetrics
from src.services.enricher.pipeline import EnrichmentPipeline, EnrichmentRetryable


@pytest_asyncio.fixture
async def pipeline_setup(fake_redis, enricher_settings):
    """Build a real pipeline wired to fakeredis. LLM is the only thing mocked."""
    idem = IdempotencyGuard(fake_redis, ttl_seconds=60)
    pub_main = StreamPublisher(
        fake_redis,
        stream=enricher_settings.enriched_news_stream,
        maxlen=1_000,
    )
    pub_dlq = StreamPublisher(
        fake_redis,
        stream=enricher_settings.enriched_news_dlq_stream,
        maxlen=1_000,
    )
    metrics = EnricherMetrics()

    llm = AsyncMock()  # replaced per test via .enrich = AsyncMock(return_value=...)

    pipeline = EnrichmentPipeline(
        llm=llm,
        idem=idem,
        publisher_main=pub_main,
        publisher_dlq=pub_dlq,
        metrics=metrics,
        settings=enricher_settings,
    )
    return {
        "pipeline": pipeline,
        "redis": fake_redis,
        "metrics": metrics,
        "llm": llm,
        "settings": enricher_settings,
    }


# =========================================================================
# Happy path
# =========================================================================

@pytest.mark.asyncio
async def test_pipeline_success_publishes_to_main_stream(
    pipeline_setup, raw_event_factory, mock_llm_ok,
):
    p = pipeline_setup
    raw = raw_event_factory(text="ЦБ повысил ставку")
    p["llm"].enrich = AsyncMock(return_value=mock_llm_ok())

    await p["pipeline"].process(raw)

    # Main stream got 1 message, DLQ empty
    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 1
    assert await p["redis"].xlen(p["settings"].enriched_news_dlq_stream) == 0

    # Counters
    snap = p["metrics"].snapshot()
    assert snap["events_in"] == 1
    assert snap["events_out"] == 1
    assert snap.get("dlq_total", 0) == 0


@pytest.mark.asyncio
async def test_pipeline_preserves_event_id_across_streams(
    pipeline_setup, raw_event_factory, mock_llm_ok,
):
    """EnrichedNewsEvent.event_id == RawNewsEvent.event_id — для трассировки цепочки."""
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_ok())

    await p["pipeline"].process(raw)

    msgs = await p["redis"].xrange(p["settings"].enriched_news_stream)
    assert len(msgs) == 1
    _, fields = msgs[0]
    # Publisher writes event_id as separate field
    assert fields[b"event_id"].decode() == raw.event_id

    # And inside the JSON payload too
    data = fields[b"data"].decode()
    enriched = EnrichedNewsEvent.model_validate_json(data)
    assert enriched.event_id == raw.event_id
    assert enriched.schema_version == "1.1.0"
    assert enriched.producer == "enricher"


@pytest.mark.asyncio
async def test_pipeline_propagates_trace(
    pipeline_setup, raw_event_factory, mock_llm_ok,
):
    """Trace from raw_event should be preserved + extended by Publisher."""
    p = pipeline_setup
    raw = raw_event_factory()
    raw_with_trace = raw.add_trace("receiver", latency_ms=10.0)
    p["llm"].enrich = AsyncMock(return_value=mock_llm_ok())

    await p["pipeline"].process(raw_with_trace)

    msgs = await p["redis"].xrange(p["settings"].enriched_news_stream)
    enriched = EnrichedNewsEvent.model_validate_json(msgs[0][1][b"data"].decode())
    # Trace should contain at least the receiver step + the publisher's xadd step
    trace_services = [step["service"] for step in enriched.trace]
    assert "receiver" in trace_services
    # publisher added its stream name
    assert p["settings"].enriched_news_stream in trace_services


# =========================================================================
# Idempotency
# =========================================================================

@pytest.mark.asyncio
async def test_pipeline_idempotency_skips_duplicate(
    pipeline_setup, raw_event_factory, mock_llm_ok,
):
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_ok())

    # First call → processed
    await p["pipeline"].process(raw)
    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 1

    # Second call with SAME event → idempotency drops it
    await p["pipeline"].process(raw)
    # Stream length unchanged
    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 1
    # LLM was called only once
    assert p["llm"].enrich.call_count == 1

    snap = p["metrics"].snapshot()
    assert snap["events_in"] == 2
    assert snap["events_skipped_idem"] == 1
    assert snap["events_out"] == 1


# =========================================================================
# DLQ path — non-retryable errors
# =========================================================================

@pytest.mark.asyncio
async def test_pipeline_invalid_json_does_not_dlq_when_retryable(
    pipeline_setup, raw_event_factory, mock_llm_err,
):
    """INVALID_JSON retryable=True → raises, no DLQ."""
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_err(
        EnrichErrorKind.INVALID_JSON, retryable=True,
    ))

    with pytest.raises(EnrichmentRetryable):
        await p["pipeline"].process(raw)

    # No publish anywhere
    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 0
    assert await p["redis"].xlen(p["settings"].enriched_news_dlq_stream) == 0

    snap = p["metrics"].snapshot()
    assert snap["errors.invalid_json"] == 1
    assert snap.get("dlq_total", 0) == 0


@pytest.mark.asyncio
async def test_pipeline_schema_violation_goes_to_dlq(
    pipeline_setup, raw_event_factory, mock_llm_err,
):
    """SCHEMA_VIOLATION non-retryable → DLQ + ack."""
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_err(
        EnrichErrorKind.SCHEMA_VIOLATION, retryable=False, msg="missing tickers field",
    ))

    # Should NOT raise — non-retryable returns normally so Consumer xacks
    await p["pipeline"].process(raw)

    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 0
    assert await p["redis"].xlen(p["settings"].enriched_news_dlq_stream) == 1

    # DLQ record has expected fields
    msgs = await p["redis"].xrange(p["settings"].enriched_news_dlq_stream)
    _, fields = msgs[0]
    assert fields[b"raw_event_id"].decode() == raw.event_id
    assert fields[b"error_kind"].decode() == "schema_violation"
    assert b"missing tickers field" in fields[b"error_message"]
    assert fields[b"prompt_version"].decode() == "1.0.0"

    snap = p["metrics"].snapshot()
    assert snap["dlq_total"] == 1
    assert snap["errors.schema_violation"] == 1


@pytest.mark.asyncio
async def test_pipeline_empty_financial_goes_to_dlq(
    pipeline_setup, raw_event_factory, mock_llm_err,
):
    """EMPTY_FINANCIAL non-retryable per design decision → DLQ."""
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_err(
        EnrichErrorKind.EMPTY_FINANCIAL, retryable=False,
    ))

    await p["pipeline"].process(raw)

    assert await p["redis"].xlen(p["settings"].enriched_news_dlq_stream) == 1
    snap = p["metrics"].snapshot()
    assert snap["dlq_total"] == 1
    assert snap["errors.empty_financial"] == 1


# =========================================================================
# Retryable path — rate limit and timeout
# =========================================================================

@pytest.mark.asyncio
async def test_pipeline_rate_limit_raises_retryable(
    pipeline_setup, raw_event_factory, mock_llm_err,
):
    """RATE_LIMIT retryable → raises, message stays in pending."""
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_err(
        EnrichErrorKind.RATE_LIMIT, retryable=True,
    ))

    with pytest.raises(EnrichmentRetryable) as exc_info:
        await p["pipeline"].process(raw)
    assert exc_info.value.kind == EnrichErrorKind.RATE_LIMIT

    # Nothing published anywhere — Consumer should reclaim later
    assert await p["redis"].xlen(p["settings"].enriched_news_stream) == 0
    assert await p["redis"].xlen(p["settings"].enriched_news_dlq_stream) == 0


# =========================================================================
# Token accounting
# =========================================================================

@pytest.mark.asyncio
async def test_pipeline_records_latency_and_tokens(
    pipeline_setup, raw_event_factory, mock_llm_ok,
):
    p = pipeline_setup
    raw = raw_event_factory()
    p["llm"].enrich = AsyncMock(return_value=mock_llm_ok())

    await p["pipeline"].process(raw)

    snap = p["metrics"].snapshot()
    assert snap["latency_samples"] == 1
    assert snap["llm_input_tokens"] == 100
    assert snap["llm_output_tokens"] == 50
