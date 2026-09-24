"""EnrichmentPipeline — orchestrates Consumer → Idempotency → LLM → Publisher/DLQ.

Handler is the function the Consumer calls per RawNewsEvent. It must:
1. Claim idempotency (skip if already processed)
2. Run LLM enrichment
3. On success → publish EnrichedNewsEvent to news:enriched
4. On non-retryable error → publish failure record to news:enriched:dlq
5. On retryable error → raise EnrichmentRetryable (StreamConsumer will NOT xack,
   message stays in pending and gets reclaimed)

The Consumer (from src.infra.consumer) does xack on successful return,
no-ack on exception. We rely on that behavior — pipeline.process() returning
normally means "consume this message", raising means "leave in pending".
"""

from __future__ import annotations

import logging
from typing import Any

from src.contracts.base import MessageEnvelope, utcnow_iso
from src.contracts.enriched_news import (
    SCHEMA_VERSION as ENRICHED_SCHEMA_VERSION,
    EnrichedNewsEvent,
)
from src.contracts.raw_news import RawNewsEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.services.enricher.config import EnricherSettings
from src.services.enricher.llm_client import (
    EnrichError,
    EnrichErrorKind,
    EnrichResult,
    GroqLLMClient,
)
from src.services.enricher.metrics import EnricherMetrics

log = logging.getLogger(__name__)


class EnrichmentRetryable(Exception):
    """Raised on retryable errors to keep the message in Consumer's pending list.

    StreamConsumer catches Exception, logs, and does NOT xack — exactly what
    we want for transient failures (timeout, rate limit, transient API errors).
    """

    def __init__(self, kind: EnrichErrorKind, message: str):
        super().__init__(f"{kind.value}: {message}")
        self.kind = kind


class EnrichmentPipeline:
    """One pipeline instance per Consumer. Stateless aside from metrics."""

    def __init__(
        self,
        *,
        llm: GroqLLMClient,
        idem: IdempotencyGuard,
        publisher_main: StreamPublisher,
        publisher_dlq: StreamPublisher,
        metrics: EnricherMetrics,
        settings: EnricherSettings,
    ):
        self.llm = llm
        self.idem = idem
        self.publisher_main = publisher_main
        self.publisher_dlq = publisher_dlq
        self.metrics = metrics
        self.settings = settings

    async def process(self, raw_event: MessageEnvelope) -> None:
        """Entry point. Called by StreamConsumer for each RawNewsEvent.

        Must accept MessageEnvelope (Consumer's generic type), but in practice
        always receives RawNewsEvent — cast for type checker.
        """
        if not isinstance(raw_event, RawNewsEvent):
            # Should never happen — consumer is parameterized with RawNewsEvent.
            # Defensive: ack and skip.
            log.error("pipeline_received_wrong_type %s", type(raw_event).__name__)
            self.metrics.inc("errors.wrong_type")
            return

        self.metrics.inc("events_in")

        # --- 1. Idempotency ---
        claimed = await self.idem.claim(
            scope=self.settings.idempotency_scope,
            key=raw_event.event_id,
        )
        if not claimed:
            log.info("idempotency_skip event_id=%s", raw_event.event_id)
            self.metrics.inc("events_skipped_idem")
            return  # уже обработан — Consumer сделает xack

        # --- 2. LLM enrichment ---
        result = await self.llm.enrich(raw_event)
        self.metrics.record_latency_ms(result.latency_ms)
        self.metrics.inc("llm_input_tokens", result.input_tokens)
        self.metrics.inc("llm_output_tokens", result.output_tokens)

        if result.ok:
            await self._publish_enriched(raw_event, result)
            self.metrics.inc("events_out")
            # Verbose log: text preview + summary + tickers w/ directions
            text_preview = raw_event.payload.text.replace("\n", " ")[:80]
            if result.payload.tickers:
                tickers_str = ", ".join(
                    f"{t.ticker}/{t.direction}({t.confidence:.2f})"
                    for t in result.payload.tickers[:5]
                )
            else:
                tickers_str = "—"
            log.info(
                "enriched event_id=%s lat=%dms cat=%s tf=%s tickers=[%s]\n"
                "  text:    %s\n"
                "  summary: %s",
                raw_event.event_id, int(result.latency_ms),
                result.payload.category, result.payload.expected_timeframe,
                tickers_str,
                text_preview,
                result.payload.summary,
            )
            return

        # --- 3. Error handling ---
        err = result.error
        assert err is not None
        self.metrics.inc(f"errors.{err.kind.value}")

        if err.retryable:
            # Не ack — Consumer оставит в pending, потом reclaim.
            log.warning(
                "enrichment_retryable_failure event_id=%s kind=%s msg=%s",
                raw_event.event_id, err.kind.value, err.message[:200],
            )
            raise EnrichmentRetryable(err.kind, err.message)

        # Non-retryable → DLQ + ack
        await self._publish_dlq(raw_event, result)
        self.metrics.inc("dlq_total")
        log.warning(
            "enrichment_dlq event_id=%s kind=%s reason=%s",
            raw_event.event_id, err.kind.value, err.message[:200],
        )

    async def _publish_enriched(self, raw_event: RawNewsEvent, result: EnrichResult) -> None:
        """Build EnrichedNewsEvent inheriting event_id from raw, publish to main stream.

        Sprint 5.4 side effect: SETEX enriched:<event_id> JSON TTL=300s.
        Это позволяет Decision (5.2) сделать GET вместо отдельного consumer на news:enriched.
        Cache write пробую/eat-exception — если Redis отказал, основная публикация важнее.
        """
        # Inherit event_id + trace from raw (так трассировка по цепочке восстанавливается).
        # Не используем default ULID — это другой event_id и связь с raw_event пропадёт.
        enriched_event = EnrichedNewsEvent(
            event_id=raw_event.event_id,
            schema_version=ENRICHED_SCHEMA_VERSION,
            producer=self.settings.producer_name,
            trace=raw_event.trace,  # сохраняем trace от Receiver, дальше Publisher добавит свой шаг
            payload=result.payload,
        )
        await self.publisher_main.publish(enriched_event)

        # Side effect for Decision cache (Sprint 5.4)
        try:
            cache_key = f"{self.settings.enrichment_cache_key_prefix}{enriched_event.event_id}"
            await self.publisher_main.redis.set(
                cache_key,
                enriched_event.model_dump_json(),
                ex=self.settings.enrichment_cache_ttl_sec,
            )
        except Exception as e:
            # Cache miss is recoverable for Decision; не блокируем pipeline.
            log.warning("enrichment_cache_setex_failed event_id=%s err=%s",
                        enriched_event.event_id, e)

    async def _publish_dlq(self, raw_event: RawNewsEvent, result: EnrichResult) -> None:
        """Publish a failure record to DLQ stream for later analysis.

        DLQ records do NOT validate against EnrichedNewsPayload — they're for ops/triage.
        We write a flat dict via xadd directly through publisher's redis client.
        """
        err = result.error
        assert err is not None
        # Use the publisher's redis connection directly with a flat fields dict.
        # We bypass MessageEnvelope here because the message wouldn't validate
        # (it's an error record, not an enriched event).
        fields: dict[str, Any] = {
            "raw_event_id": raw_event.event_id,
            "channel": raw_event.payload.channel,
            "text_hash": raw_event.payload.text_hash,
            "tg_published_at": raw_event.payload.tg_published_at,
            "raw_text_preview": raw_event.payload.text[:500],
            "error_kind": err.kind.value,
            "error_message": err.message[:1_000],
            "llm_raw_response": result.raw_response[:5_000],
            "llm_latency_ms": str(int(result.latency_ms)),
            "llm_input_tokens": str(result.input_tokens),
            "llm_output_tokens": str(result.output_tokens),
            "prompt_version": self.settings.prompt_version,
            "occurred_at": utcnow_iso(),
        }
        # All values must be strings for Redis Streams.
        str_fields = {k: str(v) for k, v in fields.items()}
        await self.publisher_dlq.redis.xadd(
            self.publisher_dlq.stream,
            fields=str_fields,
            maxlen=self.settings.enriched_news_dlq_maxlen,
            approximate=True,
        )
