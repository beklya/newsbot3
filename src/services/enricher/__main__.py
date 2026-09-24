"""Enricher service entry point.

Run with:
    python -m src.services.enricher

Reads from news:raw, enriches via Groq, publishes to news:enriched
(or news:enriched:dlq on non-retryable errors). Emits heartbeats to
system:heartbeats every settings.heartbeat_interval_sec.

Graceful shutdown on SIGINT / SIGTERM: stops reading, drains in-flight,
final heartbeat, closes Groq pool and Redis.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from redis.asyncio import Redis

from src.contracts.base import utcnow_iso
from src.contracts.raw_news import RawNewsEvent
from src.infra.consumer import StreamConsumer
from src.infra.idempotency import IdempotencyGuard
from src.infra.pel_reclaim import PelReclaimer
from src.infra.publisher import StreamPublisher
from src.infra.redis_factory import make_redis
from src.services.enricher.config import EnricherSettings, load_settings
from src.infra.heartbeat import HeartbeatPublisher
from src.services.enricher.llm_client import GroqLLMClient
from src.services.enricher.di_client import DeepInfraLLMClient
from src.services.enricher.metrics import EnricherMetrics
from src.services.enricher.pipeline import EnrichmentPipeline
from src.services.enricher.prompt import PromptBuilder

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quiet down noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _build_snapshot_fn(
    metrics: EnricherMetrics,
    llm: GroqLLMClient,
    reclaimer_snapshot: Any = None,
):
    """Return a snapshot_fn for HeartbeatPublisher that combines metrics + pool stats.

    `reclaimer_snapshot` is an optional callable returning the PelReclaimer's
    counter dict (n reclaimed, ok, fail, dlq). Wired through __main__ so that
    pel_reclaim_* counters appear in heartbeats automatically.
    """

    def _snapshot() -> dict[str, Any]:
        snap = metrics.snapshot()
        pool_stats = llm.pool.stats()
        snap["pool_n_total"] = pool_stats["n_total"]
        snap["pool_n_ready"] = pool_stats["n_ready"]
        snap["pool_n_cooldown"] = pool_stats["n_cooldown"]
        if reclaimer_snapshot is not None:
            try:
                snap.update(reclaimer_snapshot())
            except Exception:
                pass
        return snap

    return _snapshot


async def run() -> int:
    setup_logging()

    try:
        settings = load_settings()
    except (RuntimeError, FileNotFoundError) as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    log.info(
        "enricher starting provider=%s model=%s n_keys=%d prompt=%s "
        "concurrency=%d streams=%s→%s (dlq=%s)",
        settings.enricher_provider,
        settings.deepinfra_model if settings.enricher_provider == "deepinfra"
        else settings.groq_model,
        len(settings.resolve_api_keys()),
        settings.prompt_version,
        settings.enricher_concurrency,
        settings.raw_news_stream,
        settings.enriched_news_stream,
        settings.enriched_news_dlq_stream,
    )

    # --- Redis ---
    redis: Redis = make_redis(settings.redis_url)
    try:
        pong = await redis.ping()
        log.info("redis connected url=%s ping=%s", settings.redis_url, pong)
    except Exception as e:
        log.error("redis_unreachable url=%s err=%s", settings.redis_url, e)
        await redis.aclose()
        return 3

    # --- Building blocks ---
    prompt = PromptBuilder(settings.prompt_file, settings.prompt_version)
    provider = (settings.enricher_provider or "groq").lower()
    if provider == "deepinfra":
        log.info("LLM provider = DeepInfra (Sprint 6.2 train-serve match)")
        llm = DeepInfraLLMClient(settings=settings, prompt_builder=prompt)
    else:
        log.info("LLM provider = Groq (legacy)")
        llm = GroqLLMClient(settings=settings, prompt_builder=prompt)
    idem = IdempotencyGuard(redis=redis, ttl_seconds=settings.idempotency_ttl_sec)
    publisher_main = StreamPublisher(
        redis=redis,
        stream=settings.enriched_news_stream,
        maxlen=settings.enriched_news_maxlen,
    )
    publisher_dlq = StreamPublisher(
        redis=redis,
        stream=settings.enriched_news_dlq_stream,
        maxlen=settings.enriched_news_dlq_maxlen,
    )
    metrics = EnricherMetrics()
    pipeline = EnrichmentPipeline(
        llm=llm,
        idem=idem,
        publisher_main=publisher_main,
        publisher_dlq=publisher_dlq,
        metrics=metrics,
        settings=settings,
    )

    # --- Consumers (Sprint 6.2: N parallel workers) ---
    # Each consumer has unique consumer_name under the same group, so Redis
    # XREADGROUP distributes events round-robin. PEL is per-consumer-name,
    # so each one's stuck messages recover independently on restart.
    # Naming: settings.consumer_name is used as the base if N=1 (back-compat
    # with existing PEL state in production), else suffixed with "-{n}".
    n_workers = settings.enricher_concurrency
    if n_workers == 1:
        consumer_names = [settings.consumer_name]
    else:
        base = settings.consumer_name
        # Sprint 6.2 — keep "-1" suffix for the first worker so any PEL state
        # left over from the pre-multi-worker single-consumer run gets drained
        # naturally instead of stranded under the unused old name.
        if base.endswith("-1"):
            stem = base[:-2]
        else:
            stem = base
        consumer_names = [f"{stem}-{i+1}" for i in range(n_workers)]
    log.info("spawning %d consumer worker(s): %s",
             n_workers, ", ".join(consumer_names))
    consumers = [
        StreamConsumer(
            redis=redis,
            stream=settings.raw_news_stream,
            group=settings.consumer_group,
            consumer_name=name,
            event_type=RawNewsEvent,
        )
        for name in consumer_names
    ]

    # --- PEL Reclaimer (Sprint 6.2 ext) ---
    # StreamConsumer only recovers its own PEL on startup. When a handler raises
    # (e.g. DI returns invalid JSON under load), the message stays in PEL forever
    # until the next service restart. With 6 parallel workers this becomes a
    # silent throughput killer — observed 2026-06-09: 633 messages stuck in PEL
    # while workers idled 3.4h consuming only fresh messages.
    #
    # PelReclaimer runs in parallel with the consumers, periodically
    # XAUTOCLAIM'ing PEL items idle longer than `min_idle_ms`. min_idle_ms must
    # exceed the longest legitimate handler latency so we don't steal in-flight
    # work — DI 70B p95 latency on this corpus is ~36s, plus internal retries
    # can extend to ~80s, so 120s is the conservative default.
    async def _reclaim_dlq(msg_id: bytes, fields: dict, reason: str) -> None:
        """Terminal-DLQ handler: msg exceeded max_deliveries, give up cleanly."""
        try:
            mid = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
            data_raw = fields.get(b"data") or fields.get("data") or b""
            if isinstance(data_raw, bytes):
                data_str = data_raw.decode("utf-8", errors="replace")
            else:
                data_str = str(data_raw)
            await redis.xadd(
                settings.enriched_news_dlq_stream,
                fields={
                    "kind": "pel_max_deliveries",
                    "reason": reason,
                    "stream": settings.raw_news_stream,
                    "msg_id": mid,
                    "raw_event_data_preview": data_str[:2_000],
                    "occurred_at": utcnow_iso(),
                },
                maxlen=settings.enriched_news_dlq_maxlen,
                approximate=True,
            )
        except Exception:
            log.exception("reclaim DLQ publish failed for msg_id=%s", msg_id)

    pel_reclaimer = PelReclaimer(
        redis=redis,
        stream=settings.raw_news_stream,
        group=settings.consumer_group,
        consumer_name="reclaimer-1",
        event_type=RawNewsEvent,
        handler=pipeline.process,
        min_idle_ms=settings.pel_reclaim_min_idle_ms,
        poll_interval_sec=settings.pel_reclaim_poll_interval_sec,
        max_deliveries=settings.pel_reclaim_max_deliveries,
        dlq_handler=_reclaim_dlq,
    )

    # --- Heartbeat ---
    hb = HeartbeatPublisher(
        redis=redis,
        stream=settings.heartbeat_stream,
        producer="enricher",
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=_build_snapshot_fn(
            metrics, llm,
            reclaimer_snapshot=pel_reclaimer.snapshot,
        ),
    )

    # --- Graceful shutdown ---
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig_name: str) -> None:
        if shutdown.is_set():
            log.warning("second signal received, forcing exit")
            return
        log.info("signal %s received — initiating graceful shutdown", sig_name)
        shutdown.set()

    # On Windows asyncio doesn't support add_signal_handler — fall back to signal.signal.
    if sys.platform == "win32":
        # signal.signal handler runs in the main thread; set the event there.
        def _win_handler(signum, _frame):
            loop.call_soon_threadsafe(_handle_signal, signal.Signals(signum).name)

        signal.signal(signal.SIGINT, _win_handler)
        signal.signal(signal.SIGTERM, _win_handler)
    else:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal, sig.name)

    # --- Run ---
    hb.start()
    try:
        # All workers share the pipeline (and through it the LLM client,
        # idempotency guard, publisher, metrics). Each runs its own
        # StreamConsumer.run loop with a distinct consumer_name.
        # PelReclaimer runs alongside as a 7th task — it doesn't compete for
        # new messages (`>` cursor), only drains stuck PEL items.
        worker_tasks = [
            c.run(handler=pipeline.process, shutdown=shutdown)
            for c in consumers
        ]
        worker_tasks.append(pel_reclaimer.run(shutdown))
        await asyncio.gather(*worker_tasks)
    finally:
        log.info("shutting down...")
        await hb.stop()
        # Try one final snapshot so soak analyzer sees the very last counters.
        try:
            await publisher_main.redis.xadd(
                settings.heartbeat_stream,
                fields={
                    "service": "enricher",
                    "at": "final",
                    **{k: str(v) for k, v in metrics.snapshot().items()},
                },
                maxlen=5_000,
                approximate=True,
            )
        except Exception as e:
            log.warning("final heartbeat failed: %s", e)
        try:
            await llm.pool.close()
        except Exception as e:
            log.warning("llm pool close failed: %s", e)
        try:
            await redis.aclose()
        except Exception as e:
            log.warning("redis close failed: %s", e)
        log.info("enricher stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
