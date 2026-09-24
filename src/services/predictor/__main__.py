"""Predictor service entry point.

Run with:
    python -m src.services.predictor

Reads from news:enriched, performs Phase 2 XGBoost MFE/MAE inference,
publishes MLPredictionEvent to ml:predictions (or ml:predictions:dlq
on missing_market_data). Emits heartbeats to system:heartbeats every
settings.heartbeat_interval_sec.

Graceful shutdown on SIGINT/SIGTERM: stops reading, drains in-flight,
final heartbeat, closes Redis.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from redis.asyncio import Redis

from src.contracts.enriched_news import EnrichedNewsEvent
from src.infra.consumer import StreamConsumer
from src.infra.heartbeat import HeartbeatPublisher
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.infra.redis_factory import make_redis

from .candle_cache import CandleCache
from .config import PredictorSettings, load_settings
from .metrics import PredictorMetrics
from .model_loader import load_bundle
from .news_history import NewsHistory
from .pipeline import PredictorPipeline

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _build_snapshot_fn(metrics: PredictorMetrics, history: NewsHistory):
    def _snapshot() -> dict[str, Any]:
        snap = metrics.snapshot()
        snap.update(history.stats())
        return snap
    return _snapshot


async def run() -> int:
    setup_logging()

    try:
        settings = load_settings()
    except (RuntimeError, FileNotFoundError, ValueError) as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    log.info(
        "predictor starting models_dir=%s prices_dir=%s streams=%s→%s (dlq=%s)",
        settings.models_dir, settings.prices_dir,
        settings.enriched_news_stream, settings.ml_predictions_stream,
        settings.ml_predictions_dlq_stream,
    )

    # --- Redis ---
    redis: Redis = make_redis(settings.redis_url)
    try:
        await redis.ping()
        log.info("redis connected url=%s", settings.redis_url)
    except Exception as e:
        log.error("redis_unreachable url=%s err=%s", settings.redis_url, e)
        await redis.aclose()
        return 3

    # --- Heavy startup work (могут занять секунды) ---
    bundle = load_bundle(settings.models_dir)

    candles = CandleCache(settings.prices_dir)
    candles.load_all()

    history = NewsHistory(
        lookback_hours=settings.news_history_lookback_hours,
        per_ticker_maxlen=settings.news_history_per_ticker_maxlen,
    )
    await history.bootstrap(redis, settings.enriched_news_stream)

    # --- Building blocks ---
    idem = IdempotencyGuard(redis=redis, ttl_seconds=settings.idempotency_ttl_sec)
    publisher_main = StreamPublisher(
        redis=redis,
        stream=settings.ml_predictions_stream,
        maxlen=settings.ml_predictions_maxlen,
    )
    publisher_dlq = StreamPublisher(
        redis=redis,
        stream=settings.ml_predictions_dlq_stream,
        maxlen=settings.ml_predictions_dlq_maxlen,
    )
    metrics = PredictorMetrics()
    pipeline = PredictorPipeline(
        bundle=bundle,
        candles=candles,
        history=history,
        idem=idem,
        publisher_main=publisher_main,
        publisher_dlq=publisher_dlq,
        metrics=metrics,
        settings=settings,
    )

    # --- Consumer ---
    consumer = StreamConsumer(
        redis=redis,
        stream=settings.enriched_news_stream,
        group=settings.consumer_group,
        consumer_name=settings.consumer_name,
        event_type=EnrichedNewsEvent,
        block_ms=settings.consumer_block_ms,
    )

    # --- Heartbeat ---
    hb = HeartbeatPublisher(
        redis=redis,
        stream=settings.heartbeat_stream,
        producer=settings.producer_name,
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=_build_snapshot_fn(metrics, history),
    )

    # --- Graceful shutdown ---
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig_name: str) -> None:
        if shutdown.is_set():
            log.warning("second signal received, forcing exit")
            return
        log.info("signal %s — graceful shutdown", sig_name)
        shutdown.set()

    if sys.platform == "win32":
        def _win_handler(signum, _frame):
            loop.call_soon_threadsafe(_handle_signal, signal.Signals(signum).name)
        signal.signal(signal.SIGINT, _win_handler)
        signal.signal(signal.SIGTERM, _win_handler)
    else:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal, sig.name)

    # --- Live candles subscriber (Sprint 5.8) ---
    candle_sub_task = None
    if settings.live_candles_enabled:
        log.info("live candles subscriber: stream=%s", settings.live_candles_stream)
        candle_sub_task = asyncio.create_task(
            candles.subscribe_redis_stream(
                redis, settings.live_candles_stream, shutdown,
            ),
            name="predictor.candle_subscriber",
        )

    # --- Run ---
    hb.start()
    try:
        await consumer.run(handler=pipeline.process, shutdown=shutdown)
    finally:
        log.info("shutting down...")
        await hb.stop()
        if candle_sub_task is not None:
            shutdown.set()
            try:
                await asyncio.wait_for(candle_sub_task, timeout=10)
            except asyncio.TimeoutError:
                candle_sub_task.cancel()
        try:
            await redis.aclose()
        except Exception as e:
            log.warning("redis close failed: %s", e)
        log.info("predictor stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
