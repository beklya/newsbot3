"""Decision service entry point.

Run with:
    python -m src.services.decision

Reads from ml:predictions, looks up cached EnrichedNewsEvent in Redis,
applies LLM B-filter + Phase 2 R:R + RiskManager, publishes
TradeSignalEvent (EXECUTE or REJECT) to trade:signals.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from redis.asyncio import Redis

from src.contracts.ml_prediction import MLPredictionEvent
from src.infra.consumer import StreamConsumer
from src.infra.heartbeat import HeartbeatPublisher
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.infra.redis_factory import make_redis

from .config import DecisionSettings, load_settings
from .enrichment_cache import EnrichmentCache
from .metrics import DecisionMetrics
from .pipeline import DecisionPipeline
from .risk_manager import RiskManager

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _build_snapshot_fn(metrics: DecisionMetrics):
    def _snapshot() -> dict[str, Any]:
        return metrics.snapshot()
    return _snapshot


async def run() -> int:
    setup_logging()
    try:
        settings = load_settings()
    except (RuntimeError, FileNotFoundError, ValueError) as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    log.info(
        "decision starting horizon=%dm rr_th=%.1f conf_th=%.2f risk=%.3f%% max_open=%d "
        "streams=%s→%s",
        settings.horizon_min, settings.rr_threshold,
        settings.direction_filter_min_confidence,
        settings.risk_per_trade_pct * 100,
        settings.max_open_positions,
        settings.ml_predictions_stream, settings.trade_signals_stream,
    )

    redis: Redis = make_redis(settings.redis_url)
    try:
        await redis.ping()
        log.info("redis connected url=%s", settings.redis_url)
    except Exception as e:
        log.error("redis_unreachable url=%s err=%s", settings.redis_url, e)
        await redis.aclose()
        return 3

    cache = EnrichmentCache(redis, key_prefix=settings.enrichment_cache_key_prefix)
    risk = RiskManager(
        redis=redis,
        open_positions_key=settings.risk_open_positions_key,
        daily_pnl_key_prefix=settings.risk_daily_pnl_key_prefix,
        cooldown_key_prefix=settings.risk_cooldown_key_prefix,
        max_open_positions=settings.max_open_positions,
        daily_kill_pct=settings.daily_kill_pct,
        initial_equity_rub=settings.initial_equity_rub,
    )
    idem = IdempotencyGuard(redis, ttl_seconds=settings.idempotency_ttl_sec)
    publisher = StreamPublisher(
        redis, stream=settings.trade_signals_stream, maxlen=settings.trade_signals_maxlen,
    )
    metrics = DecisionMetrics()

    pipeline = DecisionPipeline(
        settings=settings,
        enrichment_cache=cache,
        risk_manager=risk,
        idem=idem,
        publisher=publisher,
        metrics=metrics,
    )

    consumer = StreamConsumer(
        redis=redis,
        stream=settings.ml_predictions_stream,
        group=settings.consumer_group,
        consumer_name=settings.consumer_name,
        event_type=MLPredictionEvent,
        block_ms=settings.consumer_block_ms,
    )

    hb = HeartbeatPublisher(
        redis=redis,
        stream=settings.heartbeat_stream,
        producer=settings.producer_name,
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=_build_snapshot_fn(metrics),
    )

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig_name: str) -> None:
        if shutdown.is_set():
            log.warning("second signal — forcing exit")
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

    hb.start()
    try:
        await consumer.run(handler=pipeline.process, shutdown=shutdown)
    finally:
        log.info("shutting down...")
        await hb.stop()
        try:
            await redis.aclose()
        except Exception as e:
            log.warning("redis close failed: %s", e)
        log.info("decision stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
