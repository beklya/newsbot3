"""Bridge service entry point (Paper mode).

Run with:
    python -m src.services.bridge

Reads from trade:signals, simulates fills using candle cache, tracks
positions bar-by-bar, publishes ExecutionResultEvent (OPEN + CLOSE) to
trade:executions. Recovers open positions from Redis on startup.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from redis.asyncio import Redis

from src.contracts.trade_signal import TradeSignalEvent
from src.infra.candles import CandleCache
from src.infra.consumer import StreamConsumer
from src.infra.heartbeat import HeartbeatPublisher
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.infra.redis_factory import make_redis

from .config import BridgeSettings, load_settings
from .metrics import BridgeMetrics
from .paper_executor import PaperExecutor
from .pipeline import BridgePipeline
from .position_tracker import PositionTracker

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _build_snapshot_fn(metrics: BridgeMetrics, tracker: PositionTracker):
    def _snapshot() -> dict[str, Any]:
        snap = metrics.snapshot()
        snap["open_positions_tracked"] = tracker.active_count()
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
        "bridge starting mode=%s prices_dir=%s streams=%s→%s",
        settings.mode, settings.prices_dir,
        settings.trade_signals_stream, settings.trade_executions_stream,
    )

    redis: Redis = make_redis(settings.redis_url)
    try:
        await redis.ping()
        log.info("redis connected url=%s", settings.redis_url)
    except Exception as e:
        log.error("redis_unreachable url=%s err=%s", settings.redis_url, e)
        await redis.aclose()
        return 3

    candles = CandleCache(settings.prices_dir)
    candles.load_all()

    publisher = StreamPublisher(
        redis, stream=settings.trade_executions_stream,
        maxlen=settings.trade_executions_maxlen,
    )
    metrics = BridgeMetrics()
    executor = PaperExecutor(settings, candles)
    tracker = PositionTracker(
        settings=settings,
        executor=executor,
        redis=redis,
        publisher=publisher,
        producer_name=settings.producer_name,
    )

    # Recover open positions from previous run
    await tracker.recover_from_redis(parent_trace=[])

    idem = IdempotencyGuard(redis, ttl_seconds=settings.idempotency_ttl_sec)
    pipeline = BridgePipeline(
        settings=settings,
        executor=executor,
        tracker=tracker,
        idem=idem,
        publisher=publisher,
        metrics=metrics,
    )

    consumer = StreamConsumer(
        redis=redis,
        stream=settings.trade_signals_stream,
        group=settings.consumer_group,
        consumer_name=settings.consumer_name,
        event_type=TradeSignalEvent,
        block_ms=settings.consumer_block_ms,
    )

    hb = HeartbeatPublisher(
        redis=redis,
        stream=settings.heartbeat_stream,
        producer=settings.producer_name,
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=_build_snapshot_fn(metrics, tracker),
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

    # Live candles subscriber (Sprint 5.8) — поверх historical CSV
    candle_sub_task = None
    if settings.live_candles_enabled:
        log.info("live candles subscriber: stream=%s", settings.live_candles_stream)
        candle_sub_task = asyncio.create_task(
            candles.subscribe_redis_stream(
                redis, settings.live_candles_stream, shutdown,
            ),
            name="bridge.candle_subscriber",
        )

    hb.start()
    try:
        await consumer.run(handler=pipeline.process, shutdown=shutdown)
    finally:
        log.info("shutting down: stopping %d trackers", tracker.active_count())
        await tracker.stop_all()
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
        log.info("bridge stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
