"""Monitor service entry point.

Run with:
    python -m src.services.monitor

Polls system:heartbeats, DLQ streams, risk:daily_pnl. Emits alerts as
log.warning / log.error. Sprint 5.4: log only; Telegram — Sprint 6.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from redis.asyncio import Redis

from src.infra.heartbeat import HeartbeatPublisher
from src.infra.redis_factory import make_redis

from .aggregator import HeartbeatAggregator
from .config import MonitorSettings, load_settings
from .metrics import MonitorMetrics
from .pipeline import MonitorPipeline

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def run() -> int:
    setup_logging()
    try:
        settings = load_settings()
    except (RuntimeError, FileNotFoundError, ValueError) as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    log.info(
        "monitor starting heartbeat=%s dlq_streams=%s tracked=%s poll=%ds",
        settings.heartbeat_stream, settings.dlq_streams,
        settings.tracked_services, settings.poll_interval_sec,
    )

    redis: Redis = make_redis(settings.redis_url)
    try:
        await redis.ping()
        log.info("redis connected url=%s", settings.redis_url)
    except Exception as e:
        log.error("redis_unreachable err=%s", e)
        await redis.aclose()
        return 3

    aggregator = HeartbeatAggregator(redis, settings.heartbeat_stream)
    metrics = MonitorMetrics()
    pipeline = MonitorPipeline(
        settings=settings, aggregator=aggregator,
        redis=redis, metrics=metrics,
    )

    hb = HeartbeatPublisher(
        redis=redis, stream=settings.heartbeat_stream,
        producer=settings.producer_name,
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=metrics.snapshot,
    )

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig_name: str) -> None:
        if shutdown.is_set():
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
        while not shutdown.is_set():
            try:
                await pipeline.tick()
            except Exception:
                log.exception("monitor_tick_error")
                metrics.inc("errors.tick")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=settings.poll_interval_sec)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("shutting down...")
        await hb.stop()
        try:
            await redis.aclose()
        except Exception as e:
            log.warning("redis close failed: %s", e)
        log.info("monitor stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
