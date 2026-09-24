"""quik_feed service entry point.

Run with:
    python -m src.services.quik_feed

Polls QUIK-populated source file (.csv or .xlsx) every N seconds, detects
new completed minute bars, publishes to Redis `candles:1m` stream.

Heartbeat every 30s into `system:heartbeats` (consumed by Monitor).
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Any

from redis.asyncio import Redis

from src.infra.heartbeat import HeartbeatPublisher
from src.infra.redis_factory import make_redis

from .config import QuikFeedSettings, load_settings
from .feeder import QuikFeeder
from .metrics import QuikFeedMetrics
from .readers import build_reader

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("openpyxl").setLevel(logging.WARNING)


def _build_snapshot_fn(metrics: QuikFeedMetrics, settings: QuikFeedSettings):
    def _snapshot() -> dict[str, Any]:
        snap = metrics.snapshot()
        snap["source"] = settings.quik_feed_source_path.name
        return snap
    return _snapshot


async def run() -> int:
    setup_logging()

    try:
        settings = load_settings()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    log.info(
        "quik_feed starting source=%s ext=%s poll_sec=%d stream=%s",
        settings.quik_feed_source_path,
        settings.quik_feed_source_path.suffix,
        settings.quik_feed_poll_sec,
        settings.candles_stream,
    )

    redis: Redis = make_redis(settings.redis_url)
    try:
        pong = await redis.ping()
        log.info("redis connected ping=%s", pong)
    except Exception as e:
        log.error("redis_unreachable url=%s err=%s", settings.redis_url, e)
        await redis.aclose()
        return 3

    reader = build_reader(
        settings.quik_feed_source_path,
        sheet_name=settings.quik_feed_sheet_name,
        bootstrap_mode=settings.bootstrap_mode,
    )
    metrics = QuikFeedMetrics()
    feeder = QuikFeeder(
        redis=redis, reader=reader, settings=settings, metrics=metrics,
    )
    hb = HeartbeatPublisher(
        redis=redis,
        stream=settings.heartbeat_stream,
        producer=settings.producer_name,
        interval_sec=settings.heartbeat_interval_sec,
        snapshot_fn=_build_snapshot_fn(metrics, settings),
    )

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal(sig_name: str) -> None:
        if shutdown.is_set():
            return
        log.info("signal %s received — initiating shutdown", sig_name)
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
        await feeder.run(shutdown)
    finally:
        log.info("shutting down...")
        await hb.stop()
        try:
            await redis.aclose()
        except Exception as e:
            log.warning("redis close failed: %s", e)
        log.info("quik_feed stopped")
    return 0


def main() -> None:
    sys.exit(asyncio.run(run()))


if __name__ == "__main__":
    main()
