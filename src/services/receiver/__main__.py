"""Receiver entry point.

Run modes:
    python -m src.services.receiver                       # realtime only
    python -m src.services.receiver --backfill-hours 1    # 1h history then realtime
    python -m src.services.receiver --backfill-hours 6    # 6h history then realtime

The --backfill-hours flag overrides BACKFILL_HOURS from the .env file.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys

from src.services.receiver.client import ReceiverClient
from src.services.receiver.config import load_settings


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


async def _run(args: argparse.Namespace) -> int:
    log = logging.getLogger("receiver.main")

    try:
        settings = load_settings()
    except FileNotFoundError as e:
        log.error(str(e))
        return 2

    if args.backfill_hours is not None:
        settings.backfill_hours = args.backfill_hours

    log.info(
        "starting receiver (channels=%s, backfill=%dh)",
        settings.channels,
        settings.backfill_hours,
    )

    client = ReceiverClient(settings)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop(*_):
        log.info("shutdown signal received")
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                # Windows: add_signal_handler not supported,
                # KeyboardInterrupt path handles it instead.
                pass

    listen_task: asyncio.Task | None = None
    try:
        await client.connect()
        if settings.backfill_hours > 0:
            await client.backfill(settings.backfill_hours)

        listen_task = asyncio.create_task(client.listen(), name="receiver.listen")
        stop_task = asyncio.create_task(stop_event.wait(), name="receiver.stop")

        try:
            await asyncio.wait(
                {listen_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for t in (listen_task, stop_task):
                if t and not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t
        return 0
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("interrupted, shutting down cleanly")
        if listen_task and not listen_task.done():
            listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await listen_task
        return 0
    finally:
        await client.close()


def main() -> int:
    _setup_logging()
    parser = argparse.ArgumentParser(
        prog="receiver",
        description="Telegram news receiver",
    )
    parser.add_argument(
        "--backfill-hours",
        type=int,
        default=None,
        help="Override BACKFILL_HOURS. 1 for first calibration, 0 for realtime only.",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
