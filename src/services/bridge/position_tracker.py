"""PositionTracker — asyncio.Task per open position, polls SL/TP/time.

Каждая OpenPosition получает свою long-running Task которая:
  1. Каждые tracker_poll_interval_sec вызывает PaperExecutor.check_exit()
  2. На срабатывание SL/TP/time:
     - DEL bridge:open_positions:<signal_event_id>
     - SREM risk:open_positions ticker
     - INCRBYFLOAT risk:daily_pnl:<today> с realized_pnl_rub
     - SET cooldown:<ticker> EX cooldown_ticker_sec
     - publish CLOSE ExecutionResultEvent (новый event_id, наследует trace)
     - exit task

Restart recovery: на startup pipeline.py XRANGE'нет
bridge:open_positions:* keys и спавнит trackers для каждой.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

from redis.asyncio import Redis

from src.contracts.base import MessageEnvelope
from src.contracts.execution_result import ExecutionResultEvent
from src.infra.publisher import StreamPublisher

from .config import BridgeSettings
from .paper_executor import OpenPosition, PaperExecutor

log = logging.getLogger(__name__)


class PositionTracker:
    """Spawns + manages asyncio.Tasks per open position.

    Single instance per bridge process. Concurrent positions tracked independently.
    """

    def __init__(
        self,
        *,
        settings: BridgeSettings,
        executor: PaperExecutor,
        redis: Redis,
        publisher: StreamPublisher,
        producer_name: str,
        on_close: Optional[Callable[[OpenPosition], None]] = None,
    ) -> None:
        self.settings = settings
        self.executor = executor
        self.redis = redis
        self.publisher = publisher
        self.producer_name = producer_name
        self._tasks: dict[str, asyncio.Task] = {}
        self._on_close = on_close

    async def spawn(self, pos: OpenPosition, parent_trace: list[dict]) -> None:
        """Start a new tracker task for pos. Persist state to Redis first."""
        await self._persist(pos)
        await self.redis.sadd(self.settings.risk_open_positions_key, pos.ticker)

        if pos.signal_event_id in self._tasks:
            log.warning("tracker_already_running signal_event_id=%s — skipping spawn",
                        pos.signal_event_id)
            return

        task = asyncio.create_task(
            self._track_loop(pos, parent_trace),
            name=f"tracker.{pos.signal_event_id[:8]}",
        )
        self._tasks[pos.signal_event_id] = task

    async def stop_all(self) -> None:
        """Cancel all tracker tasks (graceful shutdown)."""
        for task in list(self._tasks.values()):
            task.cancel()
        # Pump asyncio one tick so cancellations propagate
        await asyncio.sleep(0)

    def active_count(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.done())

    async def _persist(self, pos: OpenPosition) -> None:
        key = f"{self.settings.bridge_open_position_prefix}{pos.signal_event_id}"
        await self.redis.set(key, pos.to_json())

    async def _cleanup(self, pos: OpenPosition) -> None:
        key = f"{self.settings.bridge_open_position_prefix}{pos.signal_event_id}"
        await self.redis.delete(key)
        await self.redis.srem(self.settings.risk_open_positions_key, pos.ticker)
        # Activate cooldown for this ticker
        cooldown_key = f"{self.settings.risk_cooldown_key_prefix}{pos.ticker}"
        await self.redis.set(cooldown_key, "1", ex=self.settings.cooldown_ticker_sec)

    async def _track_loop(self, pos: OpenPosition, parent_trace: list[dict]) -> None:
        try:
            while True:
                outcome = self.executor.check_exit(pos)
                if outcome is not None:
                    # CLOSE: payload + daily_pnl writeback + cleanup
                    payload = self.executor.build_close_payload(pos, outcome)
                    close_event = ExecutionResultEvent(
                        producer=self.producer_name,
                        trace=parent_trace,
                        payload=payload,
                    )
                    await self.publisher.publish(close_event)

                    # INCRBYFLOAT daily PnL (UTC date)
                    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    daily_key = f"{self.settings.risk_daily_pnl_key_prefix}{today}"
                    await self.redis.incrbyfloat(daily_key, outcome.realized_pnl_rub)
                    # Expire next-day at end (24h covers most edge cases)
                    await self.redis.expire(daily_key, 60 * 60 * 36)

                    await self._cleanup(pos)
                    if self._on_close:
                        self._on_close(pos)
                    log.info(
                        "closed signal=%s ticker=%s reason=%s pnl=%.2f duration_s=%d",
                        pos.signal_event_id, pos.ticker, outcome.exit_reason,
                        outcome.realized_pnl_rub, outcome.duration_sec,
                    )
                    return

                await asyncio.sleep(self.settings.tracker_poll_interval_sec)
        except asyncio.CancelledError:
            log.info("tracker_cancelled signal=%s ticker=%s",
                     pos.signal_event_id, pos.ticker)
            raise
        except Exception:
            log.exception("tracker_loop_error signal=%s ticker=%s",
                          pos.signal_event_id, pos.ticker)
        finally:
            self._tasks.pop(pos.signal_event_id, None)

    async def recover_from_redis(self, parent_trace: list[dict]) -> int:
        """On startup, re-spawn tracker tasks for each persisted OpenPosition.

        Returns count of recovered positions.
        """
        prefix = self.settings.bridge_open_position_prefix
        recovered = 0
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=f"{prefix}*", count=100)
            for k in keys:
                raw = await self.redis.get(k)
                if raw is None:
                    continue
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                try:
                    pos = OpenPosition.from_json(raw)
                except Exception:
                    log.exception("recover_skip_bad_json key=%r", k)
                    continue
                await self.spawn(pos, parent_trace=parent_trace)
                recovered += 1
            if cursor == 0:
                break
        log.info("recovered %d open positions from Redis", recovered)
        return recovered
