"""BridgePipeline — trade:signals → trade:executions.

For each TradeSignalEvent:
  - REJECT events игнорируются (counter only) — Decision уже опубликовал для analytics
  - EXECUTE events:
    1. Idempotency claim by signal.event_id
    2. PaperExecutor.open_position() → fill simulation
    3. Publish OPEN ExecutionResultEvent
    4. PositionTracker.spawn() → asyncio.Task для bar-by-bar SL/TP/time

CLOSE event публикуется внутри PositionTracker._track_loop.
"""
from __future__ import annotations

import logging
import time

from src.contracts.base import MessageEnvelope
from src.contracts.execution_result import ExecutionResultEvent
from src.contracts.trade_signal import TradeSignalEvent
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher

from .config import BridgeSettings
from .metrics import BridgeMetrics
from .paper_executor import PaperExecutor
from .position_tracker import PositionTracker

log = logging.getLogger(__name__)


class BridgePipeline:
    def __init__(
        self,
        *,
        settings: BridgeSettings,
        executor: PaperExecutor,
        tracker: PositionTracker,
        idem: IdempotencyGuard,
        publisher: StreamPublisher,
        metrics: BridgeMetrics,
    ) -> None:
        self.settings = settings
        self.executor = executor
        self.tracker = tracker
        self.idem = idem
        self.publisher = publisher
        self.metrics = metrics

    async def process(self, event: MessageEnvelope) -> None:
        if not isinstance(event, TradeSignalEvent):
            log.error("pipeline_wrong_type %s", type(event).__name__)
            self.metrics.inc("errors.wrong_type")
            return

        self.metrics.inc("events_in")
        p = event.payload

        # REJECT — only counter, no fill
        if p.action == "REJECT":
            self.metrics.inc("rejects_seen")
            return

        if p.action != "EXECUTE":
            log.error("unknown_action %s", p.action)
            return

        # Idempotency claim
        claimed = await self.idem.claim(
            scope=self.settings.idempotency_scope, key=event.event_id,
        )
        if not claimed:
            self.metrics.inc("events_skipped_idem")
            return

        t_start = time.perf_counter()
        pos = self.executor.open_position(event)
        if pos is None:
            self.metrics.inc("errors.open_failed")
            # NB: this is unusual paper-failure; deliberately не raise так как
            # ack semantics — мы не хотим заблокировать PEL.
            log.warning("open_failed signal_event_id=%s ticker=%s",
                        event.event_id, p.ticker)
            return

        # Publish OPEN
        open_payload = self.executor.build_open_payload(pos)
        open_event = ExecutionResultEvent(
            producer=self.settings.producer_name,
            trace=event.trace,
            payload=open_payload,
        )
        await self.publisher.publish(open_event)
        self.metrics.inc("opens_published")

        elapsed = (time.perf_counter() - t_start) * 1000
        self.metrics.record_latency_ms(elapsed)

        # Spawn tracker (CLOSE publish happens inside)
        await self.tracker.spawn(pos, parent_trace=event.trace)
        log.info(
            "opened signal=%s ticker=%s side=%s qty=%d entry=%.4f sl=%.4f tp=%.4f",
            event.event_id, pos.ticker, pos.side, pos.quantity,
            pos.entry_price, pos.sl, pos.tp,
        )
