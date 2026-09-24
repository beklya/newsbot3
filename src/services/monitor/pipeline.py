"""MonitorPipeline — periodic loop driving aggregator + alert rules.

На каждой итерации:
  1. HeartbeatAggregator.tick() — впитать новые heartbeats
  2. XLEN на каждый DLQ stream → store sample (ts, len)
  3. GET risk:daily_pnl:<today> → float
  4. Evaluate 3 alert правила
  5. Дедупликация: для каждого (rule, context_key) — log only at most раз в
     suppress_repeats_sec секунд
  6. emit log.warning/error

Stateless (state в самом aggregator + DLQ history deque).
"""
from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict

from redis.asyncio import Redis

from .aggregator import HeartbeatAggregator
from .alerts import (
    Alert,
    evaluate_daily_pnl_kill,
    evaluate_dlq_spike,
    evaluate_missing_heartbeat,
    evaluate_telegram_reconnect_storm,
)
from .config import MonitorSettings
from .metrics import MonitorMetrics

log = logging.getLogger(__name__)

_DLQ_HISTORY_MAXLEN = 60  # 60 samples × poll_interval_sec=30s = 30min
_ALERT_SUPPRESS_TICKS = 10  # повторяем тот же alert не чаще раз в N tick'ов


class MonitorPipeline:
    def __init__(
        self,
        *,
        settings: MonitorSettings,
        aggregator: HeartbeatAggregator,
        redis: Redis,
        metrics: MonitorMetrics,
    ) -> None:
        self.settings = settings
        self.aggregator = aggregator
        self.redis = redis
        self.metrics = metrics
        self._dlq_history: Dict[str, Deque[tuple[datetime, int]]] = defaultdict(
            lambda: deque(maxlen=_DLQ_HISTORY_MAXLEN),
        )
        self._last_alert_tick: Dict[str, int] = {}
        self._tick_counter: int = 0
        # Startup grace: missing_heartbeat alerts suppressed first N seconds
        # after Monitor process starts. См. config.startup_grace_sec.
        self._started_at: datetime = datetime.now(timezone.utc)

    async def tick(self) -> list[Alert]:
        self._tick_counter += 1
        now = datetime.now(timezone.utc)

        # 1. Heartbeats
        await self.aggregator.tick()

        # 2. DLQ XLEN samples
        for stream in self.settings.dlq_streams:
            try:
                xlen = await self.redis.xlen(stream)
            except Exception:
                continue
            self._dlq_history[stream].append((now, int(xlen)))

        # 3. Daily PnL
        today = now.strftime("%Y-%m-%d")
        key = f"{self.settings.risk_daily_pnl_key_prefix}{today}"
        raw = await self.redis.get(key)
        if raw is None:
            daily_pnl = 0.0
        else:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                daily_pnl = float(raw)
            except ValueError:
                daily_pnl = 0.0

        # 4. Evaluate rules
        alerts: list[Alert] = []
        # Startup grace: missing_heartbeat suppressed для первых N секунд
        # после старта Monitor (cold start, stale heartbeats prev session).
        grace_elapsed = (now - self._started_at).total_seconds()
        within_grace = grace_elapsed < self.settings.startup_grace_sec
        if not within_grace:
            alerts.extend(evaluate_missing_heartbeat(
                self.aggregator.state,
                self.settings.tracked_services,
                self.settings.missing_heartbeat_threshold_sec,
                now,
            ))
        else:
            # Считаем как suppressed для observability
            for service in self.settings.tracked_services:
                self.metrics.inc("alerts.grace_suppressed.missing_heartbeat")
                # хватит один counter, не per-service
                break
        alerts.extend(evaluate_dlq_spike(
            {k: list(v) for k, v in self._dlq_history.items()},
            self.settings.dlq_spike_threshold,
            self.settings.dlq_window_sec,
            now,
        ))
        alerts.extend(evaluate_daily_pnl_kill(
            daily_pnl,
            self.settings.initial_equity_rub,
            self.settings.daily_kill_pct,
        ))
        # Sprint 5.9: alert on Telegram MTProto reconnect storm (read from
        # Receiver heartbeat snapshot; storm flag is set by
        # TelegramHealthMonitor in receiver).
        alerts.extend(evaluate_telegram_reconnect_storm(
            self.aggregator.state,
            receiver_service="receiver",
        ))

        # 5. Dedupe + log
        emitted: list[Alert] = []
        for a in alerts:
            key_full = f"{a.rule}:{a.context_key}"
            last = self._last_alert_tick.get(key_full, -1_000_000)
            if self._tick_counter - last < _ALERT_SUPPRESS_TICKS:
                self.metrics.inc(f"alerts.suppressed.{a.rule}")
                continue
            self._last_alert_tick[key_full] = self._tick_counter
            self.metrics.inc(f"alerts.emitted.{a.rule}")
            emitted.append(a)
            if a.severity == "crit":
                log.error("ALERT[%s] %s | %s", a.severity, a.rule, a.message)
            else:
                log.warning("ALERT[%s] %s | %s", a.severity, a.rule, a.message)

        return emitted
