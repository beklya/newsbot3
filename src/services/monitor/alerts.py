"""AlertRules — stateless rule evaluators over snapshot inputs.

Каждое правило возвращает list[Alert] (может быть пустой). Pipeline
дедуплицирует alert'ы (один и тот же ключ не повторяется чаще раз в N).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from .aggregator import ServiceHeartbeatState

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Alert:
    """Single alert payload. Pipeline writes to log + counter."""
    severity: str   # "warn" | "crit"
    rule: str
    message: str
    context_key: str = ""  # для дедупа в pipeline (один alert per (rule, key))


def evaluate_missing_heartbeat(
    hb_state: Dict[str, ServiceHeartbeatState],
    tracked: List[str],
    threshold_sec: int,
    now: datetime,
) -> List[Alert]:
    alerts: List[Alert] = []
    for service in tracked:
        st = hb_state.get(service)
        if st is None or st.last_seen_utc is None:
            alerts.append(Alert(
                severity="warn",
                rule="missing_heartbeat",
                message=f"no heartbeat ever from {service}",
                context_key=f"missing_heartbeat:{service}",
            ))
            continue
        gap_sec = (now - st.last_seen_utc).total_seconds()
        if gap_sec > threshold_sec:
            alerts.append(Alert(
                severity="crit" if gap_sec > threshold_sec * 3 else "warn",
                rule="missing_heartbeat",
                message=f"{service} last_seen {gap_sec:.0f}s ago (threshold {threshold_sec}s)",
                context_key=f"missing_heartbeat:{service}",
            ))
    return alerts


def evaluate_dlq_spike(
    dlq_history: Dict[str, List[tuple[datetime, int]]],
    threshold_delta: int,
    window_sec: int,
    now: datetime,
) -> List[Alert]:
    """dlq_history: dict[stream → list of (ts, xlen) samples]. Compare last vs window-ago."""
    alerts: List[Alert] = []
    for stream, samples in dlq_history.items():
        if len(samples) < 2:
            continue
        latest_ts, latest_len = samples[-1]
        # Find first sample at least window_sec ago
        cutoff = (now.timestamp() - window_sec)
        prior = None
        for ts, ln in samples:
            if ts.timestamp() <= cutoff:
                prior = (ts, ln)
            else:
                break
        if prior is None:
            continue
        _, prior_len = prior
        delta = latest_len - prior_len
        if delta >= threshold_delta:
            alerts.append(Alert(
                severity="warn",
                rule="dlq_rate_spike",
                message=(
                    f"{stream} +{delta} in last {window_sec}s "
                    f"(prior={prior_len} latest={latest_len})"
                ),
                context_key=f"dlq_rate_spike:{stream}",
            ))
    return alerts


def evaluate_telegram_reconnect_storm(
    hb_state: Dict[str, ServiceHeartbeatState],
    receiver_service: str = "receiver",
) -> List[Alert]:
    """Telethon MTProto reconnect storm detected by Receiver.

    Reads tg_storm_active flag and tg_reconnects_window count from the
    Receiver's last heartbeat snapshot. Receiver populates these via
    TelegramHealthMonitor (Sprint 5.9 / src/services/receiver/telegram_health.py).

    Background: in production paper soak we observed periods where the
    Telegram server drops the MTProto connection every ~5s for tens of
    minutes (likely ISP-level throttling or DC4 load). Telethon
    transparently reconnects and Get-difference's missed events, but
    pipeline throughput collapses during the storm. Without this alert
    the only signal is delayed events downstream.
    """
    alerts: List[Alert] = []
    st = hb_state.get(receiver_service)
    if st is None or not st.last_snapshot:
        return alerts
    snap = st.last_snapshot
    try:
        storm_active = int(snap.get("tg_storm_active", "0"))
    except (TypeError, ValueError):
        storm_active = 0
    if storm_active != 1:
        return alerts
    try:
        window_count = int(snap.get("tg_reconnects_window", "0"))
    except (TypeError, ValueError):
        window_count = 0
    try:
        total = int(snap.get("tg_reconnects_total", "0"))
    except (TypeError, ValueError):
        total = 0
    alerts.append(Alert(
        severity="warn",
        rule="telegram_reconnect_storm",
        message=(
            f"Telethon reconnect storm: {window_count} reconnects in last 60s "
            f"(total since start: {total}). "
            "Telegram server keeps closing the connection; messages still arrive "
            "via catch-up but pipeline throughput is degraded. "
            "Suspect ISP MTProto throttling or DC overload."
        ),
        context_key="telegram_reconnect_storm",
    ))
    return alerts


def evaluate_daily_pnl_kill(
    daily_pnl_rub: float,
    initial_equity_rub: float,
    kill_pct: float,
) -> List[Alert]:
    if initial_equity_rub <= 0:
        return []
    pct = daily_pnl_rub / initial_equity_rub
    if abs(pct) >= kill_pct:
        return [Alert(
            severity="crit",
            rule="daily_pnl_kill",
            message=(
                f"daily_pnl={daily_pnl_rub:+.0f}₽ ({pct*100:+.2f}%) "
                f"exceeds kill_pct={kill_pct*100:.1f}%"
            ),
            context_key="daily_pnl_kill",
        )]
    return []
