"""Tests for alert rules (stateless evaluators)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.services.monitor.aggregator import ServiceHeartbeatState
from src.services.monitor.alerts import (
    evaluate_daily_pnl_kill,
    evaluate_dlq_spike,
    evaluate_missing_heartbeat,
    evaluate_telegram_reconnect_storm,
)


NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def test_missing_heartbeat_never_seen():
    """tracked service never published — single alert."""
    alerts = evaluate_missing_heartbeat(
        hb_state={}, tracked=["receiver", "enricher"],
        threshold_sec=90, now=NOW,
    )
    assert len(alerts) == 2
    assert all(a.rule == "missing_heartbeat" for a in alerts)


def test_missing_heartbeat_recent_no_alert():
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW - timedelta(seconds=10),
        ),
    }
    alerts = evaluate_missing_heartbeat(
        hb_state=state, tracked=["receiver"], threshold_sec=90, now=NOW,
    )
    assert alerts == []


def test_missing_heartbeat_stale_warn():
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW - timedelta(seconds=120),
        ),
    }
    alerts = evaluate_missing_heartbeat(
        hb_state=state, tracked=["receiver"], threshold_sec=90, now=NOW,
    )
    assert len(alerts) == 1
    assert alerts[0].severity == "warn"


def test_missing_heartbeat_very_stale_crit():
    """Gap > 3× threshold escalates to crit."""
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW - timedelta(seconds=400),
        ),
    }
    alerts = evaluate_missing_heartbeat(
        hb_state=state, tracked=["receiver"], threshold_sec=90, now=NOW,
    )
    assert alerts[0].severity == "crit"


def test_dlq_spike_no_history():
    """Single sample → cannot detect delta."""
    history = {"news:enriched:dlq": [(NOW, 5)]}
    alerts = evaluate_dlq_spike(history, threshold_delta=5, window_sec=300, now=NOW)
    assert alerts == []


def test_dlq_spike_detected():
    history = {
        "news:enriched:dlq": [
            (NOW - timedelta(seconds=400), 0),
            (NOW - timedelta(seconds=200), 3),
            (NOW, 20),
        ],
    }
    alerts = evaluate_dlq_spike(history, threshold_delta=10, window_sec=300, now=NOW)
    assert len(alerts) == 1
    assert "20" in alerts[0].message


def test_dlq_spike_below_threshold():
    history = {
        "news:enriched:dlq": [
            (NOW - timedelta(seconds=400), 0),
            (NOW, 3),
        ],
    }
    alerts = evaluate_dlq_spike(history, threshold_delta=10, window_sec=300, now=NOW)
    assert alerts == []


def test_telegram_storm_no_alert_when_inactive():
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW,
            last_snapshot={"service": "receiver", "tg_storm_active": "0"},
        ),
    }
    alerts = evaluate_telegram_reconnect_storm(state)
    assert alerts == []


def test_telegram_storm_no_alert_when_no_receiver():
    alerts = evaluate_telegram_reconnect_storm({})
    assert alerts == []


def test_telegram_storm_triggers_when_active():
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW,
            last_snapshot={
                "service": "receiver",
                "tg_storm_active": "1",
                "tg_reconnects_window": "23",
                "tg_reconnects_total": "147",
            },
        ),
    }
    alerts = evaluate_telegram_reconnect_storm(state)
    assert len(alerts) == 1
    assert alerts[0].rule == "telegram_reconnect_storm"
    assert alerts[0].severity == "warn"
    assert "23 reconnects" in alerts[0].message
    assert "147" in alerts[0].message


def test_telegram_storm_handles_missing_fields_gracefully():
    """If snapshot has storm_active=1 but counts missing, still alerts."""
    state = {
        "receiver": ServiceHeartbeatState(
            service="receiver",
            last_seen_utc=NOW,
            last_snapshot={"service": "receiver", "tg_storm_active": "1"},
        ),
    }
    alerts = evaluate_telegram_reconnect_storm(state)
    assert len(alerts) == 1
    assert alerts[0].rule == "telegram_reconnect_storm"


def test_daily_pnl_kill_no_alert():
    alerts = evaluate_daily_pnl_kill(
        daily_pnl_rub=-5000, initial_equity_rub=500_000, kill_pct=0.02,
    )
    assert alerts == []  # -1% < 2%


def test_daily_pnl_kill_negative_triggers():
    alerts = evaluate_daily_pnl_kill(
        daily_pnl_rub=-12000, initial_equity_rub=500_000, kill_pct=0.02,
    )
    assert len(alerts) == 1
    assert alerts[0].severity == "crit"


def test_daily_pnl_kill_positive_also_triggers():
    """Большой плюс тоже даёт alert (abs >= kill_pct)."""
    alerts = evaluate_daily_pnl_kill(
        daily_pnl_rub=15000, initial_equity_rub=500_000, kill_pct=0.02,
    )
    assert len(alerts) == 1
