"""Tests for TelegramHealthMonitor (Sprint 5.9).

Verifies that the logging-handler-based observer correctly counts
Telethon network events and flips storm flag on threshold crossing.
"""
from __future__ import annotations

import logging
import time

import pytest

from src.services.receiver.telegram_health import (
    STORM_THRESHOLD,
    STORM_WINDOW_SEC,
    TelegramHealthMonitor,
)


def _emit(logger_name: str, msg: str) -> None:
    """Drive a log event into telethon-namespaced loggers."""
    logging.getLogger(logger_name).warning(msg)


@pytest.fixture
def monitor():
    m = TelegramHealthMonitor()
    yield m
    m.close()


def test_initial_state_is_clean(monitor):
    snap = monitor.snapshot()
    assert snap["tg_disconnects_total"] == 0
    assert snap["tg_reconnects_total"] == 0
    assert snap["tg_storm_active"] == 0
    assert snap["tg_storms_detected_total"] == 0
    assert snap["tg_last_disconnect_age_sec"] == 999_999


def test_disconnect_event_counted(monitor):
    _emit("telethon.network.connection.connection",
          "Server closed the connection: 0 bytes read on a total of 8 expected bytes")
    snap = monitor.snapshot()
    assert snap["tg_disconnects_total"] == 1
    assert snap["tg_last_disconnect_age_sec"] < 5


def test_reconnect_event_counted(monitor):
    _emit("telethon.network.mtprotosender",
          "Connection to 149.154.175.54:443/TcpFull complete!")
    snap = monitor.snapshot()
    assert snap["tg_reconnects_total"] == 1
    assert snap["tg_reconnects_window"] == 1


def test_connect_failure_event_counted(monitor):
    _emit("telethon.network.mtprotosender",
          "Attempt 1 at connecting failed: TimeoutError:")
    snap = monitor.snapshot()
    assert snap["tg_connect_failures_total"] == 1


def test_storm_flag_flips_on_threshold(monitor):
    # Simulate a reconnect burst.
    for _ in range(STORM_THRESHOLD):
        _emit("telethon.network.mtprotosender",
              "Connection to 149.154.175.54:443/TcpFull complete!")
    snap = monitor.snapshot()
    assert snap["tg_storm_active"] == 1
    assert snap["tg_storms_detected_total"] == 1
    assert snap["tg_reconnects_window"] >= STORM_THRESHOLD


def test_storm_does_not_re_increment_while_active(monitor):
    """Storms_detected counter is a rising-edge counter."""
    for _ in range(STORM_THRESHOLD + 5):
        _emit("telethon.network.mtprotosender",
              "Connection to 149.154.175.54:443/TcpFull complete!")
    snap = monitor.snapshot()
    assert snap["tg_storm_active"] == 1
    # Still 1 detected event even though many reconnects.
    assert snap["tg_storms_detected_total"] == 1


def test_storm_flag_drops_when_window_drains(monitor):
    """If reconnects stop, after STORM_WINDOW_SEC the flag should drop."""
    # Inject ts that's already outside the window.
    now = time.time()
    old_ts = now - STORM_WINDOW_SEC - 30
    for _ in range(STORM_THRESHOLD):
        monitor._reconnect_window.append(old_ts)
    monitor._storm_active = True
    snap = monitor.snapshot()
    # After window cleanup, storm should be inactive.
    assert snap["tg_storm_active"] == 0
    assert snap["tg_reconnects_window"] == 0


def test_close_detaches_handlers(monitor):
    """After close(), new log events are not counted."""
    monitor.close()
    _emit("telethon.network.mtprotosender",
          "Connection to 149.154.175.54:443/TcpFull complete!")
    # Snapshot still callable, counter stays at previous value.
    snap = monitor.snapshot()
    assert snap["tg_reconnects_total"] == 0


def test_unrelated_log_messages_ignored(monitor):
    _emit("telethon.network.mtprotosender", "Some other unrelated debug info")
    _emit("telethon.network.mtprotosender", "Going to sleep for 0.5s")
    snap = monitor.snapshot()
    assert snap["tg_disconnects_total"] == 0
    assert snap["tg_reconnects_total"] == 0
    assert snap["tg_connect_failures_total"] == 0
