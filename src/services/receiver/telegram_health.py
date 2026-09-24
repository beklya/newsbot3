"""TelegramHealthMonitor — hooks into telethon loggers to track MTProto
connection stability.

Why this is needed:
  Telethon owns the MTProto sender and reconnects transparently. From
  Receiver's point of view, news still arrive (Telethon catches up via
  GetDifference). But during reconnect storms (Telegram dropping
  connections every ~5s for tens of minutes — see Sprint 5 paper soak
  diagnostics), pipeline throughput goes to zero with no signal to
  Monitor.

Approach:
  Attach a logging.Handler to telethon's network loggers. Pattern-match
  log messages to count:
    - "Server closed the connection" → disconnect event
    - "Connection to ... complete!" → reconnect event
    - "Attempt N at connecting failed" → connect failure

Sliding window (last STORM_WINDOW_SEC seconds) decides storm vs healthy.

Exposed counters (via snapshot()):
    tg_disconnects_total          monotonic count
    tg_reconnects_total           monotonic count
    tg_connect_failures_total     monotonic count
    tg_storms_detected_total      monotonic, increments on rising edge
    tg_storm_active               0/1 — currently in storm
    tg_reconnects_window          count in last STORM_WINDOW_SEC
    tg_last_disconnect_age_sec    int (=999999 if never)

The Monitor service reads these from receiver's heartbeat snapshot and
raises an alert when tg_storm_active=1 (see Sprint 5.9).
"""
from __future__ import annotations

import logging
import re
import time
from collections import deque
from typing import Deque, Dict

# Pattern matching on Telethon log messages (formatted strings).
# Resilient to bytes/total counts in messages.
_RE_DISCONNECT = re.compile(r"Server closed the connection|Connection closed while receiving")
_RE_RECONNECT = re.compile(r"Connection to .* complete!")
_RE_CONNECT_FAIL = re.compile(r"Attempt \d+ at connecting failed")

# A storm is N reconnects in M seconds.
STORM_WINDOW_SEC = 60
STORM_THRESHOLD = 10
# Logger to attach handler to. We attach ONLY to the umbrella
# "telethon.network" — Python logger propagation ensures we see
# events from all child loggers (mtprotosender, connection.connection,
# etc.). Attaching to children separately would double-count.
_TELETHON_LOGGERS = (
    "telethon.network",
)


class TelegramHealthMonitor(logging.Handler):
    """Logging handler that counts reconnect events from Telethon loggers.

    Self-registers on construction (attaches to telethon loggers). To stop,
    call close().
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)  # capture INFO+WARNING

        # Monotonic counters.
        self.disconnects_total: int = 0
        self.reconnects_total: int = 0
        self.connect_failures_total: int = 0
        self.storms_detected_total: int = 0

        # Storm-edge state.
        self._storm_active: bool = False
        self._last_disconnect_ts: float | None = None

        # Sliding window of reconnect timestamps (epoch seconds).
        self._reconnect_window: Deque[float] = deque(maxlen=1000)

        # Track which loggers we hooked (for clean detach in close()).
        self._hooked: list[logging.Logger] = []
        self.attach()

    # ----- lifecycle -----

    def attach(self) -> None:
        for name in _TELETHON_LOGGERS:
            lg = logging.getLogger(name)
            lg.addHandler(self)
            # Don't lower telethon's effective level; we observe whatever
            # the existing logging config emits.
            self._hooked.append(lg)

    def close(self) -> None:
        for lg in self._hooked:
            try:
                lg.removeHandler(self)
            except Exception:
                pass
        self._hooked.clear()
        super().close()

    # ----- log handler interface -----

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:
            return
        now = time.time()
        if _RE_DISCONNECT.search(msg):
            self.disconnects_total += 1
            self._last_disconnect_ts = now
        elif _RE_RECONNECT.search(msg):
            self.reconnects_total += 1
            self._reconnect_window.append(now)
            self._update_storm_state(now)
        elif _RE_CONNECT_FAIL.search(msg):
            self.connect_failures_total += 1

    # ----- storm detection -----

    def _update_storm_state(self, now: float) -> None:
        cutoff = now - STORM_WINDOW_SEC
        # Drop stale entries.
        while self._reconnect_window and self._reconnect_window[0] < cutoff:
            self._reconnect_window.popleft()
        count = len(self._reconnect_window)
        is_storming = count >= STORM_THRESHOLD
        if is_storming and not self._storm_active:
            self._storm_active = True
            self.storms_detected_total += 1  # rising edge
        elif not is_storming and self._storm_active:
            # Falling edge — back to healthy.
            self._storm_active = False

    # ----- snapshot for heartbeat -----

    def snapshot(self) -> Dict[str, int]:
        # Recompute window count on read (in case no recent emit).
        now = time.time()
        cutoff = now - STORM_WINDOW_SEC
        while self._reconnect_window and self._reconnect_window[0] < cutoff:
            self._reconnect_window.popleft()
        # Also re-evaluate storm flag on read (otherwise it stays True after
        # the loop dies down with no new events).
        self._update_storm_state(now)

        if self._last_disconnect_ts is None:
            age = 999_999
        else:
            age = int(now - self._last_disconnect_ts)
        return {
            "tg_disconnects_total": self.disconnects_total,
            "tg_reconnects_total": self.reconnects_total,
            "tg_connect_failures_total": self.connect_failures_total,
            "tg_storms_detected_total": self.storms_detected_total,
            "tg_storm_active": 1 if self._storm_active else 0,
            "tg_reconnects_window": len(self._reconnect_window),
            "tg_last_disconnect_age_sec": age,
        }
