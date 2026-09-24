"""Counters для quik_feed."""
from __future__ import annotations

from collections import deque
from typing import Any, Dict


class QuikFeedMetrics:
    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._latest_bar_lag_sec: deque[float] = deque(maxlen=100)

    def inc(self, name: str, n: float = 1.0) -> None:
        self._counters[name] = self._counters.get(name, 0.0) + n

    def record_bar_lag_sec(self, lag: float) -> None:
        self._latest_bar_lag_sec.append(lag)

    def snapshot(self) -> Dict[str, Any]:
        snap: Dict[str, Any] = dict(self._counters)
        if self._latest_bar_lag_sec:
            arr = sorted(self._latest_bar_lag_sec)
            n = len(arr)
            snap["bar_lag_sec_p50"] = round(arr[n // 2], 1)
            snap["bar_lag_sec_p95"] = round(arr[min(n - 1, int(n * 0.95))], 1)
            snap["bar_lag_sec_n"] = n
        return snap
