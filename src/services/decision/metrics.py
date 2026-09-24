"""Lightweight counters for the Decision service."""
from __future__ import annotations

from collections import deque
from typing import Any, Dict

_LATENCY_WINDOW = 1000


class DecisionMetrics:
    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._latencies: deque[float] = deque(maxlen=_LATENCY_WINDOW)

    def inc(self, name: str, n: float = 1.0) -> None:
        self._counters[name] = self._counters.get(name, 0.0) + n

    def record_latency_ms(self, ms: float) -> None:
        self._latencies.append(ms)

    def snapshot(self) -> Dict[str, Any]:
        snap: Dict[str, Any] = dict(self._counters)
        if self._latencies:
            arr = sorted(self._latencies)
            n = len(arr)
            snap["latency_ms_p50"] = arr[n // 2]
            snap["latency_ms_p95"] = arr[min(n - 1, int(n * 0.95))]
            snap["latency_ms_n"] = n
        return snap
