"""Lightweight counters for the Monitor service."""
from __future__ import annotations

from typing import Any, Dict


class MonitorMetrics:
    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}

    def inc(self, name: str, n: float = 1.0) -> None:
        self._counters[name] = self._counters.get(name, 0.0) + n

    def snapshot(self) -> Dict[str, Any]:
        return dict(self._counters)
