"""In-memory metrics for the Enricher service.

Two kinds of measurements:
1. Counters — integer events: events_in, events_out, errors.* — incremented in place.
2. Latency samples — float ms, ring buffer 1000 last samples → p50/p95 percentile.

snapshot() returns a flat dict suitable for HeartbeatPublisher (one xadd per snapshot).
Reset semantics: counters survive across heartbeats but reset on process restart.
For long-term accumulation, soak analyzer reads heartbeats stream directly.
"""

from __future__ import annotations

import time
from collections import Counter, deque
from typing import Any

# Ring buffer size for latency samples. 1000 is enough for stable p95
# at our scale (367 events/day = ~one event every 4 minutes).
LATENCY_BUFFER_SIZE = 1000


class EnricherMetrics:
    """Lightweight metrics aggregator. Not thread-safe — single asyncio loop only."""

    def __init__(self) -> None:
        self._counters: Counter[str] = Counter()
        self._latencies_ms: deque[float] = deque(maxlen=LATENCY_BUFFER_SIZE)
        self._started_at: float = time.monotonic()

    # --- counters ---

    def inc(self, key: str, n: int = 1) -> None:
        """Increment a named counter. Dotted names are fine: errors.invalid_json."""
        self._counters[key] += n

    def get(self, key: str) -> int:
        return self._counters[key]

    # --- latencies ---

    def record_latency_ms(self, latency_ms: float) -> None:
        """Add one latency sample (Groq round-trip ms)."""
        if latency_ms < 0:
            return
        self._latencies_ms.append(latency_ms)

    def _percentile(self, p: float) -> float:
        """Compute percentile from current samples. Returns 0.0 if empty.

        Uses nearest-rank — simple and accurate enough for ops dashboards.
        """
        if not self._latencies_ms:
            return 0.0
        sorted_latencies = sorted(self._latencies_ms)
        n = len(sorted_latencies)
        # rank: ceil(p/100 * n), 1-indexed
        rank = max(1, min(n, int(p / 100.0 * n + 0.5)))
        return sorted_latencies[rank - 1]

    # --- snapshot ---

    def snapshot(self) -> dict[str, Any]:
        """Flat dict for HeartbeatPublisher. Keys are strings, values are ints/floats."""
        uptime_sec = int(time.monotonic() - self._started_at)
        snap: dict[str, Any] = {
            "uptime_sec": uptime_sec,
            "latency_p50_ms": int(self._percentile(50.0)),
            "latency_p95_ms": int(self._percentile(95.0)),
            "latency_samples": len(self._latencies_ms),
        }
        # All counters flattened with their full key. Empty counter → snapshot
        # still includes nothing but core fields above, which is fine.
        for k, v in self._counters.items():
            snap[k] = v
        return snap
