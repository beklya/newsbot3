"""Soak analyzer: aggregates heartbeats + DLQ + enriched events over a time range.

Reads `system:heartbeats` stream filtered by service=enricher, computes:
- Throughput (events/hour bucketed)
- Error rates per kind
- Latency p50/p95 trend
- Pool cooldown episodes
- DLQ samples grouped by error_kind
- Pending state at end

Usage:
    python scripts/analyze_soak.py                              # last 24h
    python scripts/analyze_soak.py --hours 6                    # last 6h
    python scripts/analyze_soak.py --from-id <stream_id>        # since specific stream id
    python scripts/analyze_soak.py --json                       # raw json output for piping
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Python 3.14 + Windows asyncio fix (см. scripts/redis_inspect.py).
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from redis.asyncio import Redis  # noqa: E402

# Sprint 5.11: REDIS_URL из env для работы через SSH tunnel к VPS-Redis.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

HEARTBEAT_STREAM = "system:heartbeats"
ENRICHED_STREAM = "news:enriched"
DLQ_STREAM = "news:enriched:dlq"
RAW_STREAM = "news:raw"
CONSUMER_GROUP = "enricher"


def _dec(v):
    if v is None:
        return ""
    return v.decode() if isinstance(v, bytes) else str(v)


def _try_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _try_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------

@dataclass
class HeartbeatSnap:
    """One parsed heartbeat record."""
    stream_id: str
    at_iso: str
    fields: dict[str, str] = field(default_factory=dict)

    @property
    def ts_ms(self) -> int:
        """Stream ID's milli-timestamp prefix."""
        try:
            return int(self.stream_id.split("-")[0])
        except (ValueError, IndexError):
            return 0


def parse_heartbeat(stream_id, fields) -> HeartbeatSnap:
    decoded = {_dec(k): _dec(v) for k, v in fields.items()}
    return HeartbeatSnap(
        stream_id=_dec(stream_id),
        at_iso=decoded.get("at", ""),
        fields=decoded,
    )


# ---------------------------------------------------------------------------

async def read_heartbeats_in_range(
    redis: Redis, min_id: str, max_id: str = "+",
) -> list[HeartbeatSnap]:
    """Read enricher heartbeats in stream-id range."""
    msgs = await redis.xrange(HEARTBEAT_STREAM, min=min_id, max=max_id)
    snaps = []
    for sid, f in msgs:
        snap = parse_heartbeat(sid, f)
        if snap.fields.get("service") != "enricher":
            continue
        snaps.append(snap)
    return snaps


async def dlq_summary(redis: Redis, max_id: str = "+", min_id: str = "-") -> dict[str, Any]:
    """Aggregate DLQ entries — counts per error_kind + 3 samples per kind."""
    msgs = await redis.xrange(DLQ_STREAM, min=min_id, max=max_id)
    counts: dict[str, int] = defaultdict(int)
    samples: dict[str, list[dict]] = defaultdict(list)
    for sid, f in msgs:
        kind = _dec(f.get(b"error_kind", b"unknown"))
        counts[kind] += 1
        if len(samples[kind]) < 3:
            samples[kind].append({
                "stream_id": _dec(sid),
                "raw_event_id": _dec(f.get(b"raw_event_id")),
                "channel": _dec(f.get(b"channel")),
                "text_preview": _dec(f.get(b"raw_text_preview"))[:150],
                "error_message": _dec(f.get(b"error_message"))[:200],
                "occurred_at": _dec(f.get(b"occurred_at")),
            })
    return {
        "total": len(msgs),
        "by_kind": dict(counts),
        "samples": dict(samples),
    }


async def pending_state(redis: Redis) -> dict[str, Any]:
    """Snapshot of news:raw PEL for the enricher group."""
    try:
        summary = await redis.xpending(RAW_STREAM, CONSUMER_GROUP)
    except Exception as e:
        return {"error": str(e)}
    if not summary or not summary.get("pending"):
        return {"pending": 0}
    return {
        "pending": summary.get("pending", 0),
        "min": _dec(summary.get("min")),
        "max": _dec(summary.get("max")),
        "consumers": [
            {"name": _dec(c.get("name") if isinstance(c, dict) else c[0]),
             "pending": c.get("pending") if isinstance(c, dict) else c[1]}
            for c in (summary.get("consumers") or [])
        ],
    }


# ---------------------------------------------------------------------------

def compute_throughput(snaps: list[HeartbeatSnap]) -> dict[str, Any]:
    """Compute events/hour from first and last heartbeat counters."""
    if len(snaps) < 2:
        return {"hours_covered": 0.0, "events_in": 0, "events_out": 0, "rate_per_hour": 0.0}
    first = snaps[0]
    last = snaps[-1]
    span_sec = (last.ts_ms - first.ts_ms) / 1000.0
    hours = span_sec / 3600.0
    in_delta = _try_int(last.fields.get("events_in", 0)) - _try_int(first.fields.get("events_in", 0))
    out_delta = _try_int(last.fields.get("events_out", 0)) - _try_int(first.fields.get("events_out", 0))
    rate = (in_delta / hours) if hours > 0 else 0.0
    return {
        "hours_covered": round(hours, 2),
        "first_at": first.at_iso,
        "last_at": last.at_iso,
        "events_in_delta": in_delta,
        "events_out_delta": out_delta,
        "rate_per_hour": round(rate, 2),
    }


def compute_error_rates(snaps: list[HeartbeatSnap]) -> dict[str, Any]:
    """Sum of errors.* deltas by kind."""
    if len(snaps) < 2:
        return {}
    first = snaps[0].fields
    last = snaps[-1].fields
    error_keys = {k for k in last if k.startswith("errors.")}
    deltas = {}
    for k in error_keys:
        d = _try_int(last.get(k, 0)) - _try_int(first.get(k, 0))
        if d != 0:
            deltas[k] = d
    in_delta = _try_int(last.get("events_in", 0)) - _try_int(first.get("events_in", 0))
    total_errors = sum(deltas.values())
    rate = (total_errors / in_delta) if in_delta else 0.0
    return {
        "events_in_delta": in_delta,
        "total_errors_delta": total_errors,
        "error_rate_pct": round(rate * 100, 2),
        "by_kind_delta": deltas,
        "dlq_delta": _try_int(last.get("dlq_total", 0)) - _try_int(first.get("dlq_total", 0)),
    }


def compute_latency_trend(snaps: list[HeartbeatSnap], buckets: int = 6) -> list[dict]:
    """Bucket latency p50/p95 by time into `buckets` periods.

    Each heartbeat snapshot has CURRENT p50/p95 (over its rolling sample buffer).
    Это не аккумулятивно — это «снимок последних 1000 сэмплов на момент сэйва».
    So this gives a time-series view of latency.
    """
    if not snaps:
        return []
    span_ms = snaps[-1].ts_ms - snaps[0].ts_ms
    if span_ms == 0:
        # All in one moment — single bucket.
        return [{
            "from": snaps[0].at_iso,
            "to": snaps[-1].at_iso,
            "samples": len(snaps),
            "avg_p50_ms": int(sum(_try_int(s.fields.get("latency_p50_ms", 0)) for s in snaps) / max(1, len(snaps))),
            "avg_p95_ms": int(sum(_try_int(s.fields.get("latency_p95_ms", 0)) for s in snaps) / max(1, len(snaps))),
        }]
    bucket_size_ms = span_ms / buckets
    result = []
    for i in range(buckets):
        start_ms = snaps[0].ts_ms + int(i * bucket_size_ms)
        end_ms = snaps[0].ts_ms + int((i + 1) * bucket_size_ms)
        in_bucket = [s for s in snaps if start_ms <= s.ts_ms <= end_ms]
        if not in_bucket:
            continue
        result.append({
            "from": in_bucket[0].at_iso,
            "to": in_bucket[-1].at_iso,
            "samples": len(in_bucket),
            "avg_p50_ms": int(sum(_try_int(s.fields.get("latency_p50_ms", 0)) for s in in_bucket) / len(in_bucket)),
            "avg_p95_ms": int(sum(_try_int(s.fields.get("latency_p95_ms", 0)) for s in in_bucket) / len(in_bucket)),
        })
    return result


def compute_pool_cooldowns(snaps: list[HeartbeatSnap]) -> dict[str, Any]:
    """Count heartbeats where pool_n_cooldown > 0 (and full cooldown = whole pool)."""
    any_cooldown = 0
    full_cooldown = 0  # все клиенты пула в cooldown
    for s in snaps:
        cd = _try_int(s.fields.get("pool_n_cooldown", 0))
        total = _try_int(s.fields.get("pool_n_total", 0))
        if cd > 0:
            any_cooldown += 1
        if total > 0 and cd >= total:
            full_cooldown += 1
    return {
        "heartbeats_total": len(snaps),
        "heartbeats_with_cooldown": any_cooldown,
        "heartbeats_full_cooldown": full_cooldown,
        "cooldown_pct": round((any_cooldown / max(1, len(snaps))) * 100, 1),
    }


# ---------------------------------------------------------------------------

async def run_analysis(hours: int | None, from_id: str | None) -> dict[str, Any]:
    redis = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        # Compute min stream-id.
        if from_id:
            min_id = from_id
        elif hours is not None:
            min_ms = int((time.time() - hours * 3600) * 1000)
            min_id = f"{min_ms}-0"
        else:
            # Default: 24h
            min_ms = int((time.time() - 24 * 3600) * 1000)
            min_id = f"{min_ms}-0"

        snaps = await read_heartbeats_in_range(redis, min_id)
        dlq = await dlq_summary(redis)
        pending = await pending_state(redis)

        return {
            "heartbeats_count": len(snaps),
            "throughput": compute_throughput(snaps),
            "error_rates": compute_error_rates(snaps),
            "latency_trend": compute_latency_trend(snaps),
            "pool_cooldowns": compute_pool_cooldowns(snaps),
            "dlq": dlq,
            "pending": pending,
        }
    finally:
        await redis.aclose()


def print_human(report: dict[str, Any]) -> None:
    print("\n" + "=" * 72)
    print("SOAK ANALYSIS")
    print("=" * 72)

    print(f"\nHeartbeats collected: {report['heartbeats_count']}")

    t = report["throughput"]
    print(f"\n--- Throughput ---")
    print(f"  Period: {t.get('hours_covered', 0)}h ({t.get('first_at', '?')} → {t.get('last_at', '?')})")
    print(f"  events_in delta : {t.get('events_in_delta', 0)}")
    print(f"  events_out delta: {t.get('events_out_delta', 0)}")
    print(f"  rate            : {t.get('rate_per_hour', 0)} events/hour")

    er = report["error_rates"]
    print(f"\n--- Errors (delta over period) ---")
    print(f"  Total errors: {er.get('total_errors_delta', 0)}")
    print(f"  Error rate  : {er.get('error_rate_pct', 0)}%")
    print(f"  DLQ delta   : {er.get('dlq_delta', 0)}")
    by_kind = er.get("by_kind_delta", {})
    if by_kind:
        print("  By kind:")
        for k, v in sorted(by_kind.items(), key=lambda x: -x[1]):
            print(f"    {k}: {v}")

    print(f"\n--- Latency trend (p50/p95 ms over time) ---")
    for b in report["latency_trend"]:
        print(f"  [{b['from'][:19]}] p50={b['avg_p50_ms']}  p95={b['avg_p95_ms']}  ({b['samples']} hb)")

    pc = report["pool_cooldowns"]
    print(f"\n--- Pool cooldowns ---")
    print(f"  Heartbeats with any cooldown : {pc['heartbeats_with_cooldown']} / {pc['heartbeats_total']} ({pc['cooldown_pct']}%)")
    print(f"  Heartbeats with FULL cooldown: {pc['heartbeats_full_cooldown']}")

    dlq = report["dlq"]
    print(f"\n--- DLQ (last entries in stream) ---")
    print(f"  Total: {dlq['total']}")
    if dlq["by_kind"]:
        print("  By kind:")
        for k, v in sorted(dlq["by_kind"].items(), key=lambda x: -x[1]):
            print(f"    {k}: {v}")
        for kind, samples in dlq["samples"].items():
            print(f"\n  Examples of '{kind}':")
            for s in samples:
                print(f"    [{s['occurred_at']}] {s['channel']}")
                print(f"      → {s['text_preview']}")
                if s.get('error_message'):
                    print(f"      err: {s['error_message']}")

    p = report["pending"]
    print(f"\n--- Consumer group pending (current state) ---")
    if p.get("pending", 0) == 0:
        print("  ✅ No pending messages")
    else:
        print(f"  ⚠️  Total pending: {p['pending']}")
        print(f"     Range: {p.get('min')} … {p.get('max')}")
        for c in p.get("consumers", []):
            print(f"     {c['name']}: {c['pending']}")

    print("\n" + "=" * 72)


async def main():
    parser = argparse.ArgumentParser(description="Analyze enricher soak run")
    parser.add_argument("--hours", type=int, default=None, help="Look back N hours (default 24)")
    parser.add_argument("--from-id", type=str, default=None, help="Start from this stream ID")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of human-readable")
    args = parser.parse_args()

    report = await run_analysis(args.hours, args.from_id)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_human(report)


if __name__ == "__main__":
    asyncio.run(main())
