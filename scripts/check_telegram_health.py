"""Telegram MTProto health checker — diagnostic CLI for cron / manual use.

Reads the most recent Receiver heartbeat from Redis system:heartbeats stream
and reports on Telethon ↔ Telegram connection stability.

Output: one-line status + multi-line breakdown.
Exit codes (suitable for cron / monitoring chain):
    0 — healthy   (no active storm, recent disconnect age > 300s)
    1 — warning   (recent reconnect activity but below storm threshold)
    2 — storm     (tg_storm_active=1, active Telegram MTProto outage)
    3 — no data   (Receiver never sent heartbeats, или старее 5 минут)

Usage:
    python scripts/check_telegram_health.py
    python scripts/check_telegram_health.py --json
    python scripts/check_telegram_health.py --url redis://other:6379

Examples (Windows Task Scheduler):
    Every 5 min run:
      python scripts\\check_telegram_health.py >> data\\tg_health.log
    Exit code != 0 → trigger separate action.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Optional

import redis

_HB_STREAM = "system:heartbeats"
_RECEIVER_SVC = "receiver"
_HEARTBEAT_STALE_SEC = 300  # consider data stale (= no_data) if older than this


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Sprint 5.11: default читается из REDIS_URL env var (для SSH tunnel
    # к VPS-Redis), CLI --url имеет приоритет.
    p.add_argument("--url",
                   default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                   help="Redis URL (default: $REDIS_URL or localhost:6379)")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON instead of human text")
    p.add_argument("--quiet", action="store_true",
                   help="Print only one-line summary, no breakdown")
    return p.parse_args()


def _decode_field(v: Any) -> str:
    return v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)


def find_latest_receiver_snapshot(r: redis.Redis) -> Optional[dict]:
    """Walk backwards through system:heartbeats until we find one from receiver."""
    try:
        entries = r.xrevrange(_HB_STREAM, count=500)
    except Exception as e:
        print(f"FATAL: cannot read {_HB_STREAM}: {e}", file=sys.stderr)
        return None
    for _msg_id, fields in entries:
        # Decode keys/values
        decoded = {_decode_field(k): _decode_field(v) for k, v in fields.items()}
        if decoded.get("service") == _RECEIVER_SVC:
            return decoded
    return None


def _safe_int(d: dict, key: str, default: int = 0) -> int:
    try:
        return int(d.get(key, default))
    except (TypeError, ValueError):
        return default


def classify(snap: dict) -> tuple[int, str]:
    """Return (exit_code, status_label) given a receiver snapshot."""
    # Parse heartbeat 'at' to check freshness.
    at_str = snap.get("at", "")
    from datetime import datetime, timezone
    try:
        at_dt = datetime.fromisoformat(at_str)
        if at_dt.tzinfo is None:
            at_dt = at_dt.replace(tzinfo=timezone.utc)
        hb_age = (datetime.now(timezone.utc) - at_dt).total_seconds()
    except (TypeError, ValueError):
        return 3, "no_data"

    if hb_age > _HEARTBEAT_STALE_SEC:
        return 3, "no_data"

    storm_active = _safe_int(snap, "tg_storm_active")
    window = _safe_int(snap, "tg_reconnects_window")
    last_disc_age = _safe_int(snap, "tg_last_disconnect_age_sec", default=999_999)

    if storm_active == 1:
        return 2, "storm"
    # Warning if any recent disconnect activity (within 5 min) or any
    # reconnects in the current 60s window even below threshold.
    if window > 0 or last_disc_age < 300:
        return 1, "warning"
    return 0, "healthy"


def human_output(snap: Optional[dict], code: int, label: str, hb_url: str) -> str:
    lines: list[str] = []
    lines.append(f"telegram_health: {label.upper()}  exit_code={code}  redis={hb_url}")
    if snap is None:
        lines.append("  no receiver heartbeat found in stream " + _HB_STREAM)
        lines.append("  → receiver service not running, or hasn't published yet")
        return "\n".join(lines)
    lines.append(f"  heartbeat_at:                {snap.get('at', '?')}")
    lines.append(f"  channels:                    {snap.get('channels', '?')}")
    lines.append(f"  published:                   {snap.get('published', '?')}")
    lines.append("")
    lines.append("  tg_storm_active:             "
                 f"{snap.get('tg_storm_active', '?')}")
    lines.append("  tg_reconnects_window (60s):  "
                 f"{snap.get('tg_reconnects_window', '?')}")
    lines.append("  tg_disconnects_total:        "
                 f"{snap.get('tg_disconnects_total', '?')}")
    lines.append("  tg_reconnects_total:         "
                 f"{snap.get('tg_reconnects_total', '?')}")
    lines.append("  tg_connect_failures_total:   "
                 f"{snap.get('tg_connect_failures_total', '?')}")
    lines.append("  tg_storms_detected_total:    "
                 f"{snap.get('tg_storms_detected_total', '?')}")
    lines.append("  tg_last_disconnect_age_sec:  "
                 f"{snap.get('tg_last_disconnect_age_sec', '?')}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    r = redis.Redis.from_url(args.url, decode_responses=False)
    try:
        r.ping()
    except Exception as e:
        print(f"FATAL: redis unreachable at {args.url}: {e}", file=sys.stderr)
        return 3

    snap = find_latest_receiver_snapshot(r)
    if snap is None:
        code, label = 3, "no_data"
    else:
        code, label = classify(snap)

    if args.json:
        payload = {
            "status": label,
            "exit_code": code,
            "redis_url": args.url,
            "ts": int(time.time()),
            "snapshot": snap or {},
        }
        print(json.dumps(payload, ensure_ascii=False))
    elif args.quiet:
        print(f"telegram_health: {label.upper()}  exit_code={code}")
    else:
        print(human_output(snap, code, label, args.url))
    return code


if __name__ == "__main__":
    sys.exit(main())
