"""Pull news:enriched events from VPS Redis through SSH tunnel.

Filters by news_time (or tg_published_at fallback) within [--from, --to).
Saves as JSONL — one MessageEnvelope per line — for offline replay.

Usage:
    python scripts/pull_enriched_from_vps.py --from 2026-05-23 --to 2026-06-06
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import redis

DEFAULT_TUNNEL = "redis://127.0.0.1:6380"


def parse_iso_utc(s: str) -> datetime:
    """Parse YYYY-MM-DD or ISO datetime, treat naive as UTC."""
    if "T" in s:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.strptime(s, "%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_event_news_time(envelope: dict) -> datetime | None:
    """Extract event news_time / tg_published_at / produced_at as UTC datetime."""
    payload = envelope.get("payload") or {}
    candidates = [
        payload.get("news_time"),
        payload.get("tg_published_at"),
        envelope.get("produced_at"),
    ]
    for c in candidates:
        if not c:
            continue
        try:
            dt = datetime.fromisoformat(str(c).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (TypeError, ValueError):
            continue
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD UTC")
    ap.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD UTC (exclusive)")
    ap.add_argument("--url", default=DEFAULT_TUNNEL)
    ap.add_argument("--stream", default="news:enriched")
    ap.add_argument("--output", type=Path,
                    default=Path("data/replay/enriched_vps_window.jsonl"))
    args = ap.parse_args()

    ts_from = parse_iso_utc(args.date_from)
    ts_to = parse_iso_utc(args.date_to)

    r = redis.Redis.from_url(args.url, decode_responses=False,
                             socket_connect_timeout=5, socket_timeout=30)
    print(f"Connected to {args.url} — PING: {r.ping()}")
    total = r.xlen(args.stream)
    print(f"{args.stream}: {total} total entries on VPS")
    print(f"Window:  {ts_from.isoformat()}  ->  {ts_to.isoformat()}")
    print()

    args.output.parent.mkdir(parents=True, exist_ok=True)

    # XRANGE — pull in batches of 1000 (manual cursor on stream IDs)
    saved = 0
    seen = 0
    out_of_range_before = 0
    out_of_range_after = 0
    no_news_time = 0
    by_date: dict[str, int] = {}
    cursor = "-"
    last_seen_id = None

    with args.output.open("w", encoding="utf-8") as fout:
        while True:
            batch = r.xrange(args.stream, min=cursor, max="+", count=1000)
            if not batch:
                break
            # XRANGE is inclusive on both ends — skip the boundary entry we
            # already processed in the previous batch.
            new_in_batch = 0
            for entry_id_bytes, fields in batch:
                entry_id = entry_id_bytes.decode()
                if last_seen_id is not None and entry_id == last_seen_id:
                    continue
                last_seen_id = entry_id
                new_in_batch += 1
                seen += 1

                raw = fields.get(b"data") or fields.get(b"envelope")
                if not raw:
                    continue
                try:
                    env = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                ntime = get_event_news_time(env)
                if ntime is None:
                    no_news_time += 1
                    continue
                if ntime < ts_from:
                    out_of_range_before += 1
                    continue
                if ntime >= ts_to:
                    out_of_range_after += 1
                    continue

                date_key = ntime.date().isoformat()
                by_date[date_key] = by_date.get(date_key, 0) + 1
                fout.write(json.dumps({
                    "stream_id": entry_id,
                    "envelope": env,
                    "news_time_utc": ntime.isoformat(),
                }, ensure_ascii=False) + "\n")
                saved += 1

            cursor = last_seen_id  # next batch starts at this id (will skip it)
            if new_in_batch == 0 or len(batch) < 1000:
                break

    print("---- Pull summary ----")
    print(f"  seen        : {seen}")
    print(f"  no news_time: {no_news_time}")
    print(f"  before window: {out_of_range_before}")
    print(f"  after window : {out_of_range_after}")
    print(f"  IN WINDOW    : {saved}  -> {args.output}")
    print()
    print("By date:")
    for d in sorted(by_date.keys()):
        print(f"  {d}: {by_date[d]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
