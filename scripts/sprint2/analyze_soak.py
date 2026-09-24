"""Analyze a soak test of the receiver service.

Reads news:raw and system:heartbeats from Memurai/Redis and prints
a summary: throughput, channel breakdown, edit ratio, length stats,
heartbeat coverage, gaps.

Usage from project root with .venv activated:

    python scripts/analyze_soak.py                          # all-time
    python scripts/analyze_soak.py --hours 24               # last 24 hours
    python scripts/analyze_soak.py --since 2026-05-07       # since date (UTC)
    python scripts/analyze_soak.py --hours 24 > report.txt  # redirect to file
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

# Force UTF-8 on stdout. Without this, on Windows `python script.py > file.txt`
# uses cp1251 which can't encode Unicode glyphs (e.g. histogram blocks, emoji
# in channel titles). With this, the redirected file is valid UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from redis.asyncio import Redis  # noqa: E402

REDIS_URL = "redis://localhost:6379"


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _stream_id_from_dt(dt: datetime) -> str:
    return f"{int(dt.timestamp() * 1000)}-0"


async def _read_news(redis: Redis, since_id: str, until_id: str) -> list[dict]:
    items = await redis.xrange("news:raw", since_id, until_id)
    parsed: list[dict] = []
    for xid, fields in items:
        data = json.loads(fields["data"])
        p = data["payload"]
        parsed.append({
            "xid": xid,
            "tg_at": _parse_iso(p["tg_published_at"]),
            "received_at": _parse_iso(p["received_at"]),
            "produced_at": _parse_iso(data["produced_at"]),
            "channel": p["channel"],
            "msg_id": p["message_id"],
            "hash": p["text_hash"],
            "text_len": len(p["text"]),
            "has_media": p["has_media"],
            "is_forward": p["is_forward"],
        })
    return parsed


async def _read_heartbeats(redis: Redis, since_id: str, until_id: str) -> list[dict]:
    items = await redis.xrange("system:heartbeats", since_id, until_id)
    parsed: list[dict] = []
    for xid, fields in items:
        parsed.append({
            "xid": xid,
            "at": _parse_iso(fields["at"]),
            "service": fields.get("service"),
            "published": int(fields.get("published", 0)),
            "deduped": int(fields.get("deduped", 0)),
            "empty": int(fields.get("empty", 0)),
            "errors": int(fields.get("errors", 0)),
            "channels": int(fields.get("channels", 0)),
        })
    return parsed


def _print_news(news: list[dict], period: str) -> None:
    print(f"\n=== news:raw  -  {len(news)} events over {period} ===")
    if not news:
        return

    span_h = (news[-1]["tg_at"] - news[0]["tg_at"]).total_seconds() / 3600.0
    print(f"  span:    {span_h:.2f}h  ({news[0]['tg_at']}  ->  {news[-1]['tg_at']})")
    print(f"  rate:    {len(news) / max(span_h, 0.01):.1f} events/hour")

    by_channel = Counter(e["channel"] for e in news)
    print("\n  by channel:")
    for ch, n in by_channel.most_common():
        share = n * 100.0 / len(news)
        print(f"    {ch:<22} {n:>5}  ({share:.0f}%)")

    by_hour = Counter(e["tg_at"].hour for e in news)
    peak = max(by_hour.values())
    print("\n  by hour of day (UTC):")
    for h in sorted(by_hour):
        bar = "#" * int(by_hour[h] * 40 / peak)
        print(f"    {h:02d}:00  {by_hour[h]:>4}  {bar}")

    by_msg: dict[tuple, list[dict]] = defaultdict(list)
    for e in news:
        by_msg[(e["channel"], e["msg_id"])].append(e)
    edited_msgs = sum(1 for v in by_msg.values() if len(v) > 1)
    edit_events = sum(len(v) - 1 for v in by_msg.values() if len(v) > 1)
    print(f"\n  unique tg_msg_ids:  {len(by_msg)}")
    print(f"  edited messages:    {edited_msgs}  "
          f"({edited_msgs * 100.0 / max(len(by_msg), 1):.1f}% of unique)")
    print(f"  extra edit events:  {edit_events}")

    lengths = sorted(e["text_len"] for e in news)
    p50 = lengths[len(lengths) // 2]
    p95 = lengths[int(len(lengths) * 0.95)]
    p99 = lengths[int(len(lengths) * 0.99)]
    print(f"\n  text length:  min={min(lengths)}  p50={p50}  p95={p95}  p99={p99}  max={max(lengths)}")

    e2e = sorted((e["produced_at"] - e["tg_at"]).total_seconds() for e in news)
    e50 = e2e[len(e2e) // 2]
    e95 = e2e[int(len(e2e) * 0.95)]
    print(f"  e2e latency:  p50={e50:.1f}s  p95={e95:.1f}s  (Telegram publish -> Memurai)")


def _print_heartbeats(hbs: list[dict], period: str) -> None:
    print(f"\n=== system:heartbeats  -  {len(hbs)} pings over {period} ===")
    if not hbs:
        return

    first_at = hbs[0]["at"]
    last_at = hbs[-1]["at"]
    span_h = (last_at - first_at).total_seconds() / 3600.0

    gaps = [(curr["at"] - prev["at"]).total_seconds() for prev, curr in zip(hbs, hbs[1:])]

    if gaps:
        avg = sum(gaps) / len(gaps)
        long_gaps = [g for g in gaps if g > 60]
        print(f"  span:           {span_h:.2f}h  ({first_at}  ->  {last_at})")
        print(f"  interval:       avg={avg:.1f}s  max={max(gaps):.1f}s")
        print(f"  gaps > 60s:     {len(long_gaps)}  (likely freezes / restarts)")
        if long_gaps:
            for g in sorted(long_gaps, reverse=True)[:5]:
                print(f"      gap={g:.1f}s")

    final = hbs[-1]
    print(f"\n  final counters (from last heartbeat):")
    print(f"    published = {final['published']}")
    print(f"    deduped   = {final['deduped']}")
    print(f"    empty     = {final['empty']}")
    print(f"    errors    = {final['errors']}")
    print(f"    channels  = {final['channels']}")

    if final["published"] + final["deduped"] > 0:
        ratio = final["deduped"] * 100.0 / (final["published"] + final["deduped"])
        print(f"    dedup rate = {ratio:.1f}%")


async def _main(args: argparse.Namespace) -> int:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        try:
            await redis.ping()
        except Exception as e:
            print(f"ERR: cannot reach Memurai at {REDIS_URL}: {e}")
            return 2

        if args.hours:
            since = datetime.now(timezone.utc) - timedelta(hours=args.hours)
            since_id = _stream_id_from_dt(since)
            period = f"last {args.hours}h"
        elif args.since:
            since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
            since_id = _stream_id_from_dt(since)
            period = f"since {args.since}"
        else:
            since_id = "-"
            period = "all-time"

        news = await _read_news(redis, since_id, "+")
        hbs = await _read_heartbeats(redis, since_id, "+")

        _print_news(news, period)
        _print_heartbeats(hbs, period)
        return 0
    finally:
        await redis.aclose()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Analyze receiver soak test from Memurai")
    p.add_argument("--hours", type=int, default=None, help="Last N hours")
    p.add_argument("--since", type=str, default=None, help="Since YYYY-MM-DD (UTC)")
    args = p.parse_args()
    sys.exit(asyncio.run(_main(args)))