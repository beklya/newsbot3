"""Quick inspector for Memurai/Redis streams.
    Quick inspector for the news:raw stream in Memurai/Redis.
    Usage from project root with .venv activated:

    python scripts/inspect_stream.py                          # news:raw, last 5
    python scripts/inspect_stream.py --count 20               # news:raw, last 20
    python scripts/inspect_stream.py --tail                   # full text
    python scripts/inspect_stream.py --hash 63da7195          # find by hash prefix
    python scripts/inspect_stream.py --stream system:heartbeats   # heartbeats
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from redis.asyncio import Redis

REDIS_URL = "redis://localhost:6379"
DEFAULT_STREAM = "news:raw"
HEARTBEAT_STREAM = "system:heartbeats"


def _fmt_news_event(xid: str, raw_data: str, *, full: bool) -> str:
    data = json.loads(raw_data)
    p = data["payload"]
    text = p["text"] if full else p["text"][:80].replace("\n", " ")
    return "\n".join([
        f"  xid       = {xid}",
        f"  event_id  = {data['event_id']}",
        f"  produced  = {data['produced_at']}",
        f"  channel   = {p['channel']}",
        f"  tg_msg_id = {p['message_id']}",
        f"  tg_at     = {p['tg_published_at']}",
        f"  hash      = {p['text_hash'][:16]}...",
        f"  flags     = media={p['has_media']} reply={p['is_reply']} fwd={p['is_forward']}",
        f"  trace     = {data.get('trace', [])}",
        f"  text      = {text}{'' if full else ' ...'}",
    ])


def _fmt_heartbeat(xid: str, fields: dict) -> str:
    # Heartbeat has flat key:value fields, not a nested JSON envelope
    parts = [f"  xid     = {xid}"]
    for k, v in fields.items():
        parts.append(f"  {k:<8}= {v}")
    return "\n".join(parts)


async def _count_idem_keys(redis: Redis, scope: str = "news_text") -> int:
    cursor = 0
    total = 0
    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=f"idem:{scope}:*", count=500)
        total += len(keys)
        if cursor == 0:
            break
    return total


async def main(args: argparse.Namespace) -> int:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        try:
            await redis.ping()
        except Exception as e:
            print(f"ERR: cannot reach Memurai/Redis at {REDIS_URL}: {e}")
            return 2

        stream = args.stream
        length = await redis.xlen(stream)
        is_news = stream == DEFAULT_STREAM

        print(f"=== {stream} ===")
        print(f"XLEN = {length}")

        if is_news:
            idem_count = await _count_idem_keys(redis)
            print(f"idempotency keys (idem:news_text:*) = {idem_count}")
            if length > 0 and idem_count < length:
                print("  WARN: idem_count < XLEN — duplicates may have leaked into stream")
        print()

        if length == 0:
            print("(stream empty)")
            return 0

        # Hash search only makes sense for news:raw
        if args.hash and is_news:
            print(f"Searching for events with hash starting with '{args.hash}'...\n")
            found = 0
            items = await redis.xrange(stream, "-", "+")
            for xid, fields in items:
                if json.loads(fields["data"])["payload"]["text_hash"].startswith(args.hash):
                    print(_fmt_news_event(xid, fields["data"], full=True))
                    print()
                    found += 1
            print(f"matches: {found}")
            return 0

        # Default: last N events newest first
        items = await redis.xrevrange(stream, count=args.count)
        print(f"Last {len(items)} events (newest first):\n")
        for xid, fields in items:
            if is_news:
                print(_fmt_news_event(xid, fields["data"], full=args.tail))
            else:
                print(_fmt_heartbeat(xid, fields))
            print()
        return 0
    finally:
        await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Memurai/Redis streams")
    parser.add_argument("--stream", default=DEFAULT_STREAM, help=f"Stream name (default: {DEFAULT_STREAM})")
    parser.add_argument("--count", type=int, default=5, help="How many events to show (default 5)")
    parser.add_argument("--tail", action="store_true", help="Print full text instead of preview")
    parser.add_argument("--hash", type=str, default=None, help="Find news events by text_hash prefix")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args)))
