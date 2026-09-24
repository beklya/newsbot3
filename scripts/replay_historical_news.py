"""Replay historical Telegram news through the live pipeline.

Reads `D:\\quik_sber\\newsbot\\duble3\\telegram_news.jsonl` (860k records since 2016),
samples N random news within a date range, builds RawNewsEvent for each, and
publishes them to `news:raw` stream. The live Enricher/Predictor/Decision/Bridge
pick them up automatically and process exactly as if they came from Telegram.

Caveats:
  - Each replay event = ~5k Groq input tokens. 1000 events = ~5M tokens.
    Live news will queue behind.
  - Predictor needs candles around news_time. For news outside the Phase 2 prices
    range (`D:\\quik_sber\\newsbot\\prices`), Predictor will warn and skip.
  - Bridge in paper mode fills against the same historical prices, so the trades
    will use 2022-2026 prices (matches the news date).
  - All events get fresh ULIDs at publish time, so Enricher dedup (by event_id)
    won't collide with anything.

Usage:
    nb-script replay_historical_news.py --count 100
    nb-script replay_historical_news.py --count 1000 --from 2024-01-01
    nb-script replay_historical_news.py --count 50 --dry-run         # sample but don't publish
    nb-script replay_historical_news.py --count 200 --rate-limit-sec 0.5

Read the printed run_id afterwards. To analyze just YOUR replay (not live),
use the timestamp window from start_at -> end_at.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from redis.asyncio import Redis  # noqa: E402

from src.contracts.base import utcnow_iso  # noqa: E402
from src.contracts.raw_news import RawNewsEvent, RawNewsPayload  # noqa: E402
from src.infra.publisher import StreamPublisher  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
# Sprint 6.2 — switched default from duble3\telegram_news.jsonl (frozen at
# 2026-04-30 since pre-Y7) to the up-to-date scrape used by Y7 gap fill,
# which extends through 2026-06-06+. Override via --jsonl if needed.
DEFAULT_JSONL = r"D:\quik_sber\newsbot\telegram_news.jsonl"
TEXT_HARD_LIMIT = 10_000


def _normalize_text(text: str) -> str:
    return unicodedata.normalize("NFC", text.strip())


def _text_sha256(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD as UTC midnight."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _record_within_range(rec: dict, ts_from: float, ts_to: float) -> bool:
    """Return True if record's timestamp falls inside [ts_from, ts_to)."""
    ts = rec.get("timestamp")
    if ts is None:
        return False
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        return False
    return ts_from <= ts < ts_to


def _sample_records(jsonl_path: Path, count: int,
                    ts_from: float, ts_to: float) -> list[dict]:
    """Reservoir sampling — single pass through file, O(N) read, O(count) memory.

    Filters by timestamp range first, then samples uniformly from those.
    """
    print(f"Sampling {count} records from {jsonl_path}...")
    sampled: list[dict] = []
    n_seen = 0
    n_in_range = 0
    rng = random.Random(int(time.time()))

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            n_seen += 1
            if n_seen % 100_000 == 0:
                print(f"  ... read {n_seen:,} lines, kept {len(sampled)} in reservoir, "
                      f"{n_in_range:,} in date range")
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not _record_within_range(rec, ts_from, ts_to):
                continue
            n_in_range += 1
            # Reservoir sampling
            if len(sampled) < count:
                sampled.append(rec)
            else:
                # Probabilistic replace
                idx = rng.randint(0, n_in_range - 1)
                if idx < count:
                    sampled[idx] = rec

    print(f"  done. Total lines: {n_seen:,}, in date range: {n_in_range:,}, sampled: {len(sampled)}")
    return sampled


def _build_event(rec: dict) -> RawNewsEvent | None:
    """Build a RawNewsEvent from a jsonl record. Returns None if invalid."""
    full_text = rec.get("full_text") or ""
    canonical = _normalize_text(full_text)
    if not canonical:
        return None

    ts = rec.get("timestamp")
    if ts is None:
        return None
    try:
        tg_dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None

    channel = rec.get("channel") or "unknown"
    msg_id = rec.get("tg_msg_id")
    if not isinstance(msg_id, int):
        # Some records may have it as string
        try:
            msg_id = int(msg_id)
        except (TypeError, ValueError):
            return None

    text_hash = _text_sha256(canonical)
    text_for_payload = canonical[:TEXT_HARD_LIMIT]

    payload = RawNewsPayload(
        channel=f"@{channel}",
        message_id=msg_id,
        text=text_for_payload,
        tg_published_at=tg_dt.isoformat(timespec="milliseconds"),
        received_at=utcnow_iso(),
        text_hash=text_hash,
        has_media=False,
        is_reply=False,
        is_forward=False,
    )

    return RawNewsEvent(payload=payload)


async def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--count", type=int, default=100,
                   help="How many news to replay (default: 100)")
    p.add_argument("--from", dest="date_from", default="2022-01-01",
                   help="Start date YYYY-MM-DD (default: 2022-01-01)")
    p.add_argument("--to", dest="date_to", default="2026-12-31",
                   help="End date YYYY-MM-DD inclusive (default: 2026-12-31)")
    p.add_argument("--jsonl", default=DEFAULT_JSONL,
                   help=f"Path to telegram_news.jsonl (default: {DEFAULT_JSONL})")
    p.add_argument("--rate-limit-sec", type=float, default=0.0,
                   help="Sleep between publishes to throttle Enricher load. "
                        "0 = publish as fast as possible (default). "
                        "0.5 = 2 events/sec.")
    p.add_argument("--dry-run", action="store_true",
                   help="Sample and validate but don't publish to Redis")
    p.add_argument("--no-confirm", action="store_true",
                   help="Skip confirmation prompt")
    args = p.parse_args()

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.exists():
        print(f"FATAL: jsonl not found: {jsonl_path}", file=sys.stderr)
        return 1

    ts_from = _parse_date(args.date_from).timestamp()
    ts_to = (_parse_date(args.date_to).timestamp() + 86400)  # end of day inclusive

    print(f"Date range: {args.date_from} -> {args.date_to}")
    print(f"Target count: {args.count}")
    print(f"Redis: {REDIS_URL}")
    print(f"Dry-run: {args.dry_run}")
    print()

    # Sample
    records = _sample_records(jsonl_path, args.count, ts_from, ts_to)
    if not records:
        print("FATAL: no records found in date range", file=sys.stderr)
        return 1

    # Build events
    events: list[RawNewsEvent] = []
    skipped = 0
    for rec in records:
        ev = _build_event(rec)
        if ev is None:
            skipped += 1
        else:
            events.append(ev)
    print(f"Built {len(events)} events ({skipped} skipped due to missing fields)")
    print()

    # Date distribution
    by_year: dict[int, int] = {}
    for ev in events:
        try:
            yr = datetime.fromisoformat(ev.payload.tg_published_at.replace("Z", "+00:00")).year
            by_year[yr] = by_year.get(yr, 0) + 1
        except ValueError:
            pass
    print("Distribution by year:")
    for yr in sorted(by_year.keys()):
        print(f"  {yr}: {by_year[yr]}")
    print()

    if args.dry_run:
        print("DRY-RUN: not publishing. First 3 events:")
        for ev in events[:3]:
            print(f"  - {ev.payload.channel} msg_id={ev.payload.message_id} "
                  f"at={ev.payload.tg_published_at} text={ev.payload.text[:80]!r}")
        return 0

    # Confirm large replays
    estimated_tokens = len(events) * 5000
    print(f"Estimated Groq input: ~{estimated_tokens:,} tokens")
    if estimated_tokens > 1_710_000 and not args.no_confirm:
        ans = input("Large replay, live news will queue behind. Continue? (y/n): ")
        if ans.lower() != "y":
            print("Aborted.")
            return 0

    # Publish
    r = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        await r.ping()
    except Exception as e:
        print(f"FATAL: Redis unreachable at {REDIS_URL}: {e}", file=sys.stderr)
        return 1

    pub = StreamPublisher(redis=r, stream="news:raw")
    start_at = datetime.now(timezone.utc)
    run_id = start_at.strftime("%Y%m%dT%H%M%SZ")

    print(f"Publishing to news:raw (run_id={run_id})...")
    n_published = 0
    first_event_id = None
    last_event_id = None
    for i, ev in enumerate(events):
        try:
            await pub.publish(ev)
            n_published += 1
            if first_event_id is None:
                first_event_id = ev.event_id
            last_event_id = ev.event_id
            if (i + 1) % 50 == 0:
                print(f"  ... published {i + 1}/{len(events)}")
            if args.rate_limit_sec > 0:
                await asyncio.sleep(args.rate_limit_sec)
        except Exception as e:
            print(f"  publish FAILED for event {i}: {e}", file=sys.stderr)

    end_at = datetime.now(timezone.utc)
    duration_sec = (end_at - start_at).total_seconds()
    print()
    print(f"=== Replay complete ===")
    print(f"  run_id:        {run_id}")
    print(f"  published:     {n_published}/{len(events)}")
    print(f"  duration:      {duration_sec:.1f}s")
    print(f"  start_at:      {start_at.isoformat(timespec='seconds')}")
    print(f"  end_at:        {end_at.isoformat(timespec='seconds')}")
    print(f"  first event_id: {first_event_id}")
    print(f"  last event_id:  {last_event_id}")
    print()
    print("What to do now:")
    print("  1. Watch enricher process them (will take 10-30 min for 100 events):")
    print("       nb-log-enricher")
    print("  2. After 15+ min, check signal stats:")
    print("       nb-script analyze_signals.py")
    print("       nb-script analyze_pnl.py")
    print(f"  3. Or use the time window {start_at.strftime('%H:%M:%S')} -> now:")
    print("       nb-enriched 50")

    await r.aclose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
