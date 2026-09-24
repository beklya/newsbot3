"""Replay authentic RawNewsEvent envelopes (e.g. from `pull_raw_from_vps.py`)
through the live pipeline.

Sister script of `replay_historical_news.py` — the difference:
- replay_historical_news.py reads a CSV/JSONL with Telegram message fields
  (`timestamp`, `full_text`, `channel`, `tg_msg_id`) and BUILDS new RawNewsEvent
  payloads from scratch.
- This script reads a JSONL of ALREADY-BUILT RawNewsEvent envelopes
  (the same shape Receiver wrote them in originally) and republishes them
  verbatim — same channel, same text, same text_hash, same tg_published_at.

Why this matters: Sprint 6.2 demonstrated that replay_historical_news.py
pulls from a different JSONL than what VPS Receiver actually captured live
during 2026-06-01..05 — the two sets overlap only ~73%, with the missing
606 baseline-only events being 66% financial (the high-value triggers).
Using authentic envelopes bypasses any input-set divergence.

Each line of the input must be either:
  {"stream_id": "...", "envelope": {full RawNewsEvent JSON}, "news_time_utc": "..."}
  (the format produced by pull_enriched_from_vps.py / pull_raw_from_vps.py)
OR
  {full RawNewsEvent JSON}  (a bare envelope per line)

Fresh ULIDs are minted per event so that:
  - news:raw treats them as new XADD entries (no event_id collision)
  - downstream idempotency keys (scope=news_enriched, scope=trade_signal)
    do not collide with prior runs (but text_hash dedup in Receiver
    IdempotencyGuard is bypassed because we publish DIRECTLY to news:raw,
    not through Receiver)

Usage:
    nb-script replay_raw_envelopes.py --input data/replay/raw_vps_window_authentic.jsonl
    nb-script replay_raw_envelopes.py --input ... --rate-limit-sec 1.0
    nb-script replay_raw_envelopes.py --input ... --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from redis.asyncio import Redis  # noqa: E402

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload  # noqa: E402
from src.infra.publisher import StreamPublisher  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


def _extract_envelope(rec: dict) -> dict | None:
    """Accept either {envelope: {...}} or a bare envelope dict."""
    if "envelope" in rec and isinstance(rec["envelope"], dict):
        return rec["envelope"]
    if "payload" in rec and isinstance(rec["payload"], dict):
        return rec
    return None


def _build_event(env_dict: dict) -> RawNewsEvent | None:
    """Build a fresh RawNewsEvent from a saved envelope.

    The payload is re-used verbatim (channel, message_id, text, text_hash,
    tg_published_at, received_at, has_media, is_reply, is_forward).
    The envelope-level event_id is REGENERATED so each replay gets a fresh
    ULID — this is intentional, so the pipeline treats them as new XADDs.
    """
    payload_dict = env_dict.get("payload")
    if not isinstance(payload_dict, dict):
        return None
    try:
        payload = RawNewsPayload(**payload_dict)
    except Exception as e:
        return None
    # Construct fresh envelope (new event_id ULID, fresh produced_at)
    try:
        return RawNewsEvent(payload=payload)
    except Exception:
        return None


async def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", type=Path, required=True,
                   help="JSONL of RawNewsEvent envelopes")
    p.add_argument("--rate-limit-sec", type=float, default=0.0,
                   help="Sleep between publishes (default: as fast as possible)")
    p.add_argument("--dry-run", action="store_true",
                   help="Parse + validate but do not XADD")
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after N events (0 = all)")
    p.add_argument("--stream", default="news:raw")
    args = p.parse_args()

    if not args.input.exists():
        print(f"FATAL: input not found: {args.input}", file=sys.stderr)
        return 1

    # Pass 1: parse + validate
    events: list[RawNewsEvent] = []
    n_lines = 0
    n_failed = 0
    with args.input.open(encoding="utf-8") as f:
        for line in f:
            n_lines += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_failed += 1
                continue
            env = _extract_envelope(rec)
            if env is None:
                n_failed += 1
                continue
            ev = _build_event(env)
            if ev is None:
                n_failed += 1
                continue
            events.append(ev)
            if args.limit and len(events) >= args.limit:
                break

    print(f"Input lines: {n_lines}")
    print(f"Failed to parse: {n_failed}")
    print(f"Events to publish: {len(events)}")
    if not events:
        print("Nothing to do.")
        return 0

    # Date distribution
    by_date: dict[str, int] = {}
    for ev in events:
        d = ev.payload.tg_published_at[:10]
        by_date[d] = by_date.get(d, 0) + 1
    print("By tg_published_at date:")
    for d in sorted(by_date):
        print(f"  {d}: {by_date[d]}")
    print()

    if args.dry_run:
        print("DRY-RUN. First 3 events:")
        for ev in events[:3]:
            print(f"  channel={ev.payload.channel} msg_id={ev.payload.message_id} "
                  f"at={ev.payload.tg_published_at} text={ev.payload.text[:80]!r}")
        return 0

    # Publish
    r = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        await r.ping()
    except Exception as e:
        print(f"FATAL: Redis unreachable at {REDIS_URL}: {e}", file=sys.stderr)
        return 1

    pub = StreamPublisher(redis=r, stream=args.stream)
    start_at = datetime.now(timezone.utc)
    run_id = start_at.strftime("%Y%m%dT%H%M%SZ")
    print(f"Publishing to {args.stream} (run_id={run_id})...")
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
            if (i + 1) % 100 == 0:
                print(f"  ... published {i + 1}/{len(events)}")
            if args.rate_limit_sec > 0:
                await asyncio.sleep(args.rate_limit_sec)
        except Exception as e:
            print(f"  publish FAILED for event {i}: {e}", file=sys.stderr)

    end_at = datetime.now(timezone.utc)
    duration_sec = (end_at - start_at).total_seconds()
    print()
    print(f"=== Replay complete ===")
    print(f"  run_id:          {run_id}")
    print(f"  published:       {n_published}/{len(events)}")
    print(f"  duration:        {duration_sec:.1f}s")
    print(f"  start_at:        {start_at.isoformat(timespec='seconds')}")
    print(f"  end_at:          {end_at.isoformat(timespec='seconds')}")
    print(f"  first event_id:  {first_event_id}")
    print(f"  last event_id:   {last_event_id}")
    await r.aclose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
