"""Live tail of news:enriched stream — see Groq's verdicts as they happen.

Connects to Redis, follows `news:enriched` with XREAD blocking, and for each
new event prints:
- timestamp
- original news headline (looked up from news:raw via raw_event_id from trace)
- LLM verdict: category, tickers, summary

Usage:
    python scripts/watch_enriched.py
    python scripts/watch_enriched.py --no-raw          # don't look up original text
    python scripts/watch_enriched.py --from-start      # start from beginning of stream

Ctrl+C to stop.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from pathlib import Path

# Python 3.14 + Windows: ProactorEventLoop ломается на tunneled TCP.
# См. scripts/redis_inspect.py для деталей.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from redis.asyncio import Redis  # noqa: E402

# Sprint 5.11: REDIS_URL из env для работы через SSH tunnel к VPS-Redis.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
ENRICHED_STREAM = "news:enriched"
RAW_STREAM = "news:raw"


def _d(v):
    return v.decode() if isinstance(v, bytes) else v


# Color codes for terminals that support them (Windows 10+ should be fine)
class C:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    BLUE = "\033[94m"


def _color_direction(direction: str) -> str:
    if direction == "long":
        return f"{C.GREEN}long{C.RESET}"
    if direction == "short":
        return f"{C.RED}short{C.RESET}"
    return f"{C.DIM}neutral{C.RESET}"


def _color_category(category: str) -> str:
    color_map = {
        "cbr": C.CYAN,
        "geopolitics": C.MAGENTA,
        "corporate": C.GREEN,
        "macro": C.BLUE,
        "commodity": C.YELLOW,
        "currency": C.CYAN,
        "market": C.BLUE,
        "other": C.DIM,
    }
    c = color_map.get(category, "")
    return f"{c}{category}{C.RESET}"


def _color_actionable(act: bool) -> str:
    return f"{C.GREEN}A{C.RESET}" if act else f"{C.DIM}—{C.RESET}"


async def lookup_raw_text(redis: Redis, raw_event_id: str) -> str:
    """Find the original news text by raw_event_id in news:raw.

    XREVRANGE is O(N) scan unfortunately — but we only do this for live events,
    and news:raw is usually small. If it gets slow, cache locally.
    """
    if not raw_event_id:
        return "(no raw_event_id)"
    # Search recent first (newest events likely just published)
    try:
        msgs = await redis.xrevrange(RAW_STREAM, count=200)
    except Exception as e:
        return f"(lookup error: {e})"

    for _msg_id, fields in msgs:
        eid = fields.get(b"event_id")
        if eid and _d(eid) == raw_event_id:
            data = fields.get(b"data")
            if data is None:
                continue
            try:
                obj = json.loads(_d(data))
                text = obj.get("payload", {}).get("text", "")
                return text.replace("\n", " ").strip()
            except Exception:
                continue
    return "(raw not found in last 200)"


def print_event(payload: dict, raw_text: str | None) -> None:
    """Pretty-print one enriched event."""
    cat = payload.get("category", "?")
    tf = payload.get("expected_timeframe", "?")
    urg = payload.get("urgency", "?")
    act = payload.get("is_actionable", False)
    summary = payload.get("summary", "")
    tickers = payload.get("tickers", [])
    is_fin = payload.get("is_financial", False)

    # Header line
    header = f"{C.BOLD}{_color_category(cat):<20}{C.RESET} tf={tf:<7} urg={urg:<6} {_color_actionable(act)}"
    if not is_fin:
        header += f" {C.DIM}(not financial){C.RESET}"
    print(header)

    if raw_text is not None:
        print(f"  {C.DIM}original:{C.RESET} {raw_text[:200]}")
    print(f"  {C.BOLD}summary:{C.RESET}  {summary[:200]}")

    if tickers:
        for t in tickers[:8]:
            print(
                f"    {t.get('ticker', '?'):<7} "
                f"{_color_direction(t.get('direction', '?')):<25} "  # ansi codes shift width
                f"conf={t.get('confidence', 0):.2f} "
                f"impact={t.get('impact_strength', 0):.2f} "
                f"sent={t.get('sentiment', '?'):<8}"
            )
            rat = t.get("rationale", "")
            if rat:
                print(f"      {C.DIM}{rat[:150]}{C.RESET}")
    else:
        print(f"  {C.DIM}tickers:  []{C.RESET}")


async def run(start_from: str, lookup_raw: bool) -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=False)
    print(f"watching {ENRICHED_STREAM} from {start_from}... (Ctrl+C to stop)\n")
    last_id = start_from

    stop = asyncio.Event()

    def _sig_handler(*_):
        stop.set()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, _sig_handler)
    else:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, _sig_handler)

    try:
        while not stop.is_set():
            try:
                # Blocking XREAD — wakes up on new message OR on timeout
                msgs = await redis.xread(
                    streams={ENRICHED_STREAM: last_id},
                    block=2_000,
                    count=10,
                )
            except Exception as e:
                print(f"{C.RED}XREAD error: {e}{C.RESET}")
                await asyncio.sleep(1)
                continue

            if not msgs:
                continue

            for _stream_name, entries in msgs:
                for msg_id, fields in entries:
                    last_id = _d(msg_id)
                    try:
                        data = fields.get(b"data") or fields.get("data")
                        if data is None:
                            continue
                        obj = json.loads(_d(data))
                        payload = obj.get("payload", {})
                        raw_id = payload.get("raw_event_id", "")
                        ts = obj.get("produced_at", "")[:19]

                        raw_text = None
                        if lookup_raw:
                            raw_text = await lookup_raw_text(redis, raw_id)

                        # Separator + timestamp
                        print(f"\n{C.DIM}--- {ts}  ev={obj.get('event_id', '?')} ---{C.RESET}")
                        print_event(payload, raw_text)
                    except Exception as e:
                        print(f"{C.RED}parse error: {e}{C.RESET}")
    finally:
        await redis.aclose()
        print(f"\n{C.DIM}watch_enriched stopped.{C.RESET}")


async def main():
    parser = argparse.ArgumentParser(description="Live tail of news:enriched")
    parser.add_argument(
        "--no-raw", action="store_true",
        help="Don't look up original text from news:raw (faster, less info)",
    )
    parser.add_argument(
        "--from-start", action="store_true",
        help="Start from beginning of stream (default: only new events)",
    )
    args = parser.parse_args()

    start_from = "0" if args.from_start else "$"
    await run(start_from, lookup_raw=not args.no_raw)


if __name__ == "__main__":
    asyncio.run(main())
