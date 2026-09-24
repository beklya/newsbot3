"""Breakdown trade:signals stream — EXECUTE vs REJECT, reasons, last EXECUTEs.

Usage:
    nb-script analyze_signals.py
    nb-script analyze_signals.py --hours 24
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from redis.asyncio import Redis  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=int, default=0,
                   help="Only signals from last N hours (default: all)")
    p.add_argument("--show-executes", type=int, default=10,
                   help="How many last EXECUTE signals to show")
    args = p.parse_args()

    r = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        if args.hours > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
            min_id = f"{int(cutoff.timestamp() * 1000)}-0"
            entries = await r.xrange("trade:signals", min=min_id, max="+", count=100_000)
            print(f"Signals from last {args.hours}h:")
        else:
            entries = await r.xrange("trade:signals", min="-", max="+", count=100_000)
            print("Signals (all):")

        actions: dict[str, int] = {}
        reject_reasons: dict[str, int] = {}
        tickers: dict[str, int] = {}
        executes = []

        for msg_id, fields in entries:
            raw = fields.get(b"data") or fields.get("data")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            d = json.loads(raw)
            payload = d.get("payload", {})

            action = payload.get("action", "unknown")
            actions[action] = actions.get(action, 0) + 1

            tk = payload.get("ticker", "?")
            tickers[tk] = tickers.get(tk, 0) + 1

            if action == "REJECT":
                reason = (
                    payload.get("reject_reason")
                    or payload.get("reason")
                    or "unknown"
                )
                # shorten common reasons
                short = reason[:80] + ("..." if len(reason) > 80 else "")
                reject_reasons[short] = reject_reasons.get(short, 0) + 1
            elif action == "EXECUTE":
                executes.append(payload)

        total = len(entries)
        print(f"  total:   {total}")
        for act, n in sorted(actions.items(), key=lambda x: -x[1]):
            pct = 100.0 * n / total if total else 0.0
            print(f"  {act:<8} {n:5d}  ({pct:.1f}%)")
        print()

        if reject_reasons:
            print("REJECT reasons (top 10):")
            for reason, n in sorted(reject_reasons.items(), key=lambda x: -x[1])[:10]:
                print(f"  {n:5d}  {reason}")
            print()

        print("Tickers (top 10):")
        for tk, n in sorted(tickers.items(), key=lambda x: -x[1])[:10]:
            print(f"  {tk:<10} {n:5d}")
        print()

        if executes:
            n_show = min(args.show_executes, len(executes))
            print(f"Last {n_show} EXECUTE signals (newest first):")
            for ex in executes[-n_show:][::-1]:
                ticker = ex.get("ticker", "?")
                side = ex.get("side", "?")
                entry = ex.get("entry_price_hint")
                sl = ex.get("sl_price")
                tp = ex.get("tp_price")
                qty = ex.get("quantity_lots", ex.get("quantity"))
                rr = ex.get("rr")
                conf = ex.get("confidence")
                ts = ex.get("produced_at", ex.get("decision_at", ""))
                print(
                    f"  {ts[:19]}  {ticker:<6} {side:<4}  "
                    f"entry={entry}  sl={sl}  tp={tp}  qty={qty}  rr={rr}  conf={conf}"
                )

        return 0
    finally:
        await r.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
