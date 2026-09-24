"""Paper trading P&L summary from trade:executions stream.

Schema v1.0.1: OPEN/CLOSE pair share signal_event_id.
  - OPEN  = exit_reason is None
  - CLOSE = exit_reason is set (tp / sl / time / kill)

Ticker is not in ExecutionResultEvent payload — we look it up by
signal_event_id in trade:signals stream.

Usage:
    nb-script analyze_pnl.py
    nb-script analyze_pnl.py --hours 24
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from redis.asyncio import Redis  # noqa: E402

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


async def _load_signals_lookup(r: Redis) -> dict[str, dict]:
    """Map signal event_id -> {ticker, side}. Reads trade:signals once."""
    entries = await r.xrange("trade:signals", min="-", max="+", count=100_000)
    out: dict[str, dict] = {}
    for _msg_id, fields in entries:
        raw = fields.get(b"data") or fields.get("data")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        d = json.loads(raw)
        eid = d.get("event_id")
        if not eid:
            continue
        payload = d.get("payload", {})
        out[eid] = {
            "ticker": payload.get("ticker", "?"),
            "side": payload.get("side", "?"),
        }
    return out


async def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=int, default=0)
    args = p.parse_args()

    r = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        signals_meta = await _load_signals_lookup(r)
        print(f"Loaded {len(signals_meta)} signals for ticker lookup")
        print()

        if args.hours > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)
            min_id = f"{int(cutoff.timestamp() * 1000)}-0"
            entries = await r.xrange("trade:executions", min=min_id, max="+", count=100_000)
        else:
            entries = await r.xrange("trade:executions", min="-", max="+", count=100_000)

        # Group by signal_event_id, classify OPEN vs CLOSE
        by_sig: dict[str, dict] = defaultdict(lambda: {"open": None, "close": None})
        for _msg_id, fields in entries:
            raw = fields.get(b"data") or fields.get("data")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            d = json.loads(raw)
            payload = d.get("payload", {})
            sig_id = payload.get("signal_event_id", "?")
            kind = "close" if payload.get("exit_reason") else "open"
            by_sig[sig_id][kind] = {
                "payload": payload,
                "produced_at": d.get("produced_at", ""),
            }

        open_positions = []
        closed_trades = []
        for sig_id, pair in by_sig.items():
            meta = signals_meta.get(sig_id, {"ticker": "?", "side": "?"})
            o = pair["open"]
            c = pair["close"]
            if not o:
                continue
            op = o["payload"]
            entry_price = op.get("filled_price")
            qty = op.get("filled_quantity")
            fill_time = op.get("fill_time", "")

            if c:
                cp = c["payload"]
                exit_price = cp.get("exit_price")
                exit_reason = cp.get("exit_reason")
                pnl_rub = cp.get("realized_pnl_rub")
                dur = cp.get("duration_sec")
                exit_time = cp.get("exit_time", "")
                closed_trades.append({
                    "sig_id": sig_id, "ticker": meta["ticker"], "side": meta["side"],
                    "entry": entry_price, "exit": exit_price,
                    "qty": qty, "pnl": pnl_rub,
                    "reason": exit_reason, "duration_sec": dur,
                    "fill_time": fill_time, "exit_time": exit_time,
                })
            else:
                open_positions.append({
                    "sig_id": sig_id, "ticker": meta["ticker"], "side": meta["side"],
                    "entry": entry_price, "qty": qty, "fill_time": fill_time,
                })

        print(f"=== Paper trading P&L (executions={len(entries)}, trades={len(by_sig)}) ===")
        print()

        if open_positions:
            print(f"=== OPEN positions ({len(open_positions)}) ===")
            for p in open_positions:
                print(f"  {p['ticker']:<6} {p['side']:<4}  qty={p['qty']:<6}  entry={p['entry']:<10.4f}  opened={p['fill_time'][:19]}")
            print()

        if closed_trades:
            # Sort by exit_time descending
            closed_trades.sort(key=lambda t: t.get("exit_time") or "", reverse=True)
            print(f"=== CLOSED trades ({len(closed_trades)}) ===")
            print(f"  {'ticker':<6} {'side':<4} {'qty':<6} {'entry':<10} {'exit':<10} {'pnl_rub':<12} {'reason':<6} {'dur(s)':<7} closed_at")
            print("  " + "-" * 100)
            total_pnl = 0.0
            wins = 0
            losses = 0
            by_reason: dict[str, int] = defaultdict(int)
            for t in closed_trades:
                pnl = t.get("pnl") or 0.0
                total_pnl += pnl
                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1
                by_reason[t.get("reason") or "?"] += 1
                entry = t.get("entry") or 0.0
                ex = t.get("exit") or 0.0
                dur = t.get("duration_sec") or 0
                print(
                    f"  {t['ticker']:<6} {t['side']:<4} {str(t['qty']):<6} "
                    f"{entry:<10.4f} {ex:<10.4f} {pnl:>+10.2f}   "
                    f"{t.get('reason','?'):<6} {dur:<7} {t.get('exit_time','')[:19]}"
                )
            print("  " + "-" * 100)
            print()
            n = len(closed_trades)
            win_rate = 100.0 * wins / n if n else 0.0
            avg_pnl = total_pnl / n if n else 0.0
            print(f"  Total PnL:    {total_pnl:+.2f} RUB ({n} trades)")
            print(f"  Avg PnL/trade {avg_pnl:+.2f} RUB")
            print(f"  Win rate:     {wins}/{n} = {win_rate:.1f}%  (losses: {losses})")
            print(f"  Exit reasons: {dict(by_reason)}")

        if not open_positions and not closed_trades:
            print("No trades found in window")

        return 0
    finally:
        await r.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
