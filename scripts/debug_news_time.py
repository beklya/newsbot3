"""Diagnostic: check news_time propagation through signals + executions."""
import asyncio
import json
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from redis.asyncio import Redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


async def main():
    r = Redis.from_url(REDIS_URL, decode_responses=False)

    print("=== Last 5 EXECUTE signals (newest first) ===")
    entries = await r.xrevrange("trade:signals", "+", "-", count=200)
    n_exec = 0
    for msg_id, fields in entries:
        if n_exec >= 5:
            break
        raw = fields.get(b"data") or fields.get("data")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        d = json.loads(raw)
        p = d["payload"]
        if p["action"] != "EXECUTE":
            continue
        print(f"  event_id={d['event_id']}")
        print(f"    produced_at = {d['produced_at'][:19]}")
        print(f"    news_time   = {p.get('news_time')}")
        print(f"    ticker={p['ticker']} side={p['side']} entry={p['entry_price']}")
        n_exec += 1

    print()
    print("=== Last 3 executions ===")
    exec_entries = await r.xrevrange("trade:executions", "+", "-", count=6)
    for msg_id, fields in exec_entries[:3]:
        raw = fields.get(b"data") or fields.get("data")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        d = json.loads(raw)
        p = d["payload"]
        print(f"  event_id={d['event_id']} produced_at={d['produced_at'][:19]}")
        print(f"    signal_event_id={p.get('signal_event_id')}")
        print(f"    fill_time={p.get('fill_time')}")
        print(f"    exit_reason={p.get('exit_reason')} pnl_rub={p.get('realized_pnl_rub')}")

    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
