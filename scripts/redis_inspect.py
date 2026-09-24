"""Quick Redis inspection without redis-cli.

Usage:
    python scripts/redis_inspect.py len news:raw
    python scripts/redis_inspect.py groups news:raw
    python scripts/redis_inspect.py pending news:raw enricher          # зависшие сообщения
    python scripts/redis_inspect.py claim news:raw enricher 60000      # забрать pending idle > 60s обратно в очередь
    python scripts/redis_inspect.py enriched 20                        # ⭐ последние N enriched с разбором JSON
    python scripts/redis_inspect.py last news:enriched 5
    python scripts/redis_inspect.py last system:heartbeats 3
    python scripts/redis_inspect.py dlq                # last 10 DLQ entries
    python scripts/redis_inspect.py summary            # обзор всех потоков
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

# Python 3.14 + Windows: дефолтный ProactorEventLoop падает на TCP-коннектах
# через tunneled localhost (наш кейс — Redis на VPS через `ssh -L`).
# SelectorEventLoop работает корректно. На Linux/macOS no-op.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from redis.asyncio import Redis  # noqa: E402

# Sprint 5.11: читаем REDIS_URL из env чтобы можно было указывать на VPS-Redis
# через SSH tunnel (например REDIS_URL=redis://127.0.0.1:6380), не правя скрипт.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


def _fmt_fields(fields) -> str:
    """Format fields dict for printing. Tolerates both str and bytes keys/values."""
    decoded = {}
    for k, v in fields.items():
        ks = k.decode() if isinstance(k, bytes) else str(k)
        vs = v.decode() if isinstance(v, bytes) else str(v)
        if len(vs) > 200:
            vs = vs[:200] + "..."
        decoded[ks] = vs
    return json.dumps(decoded, ensure_ascii=False, indent=2)


async def cmd_len(redis, stream):
    n = await redis.xlen(stream)
    print(f"{stream}: {n}")


async def cmd_groups(redis, stream):
    try:
        groups = await redis.xinfo_groups(stream)
    except Exception as e:
        print(f"error: {e}")
        return
    if not groups:
        print(f"{stream}: no groups")
        return

    def _get(d, key):
        """Get value by key — try both str and bytes (redis-py версии возвращают по-разному)."""
        if key in d:
            return d[key]
        bkey = key.encode() if isinstance(key, str) else key
        skey = key.decode() if isinstance(key, bytes) else key
        return d.get(bkey, d.get(skey, ""))

    def _dec(v):
        return v.decode() if isinstance(v, bytes) else str(v)

    for g in groups:
        name = _dec(_get(g, "name"))
        consumers = _get(g, "consumers")
        pending = _get(g, "pending")
        last_id = _dec(_get(g, "last-delivered-id"))
        print(
            f"  group={name}  consumers={consumers}  "
            f"pending={pending}  last-delivered={last_id}"
        )

    # Also show per-consumer details (useful to see who has pending)
    for g in groups:
        gname = _dec(_get(g, "name"))
        try:
            consumers = await redis.xinfo_consumers(stream, gname)
        except Exception:
            continue
        for c in consumers:
            cname = _dec(_get(c, "name"))
            pending = _get(c, "pending")
            idle = _get(c, "idle")
            print(f"    consumer={cname}  pending={pending}  idle_ms={idle}")


async def cmd_pending(redis, stream, group):
    """XPENDING summary + first 10 pending message details."""
    try:
        summary = await redis.xpending(stream, group)
    except Exception as e:
        print(f"error: {e}")
        return

    if not summary or not summary.get("pending"):
        print(f"{stream}/{group}: no pending messages ✅")
        return

    def _dec(v):
        if v is None:
            return ""
        return v.decode() if isinstance(v, bytes) else str(v)

    total = summary.get("pending", 0)
    min_id = summary.get("min")
    max_id = summary.get("max")
    consumers = summary.get("consumers", []) or []

    print(f"\n=== {stream}/{group} PENDING ===")
    print(f"total={total}  min={_dec(min_id)}  max={_dec(max_id)}")

    if consumers:
        print("per-consumer:")
        # Может быть list[dict], list[tuple], или dict — нормализуем
        if isinstance(consumers, dict):
            iter_ = consumers.items()
        else:
            # Список объектов вида {b"name": ..., b"pending": ...} или (name, count)
            normalized = []
            for c in consumers:
                if isinstance(c, dict):
                    name = c.get("name") or c.get(b"name")
                    cnt = c.get("pending") or c.get(b"pending")
                    normalized.append((name, cnt))
                elif isinstance(c, (tuple, list)) and len(c) >= 2:
                    normalized.append((c[0], c[1]))
            iter_ = normalized
        for cname, cnt in iter_:
            print(f"  {_dec(cname)}: {cnt}")

    # Detail — first 10 pending
    try:
        details = await redis.xpending_range(stream, group, min="-", max="+", count=10)
    except Exception as e:
        print(f"  (xpending_range error: {e})")
        return
    if details:
        print("\nfirst 10 pending entries:")
        for d in details:
            # d может быть dict с str-ключами или с bytes-ключами
            def _get(key):
                if isinstance(d, dict):
                    return d.get(key, d.get(key.encode() if isinstance(key, str) else key))
                return None

            msg_id = _dec(_get("message_id"))
            consumer = _dec(_get("consumer"))
            idle_ms = _get("time_since_delivered") or 0
            delivered = _get("times_delivered") or 0
            print(f"  [{msg_id}] consumer={consumer}  idle_ms={idle_ms}  delivered={delivered}x")


async def cmd_claim(redis, stream, group, min_idle_ms, consumer_name="enricher-1"):
    """XAUTOCLAIM: reassign pending messages idle longer than min_idle_ms to consumer.

    После claim сообщения снова станут видны для xreadgroup нового консьюмера
    (или того же), поскольку их last-delivered-id обновится. Это способ
    вручную разморозить зависшие retryable.
    """
    try:
        # XAUTOCLAIM returns: (next_id, claimed_messages, deleted_ids)
        result = await redis.xautoclaim(
            stream, group, consumer_name,
            min_idle_time=int(min_idle_ms),
            start_id="0",
            count=100,
        )
    except Exception as e:
        print(f"error: {e}")
        return

    next_id, claimed, deleted = result if len(result) >= 3 else (result[0], result[1], [])

    def _dec(v):
        return v.decode() if isinstance(v, bytes) else str(v)

    print(f"\n=== XAUTOCLAIM {stream}/{group} (min_idle={min_idle_ms}ms) ===")
    print(f"next_id={_dec(next_id)}  claimed={len(claimed)}  deleted={len(deleted)}")
    if claimed:
        print(f"claimed messages reassigned to consumer='{consumer_name}':")
        for entry in claimed[:10]:
            # entry is (msg_id, fields) tuple
            msg_id = _dec(entry[0]) if isinstance(entry, (list, tuple)) else _dec(entry)
            print(f"  {msg_id}")
        if len(claimed) > 10:
            print(f"  ... and {len(claimed) - 10} more")
    print(
        f"\nNow restart Enricher and it will receive these messages "
        f"on its next xreadgroup tick."
    )


async def cmd_enriched(redis, n=10):
    """Last N enriched events with parsed payload — like a tail of intelligible decisions."""
    msgs = await redis.xrevrange("news:enriched", count=n)
    print(f"\n=== news:enriched (last {len(msgs)}, newest first) ===")
    for msg_id, fields in msgs:
        try:
            data = fields.get(b"data") or fields.get("data")
            if isinstance(data, bytes):
                data = data.decode()
            obj = json.loads(data)
            payload = obj.get("payload", {})

            tickers = payload.get("tickers", [])
            t_str = ", ".join(
                f"{t.get('ticker')}/{t.get('direction')}({t.get('confidence'):.2f})"
                for t in tickers[:5]
            ) if tickers else "—"

            ts = obj.get("produced_at", "")[:19]
            print(
                f"\n[{ts}] cat={payload.get('category', '?'):<11} "
                f"tf={payload.get('expected_timeframe', '?'):<7} "
                f"urg={payload.get('urgency', '?'):<6} "
                f"act={payload.get('is_actionable', '?')}"
            )
            print(f"  summary : {payload.get('summary', '')[:200]}")
            print(f"  tickers : {t_str}")
            # Show first ticker rationale if any
            if tickers and tickers[0].get("rationale"):
                print(f"  why     : {tickers[0]['rationale'][:200]}")
        except Exception as e:
            print(f"  (parse error: {e})")


async def cmd_last(redis, stream, n=5):
    msgs = await redis.xrevrange(stream, count=n)
    print(f"\n=== {stream} (last {len(msgs)}) ===")
    for msg_id, fields in msgs:
        msg_id_s = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
        print(f"\n[{msg_id_s}]")
        print(_fmt_fields(fields))


async def cmd_dlq(redis, n=10):
    await cmd_last(redis, "news:enriched:dlq", n)


async def cmd_summary(redis):
    streams = [
        "news:raw",
        "news:enriched",
        "news:enriched:dlq",
        "system:heartbeats",
    ]
    print(f"{'stream':<25} {'len':>8} {'groups':>10}")
    print("-" * 45)
    for s in streams:
        try:
            length = await redis.xlen(s)
        except Exception:
            length = "-"
        try:
            groups = await redis.xinfo_groups(s)
            ngroups = len(groups) if groups else 0
        except Exception:
            ngroups = "-"
        print(f"{s:<25} {length:>8} {ngroups:>10}")


async def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return

    redis = Redis.from_url(REDIS_URL, decode_responses=False)
    try:
        cmd = args[0]
        if cmd == "len" and len(args) >= 2:
            await cmd_len(redis, args[1])
        elif cmd == "groups" and len(args) >= 2:
            await cmd_groups(redis, args[1])
        elif cmd == "pending" and len(args) >= 3:
            await cmd_pending(redis, args[1], args[2])
        elif cmd == "claim" and len(args) >= 4:
            # claim <stream> <group> <min_idle_ms> [consumer_name]
            consumer = args[4] if len(args) >= 5 else "enricher-1"
            await cmd_claim(redis, args[1], args[2], args[3], consumer)
        elif cmd == "enriched":
            n = int(args[1]) if len(args) >= 2 else 10
            await cmd_enriched(redis, n)
        elif cmd == "last" and len(args) >= 2:
            n = int(args[2]) if len(args) >= 3 else 5
            await cmd_last(redis, args[1], n)
        elif cmd == "dlq":
            n = int(args[1]) if len(args) >= 2 else 10
            await cmd_dlq(redis, n)
        elif cmd == "summary":
            await cmd_summary(redis)
        else:
            print(f"Unknown command: {cmd}")
            print(__doc__)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
