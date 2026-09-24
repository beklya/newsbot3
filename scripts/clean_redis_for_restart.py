"""Reset transient Redis state before a fresh paper-soak restart.

Three modes (cumulative):

[default] Soft restart:
  1. system:heartbeats stream -- XTRIM to 0 entries.
  2. Consumer groups -- xgroup_setid <stream> <group> $.

[--full] Also clears paper-trade state:
  3. bridge:open_positions:* and risk:* keys.

[--wipe] NUCLEAR — delete EVERYTHING (Sprint 6, for clean baseline):
  4. DEL all event streams (news:raw, news:enriched, news:enriched:dlq,
     ml:predictions, ml:predictions:dlq, trade:signals, trade:executions).
  5. DEL all idem:* keys (idempotency cache).
  6. DEL all enriched:* keys (Decision cache).
  7. DEL all bridge:* and risk:* keys.
  8. DEL candles:1m (live feed will repopulate).
  USE BEFORE first day of real paper PnL collection so stats start clean.

What it NEVER touches:
  - .env, models on disk, source CSV files, sessions.

Usage:
  python scripts/clean_redis_for_restart.py            # soft: heartbeats + PEL
  python scripts/clean_redis_for_restart.py --full     # also clear paper risk state
  python scripts/clean_redis_for_restart.py --wipe     # FULL NUKE — delete streams
  python scripts/clean_redis_for_restart.py --dry-run  # show what would happen
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root on sys.path so we can re-use settings if needed
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import redis

# Tuples of (stream, consumer_group) that exist in production paper soak.
_CONSUMER_GROUPS = [
    ("news:raw", "enricher"),
    ("news:enriched", "predictor"),
    ("ml:predictions", "decision"),
    ("trade:signals", "bridge"),
]

_HEARTBEAT_STREAM = "system:heartbeats"

# For --full mode only
_RISK_KEY_PATTERNS = [
    "bridge:open_positions:*",
    "risk:open_positions",
    "risk:cooldown:*",
    "risk:daily_pnl:*",
]


def parse_args() -> argparse.Namespace:
    import os
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    # Sprint 5.11: read REDIS_URL from env (for SSH tunnel default)
    p.add_argument("--url",
                   default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                   help="Redis URL (default: $REDIS_URL or localhost:6379)")
    p.add_argument("--full", action="store_true",
                   help="Also clear paper trade risk state (bridge:open_positions, risk:*)")
    p.add_argument("--wipe", action="store_true",
                   help="NUCLEAR: DEL ALL streams + idem + cache (for fresh baseline)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen without executing")
    p.add_argument("--yes", action="store_true",
                   help="Skip confirmation for --wipe")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    r = redis.Redis.from_url(args.url, decode_responses=False)

    try:
        pong = r.ping()
    except Exception as e:
        print(f"FATAL: redis not reachable at {args.url}: {e}", file=sys.stderr)
        return 1
    print(f"Connected to Redis at {args.url} (ping={pong})")

    # === Step 1: Trim heartbeats ===
    try:
        hb_count = r.xlen(_HEARTBEAT_STREAM)
    except Exception:
        hb_count = 0
    if hb_count > 0:
        if args.dry_run:
            print(f"[dry-run] would XTRIM {_HEARTBEAT_STREAM} (n={hb_count}) -> 0")
        else:
            r.xtrim(_HEARTBEAT_STREAM, maxlen=0, approximate=False)
            print(f"  XTRIM {_HEARTBEAT_STREAM}: {hb_count} -> 0")
    else:
        print(f"  {_HEARTBEAT_STREAM}: empty, nothing to trim")

    # === Step 2: Reset consumer groups (drop PEL) ===
    for stream, group in _CONSUMER_GROUPS:
        if not r.exists(stream):
            print(f"  {stream}/{group}: stream missing, skip")
            continue
        try:
            groups_info = r.xinfo_groups(stream)
        except Exception as e:
            print(f"  {stream}/{group}: xinfo_groups error {e}")
            continue
        # groups_info: list of dicts
        names = []
        for g in groups_info:
            # bytes or str key
            nm = g.get(b"name") if b"name" in g else g.get("name")
            if isinstance(nm, bytes):
                nm = nm.decode()
            names.append(nm)
        if group not in names:
            print(f"  {stream}/{group}: group not created yet, skip")
            continue
        # Get current pending count
        pending = 0
        for g in groups_info:
            nm = g.get(b"name") if b"name" in g else g.get("name")
            if isinstance(nm, bytes):
                nm = nm.decode()
            if nm == group:
                pending = g.get(b"pending", 0) or g.get("pending", 0)
                break
        if args.dry_run:
            print(f"[dry-run] would xgroup_setid {stream}/{group} -> $ (drops {pending} pending)")
        else:
            r.xgroup_setid(stream, group, id="$")
            print(f"  xgroup_setid {stream}/{group} -> $ (dropped {pending} pending)")

    # === Step 3 (--full): Clear paper risk state ===
    if args.full or args.wipe:
        print()
        print("--full: clearing paper-trade risk state")
        total = 0
        for pattern in _RISK_KEY_PATTERNS:
            keys = list(r.scan_iter(match=pattern, count=500))
            if not keys:
                print(f"  {pattern}: 0 keys")
                continue
            if args.dry_run:
                print(f"[dry-run] would DEL {len(keys)} keys matching {pattern}")
            else:
                r.delete(*keys)
                print(f"  DEL {pattern}: {len(keys)} keys")
                total += len(keys)
        if not args.dry_run:
            print(f"  Total risk-state keys deleted: {total}")

    # === Step 4 (--wipe): NUCLEAR — delete all event streams + caches ===
    if args.wipe:
        print()
        print("=" * 60)
        print("--wipe: NUCLEAR cleanup — deleting all event streams + caches")
        print("=" * 60)
        if not args.yes and not args.dry_run:
            ans = input("This will delete ALL events history. Type 'WIPE' to confirm: ")
            if ans != "WIPE":
                print("Aborted.")
                return 0

        streams_to_del = [
            "news:raw",
            "news:enriched",
            "news:enriched:dlq",
            "ml:predictions",
            "ml:predictions:dlq",
            "trade:signals",
            "trade:executions",
            "candles:1m",
            "system:heartbeats",
        ]
        for s in streams_to_del:
            try:
                n = r.xlen(s)
            except Exception:
                n = 0
            if n == 0 and not r.exists(s):
                print(f"  {s}: empty, skip")
                continue
            if args.dry_run:
                print(f"[dry-run] would DEL stream {s} (len={n})")
            else:
                r.delete(s)
                print(f"  DEL {s} (was {n} entries)")

        # Pattern-based cleanup
        patterns = [
            ("idem:*", "idempotency cache"),
            ("enriched:*", "Decision enrichment cache (Enricher SETEX side effect)"),
        ]
        for pattern, label in patterns:
            keys = list(r.scan_iter(match=pattern, count=1000))
            if not keys:
                print(f"  {pattern} ({label}): 0 keys")
                continue
            if args.dry_run:
                print(f"[dry-run] would DEL {len(keys)} keys matching {pattern} ({label})")
            else:
                # Delete in batches of 500 to avoid command-line limits
                BATCH = 500
                for i in range(0, len(keys), BATCH):
                    r.delete(*keys[i:i + BATCH])
                print(f"  DEL {pattern}: {len(keys)} keys ({label})")

    print()
    print("Done.")
    if not args.dry_run and args.wipe:
        print("=" * 60)
        print("Redis is now CLEAN.")
        print()
        print("CRITICAL: --wipe deleted all consumer groups. ANY service that")
        print("was running before this script will keep retrying with stale")
        print("group state -> NOGROUP loop forever. You MUST RESTART all")
        print("services with consumer groups (NOT start — restart):")
        print()
        print("  1. VPS (Enricher reads news:raw, Receiver only publishes):")
        print("       ssh USER@VPS_HOST 'sudo systemctl restart newsbot-receiver newsbot-enricher'")
        print()
        print("  2. Local (Predictor, Decision, Bridge all have consumer groups):")
        print("       nb-stop ; nb-launch")
        print()
        print("Monitor doesn't have a consumer group — restart not strictly required.")
        print("=" * 60)
    elif not args.dry_run:
        print("Restart now: .\\scripts\\launch_paper_soak.bat")
    return 0


if __name__ == "__main__":
    sys.exit(main())
