"""Configure Memurai/Redis persistence.

Default Memurai config has RDB snapshots only — they happen every N minutes
when there are changes. For our soak we want AOF (append-only file) which
fsyncs every second — guarantees we don't lose more than ~1 sec of data
on a Memurai/Windows restart.

Usage:
    python scripts/enable_aof.py check    # current config
    python scripts/enable_aof.py enable   # turn on AOF + persist config
    python scripts/enable_aof.py off      # turn off AOF (revert to default)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from redis.asyncio import Redis  # noqa: E402

REDIS_URL = "redis://localhost:6379"


def _d(v):
    """Decode bytes or pass str."""
    return v.decode() if isinstance(v, bytes) else v


def _print_dict(d, prefix=""):
    """Print a config dict that may have bytes or str keys/values."""
    for k, v in d.items():
        print(f"  {prefix}{_d(k)} = {_d(v)!r}")


async def cmd_check():
    r = Redis.from_url(REDIS_URL)
    try:
        # Persistence-related settings
        for key in ("save", "appendonly", "appendfsync", "dir", "dbfilename", "appendfilename"):
            try:
                conf = await r.config_get(key)
                _print_dict(conf)
            except Exception as e:
                print(f"  {key} = (error: {e})")

        # Server info — last save, AOF state
        info = await r.info("persistence")
        print("\nINFO persistence:")
        keys_of_interest = [
            "rdb_changes_since_last_save",
            "rdb_last_save_time",
            "rdb_last_bgsave_status",
            "aof_enabled",
            "aof_last_rewrite_time_sec",
            "aof_current_size",
            "aof_last_write_status",
        ]
        for k in keys_of_interest:
            if k in info:
                print(f"  {k} = {info[k]!r}")

    finally:
        await r.aclose()


async def cmd_enable():
    r = Redis.from_url(REDIS_URL)
    try:
        print("Enabling AOF...")

        # Turn on AOF — will trigger background rewrite of current dataset
        await r.config_set("appendonly", "yes")
        print("  appendonly = yes ✓")

        # fsync once per second — balances perf and durability
        await r.config_set("appendfsync", "everysec")
        print("  appendfsync = everysec ✓")

        # Try to persist the config to disk so it survives restarts.
        # Memurai might or might not support this depending on whether the
        # config file is writable.
        try:
            await r.config_rewrite()
            print("  config_rewrite() OK — settings persisted to memurai.conf ✓")
        except Exception as e:
            print(f"  config_rewrite() failed: {e}")
            print(
                "  → Edit memurai.conf manually to add these lines:\n"
                "       appendonly yes\n"
                "       appendfsync everysec\n"
                "    Otherwise these settings will reset on Memurai restart."
            )

        # Wait briefly for AOF rewrite to start
        await asyncio.sleep(0.5)

        # Verify
        info = await r.info("persistence")
        aof = info.get("aof_enabled", "?")
        print(f"\nVerification: aof_enabled={aof}")
        if aof == 1:
            print("✅ AOF is now active. Data is being written to .aof file every second.")
        else:
            print("⚠️  AOF reported as not enabled — check Memurai logs.")

    finally:
        await r.aclose()


async def cmd_off():
    r = Redis.from_url(REDIS_URL)
    try:
        print("Disabling AOF (reverting to RDB-only)...")
        await r.config_set("appendonly", "no")
        try:
            await r.config_rewrite()
        except Exception as e:
            print(f"  config_rewrite() failed: {e}")
        print("✅ AOF disabled. Memurai now uses only periodic RDB snapshots.")
    finally:
        await r.aclose()


async def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("check", "enable", "off"):
        print(__doc__)
        return
    cmd = sys.argv[1]
    if cmd == "check":
        await cmd_check()
    elif cmd == "enable":
        await cmd_enable()
    elif cmd == "off":
        await cmd_off()


if __name__ == "__main__":
    asyncio.run(main())
