"""Sprint 6.1 Y7 — scrape news gap 2026-04-30 → today using existing newsbot3
receiver session (which IS authorized) instead of the dead duble2/scraper session.

Appends to D:\\quik_sber\\newsbot\\telegram_news.jsonl (already seeded from
duble3 snapshot of 860k records).  Deduplicates by tg_msg_id.

Channels: interfaxonline, rian_ru, tass_agency, rbc_news (same 4 Phase 2 channels).

Usage:
    python scripts/scrape_gap_via_receiver_session.py --from 2026-04-29
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

SESSION_PATH = Path(r"D:\quik_sber\newsbot\newsbot3\data\sessions\receiver")
OUTPUT_FILE = Path(r"D:\quik_sber\newsbot\telegram_news.jsonl")

CHANNELS = ["interfaxonline", "rian_ru", "tass_agency", "rbc_news"]

log = logging.getLogger("scrape_gap")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


def parse_message(msg, channel_username: str) -> dict | None:
    text = msg.message or ""
    if not text.strip():
        return None
    lines = text.strip().split("\n")
    headline = lines[0].strip()
    if len(headline) < 10:
        return None
    if headline.startswith(("@", "http", "//", "===", "---")):
        return None
    dt = msg.date  # UTC tz-aware
    dt_local = dt.astimezone().replace(tzinfo=None)
    raw_id = f"{channel_username}_{msg.id}_{headline[:50]}"
    news_id = hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:12]
    return {
        "id": news_id,
        "tg_msg_id": msg.id,
        "source": f"telegram/@{channel_username}",
        "channel": channel_username,
        "datetime": dt_local.isoformat(),
        "timestamp": dt_local.timestamp(),
        "date": dt_local.strftime("%d.%m.%Y"),
        "time": dt_local.strftime("%H:%M:%S"),
        "headline": headline,
        "full_text": text[:20000],
        "analysis": None,
        "collected_at": datetime.now().isoformat(),
    }


def load_existing_ids() -> set[tuple[str, int]]:
    """Return set of (channel, tg_msg_id) pairs to dedupe against."""
    seen: set[tuple[str, int]] = set()
    if not OUTPUT_FILE.exists():
        return seen
    log.info("loading existing IDs from %s (%.1f MB)...",
             OUTPUT_FILE, OUTPUT_FILE.stat().st_size / 1024**2)
    n = 0
    with OUTPUT_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            ch = r.get("channel") or ""
            mid = r.get("tg_msg_id")
            if isinstance(mid, int):
                seen.add((ch, mid))
                n += 1
    log.info("  loaded %d (channel, msg_id) pairs", n)
    return seen


async def scrape_channel(client, channel: str, date_from: datetime,
                         existing: set) -> int:
    from telethon.errors import FloodWaitError
    log.info("=" * 50)
    log.info("channel: @%s", channel)
    try:
        entity = await client.get_entity(channel)
    except Exception as e:
        log.error("get_entity failed for @%s: %s", channel, e)
        return 0
    log.info("  title: %s", getattr(entity, "title", channel))
    count = 0
    skipped = 0
    seen_msgs = 0
    try:
        async for msg in client.iter_messages(entity, limit=None,
                                               offset_date=None, reverse=False):
            seen_msgs += 1
            if msg.date is None:
                continue
            dt_local = msg.date.astimezone().replace(tzinfo=None)
            if dt_local < date_from:
                log.info("  reached %s (older than %s) — stop",
                         dt_local.date(), date_from.date())
                break
            if (channel, msg.id) in existing:
                skipped += 1
                continue
            rec = parse_message(msg, channel)
            if rec is None:
                continue
            with OUTPUT_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            count += 1
            if count % 200 == 0:
                log.info("  @%s: collected %d new (skipped %d dupes, seen %d msgs)",
                         channel, count, skipped, seen_msgs)
            await asyncio.sleep(0.01)  # courtesy
    except FloodWaitError as e:
        log.error("  FloodWait %d sec — bailing on @%s", e.seconds, channel)
    except Exception:
        log.exception("  unexpected error on @%s", channel)
    log.info("  ✓ @%s: %d new (%d dupes skipped, %d msgs seen)",
             channel, count, skipped, seen_msgs)
    return count


async def main_async(date_from: datetime) -> int:
    from pydantic import ValidationError
    from telethon import TelegramClient

    from src.services.receiver.config import load_settings
    try:
        s = load_settings()  # TG_API_ID / TG_API_HASH (+ TG_PHONE) from project .env
    except ValidationError as e:
        # pydantic's own message echoes input values — log field names only
        log.error("receiver settings missing/invalid in .env: %s",
                  ", ".join(f"{'.'.join(map(str, x['loc'])).upper()} ({x['type']})"
                            for x in e.errors()))
        return 1
    if not SESSION_PATH.with_suffix(".session").exists():
        log.error("Session file not found: %s.session", SESSION_PATH)
        return 1
    client = TelegramClient(str(SESSION_PATH), s.tg_api_id, s.tg_api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        log.error("Session NOT authorized — receiver session expired?")
        await client.disconnect()
        return 1
    me = await client.get_me()
    log.info("authorized as: %s id=%d", me.first_name, me.id)

    existing = load_existing_ids()

    total_new = 0
    for ch in CHANNELS:
        try:
            n = await scrape_channel(client, ch, date_from, existing)
            total_new += n
        except Exception:
            log.exception("channel %s failed", ch)

    await client.disconnect()
    log.info("=" * 50)
    log.info("DONE: %d new records appended to %s", total_new, OUTPUT_FILE)
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="date_from", default="2026-04-29",
                   help="YYYY-MM-DD inclusive")
    args = p.parse_args()
    date_from = datetime.strptime(args.date_from, "%Y-%m-%d")
    log.info("scrape window: %s -> now", date_from.date())
    sys.exit(asyncio.run(main_async(date_from)))


if __name__ == "__main__":
    main()
