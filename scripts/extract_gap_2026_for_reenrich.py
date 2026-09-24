"""Sprint 6.1 — Y5 extract: build re-enrich input for events 2026-04-21 → today.

Reads root telegram_news.jsonl (which we synced from duble3 + scraper gap-fill),
filters to events with datetime ∈ [from, to], formats as parquet matching the
deepinfra_runner.py input schema.  All 4 channels (rian, interfax, tass, rbc)
included — unlike Phase 2 sample which dropped tass+rbc.

Usage:
    python scripts/extract_gap_2026_for_reenrich.py \
        --jsonl D:\\quik_sber\\newsbot\\telegram_news.jsonl \
        --from 2026-04-21 --to 2026-06-07 \
        --output data/reenrich_phase2/gap_2026_04_21_to_today_input.parquet
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("extract_gap")

TEXT_CAP = 8000
EMOJI_PATTERN = re.compile(r"[☀-➿\U0001F000-\U0001FFFF]")
MSK = timezone(timedelta(hours=3))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jsonl", type=Path,
                   default=Path(r"D:\quik_sber\newsbot\telegram_news.jsonl"),
                   help="Source telegram_news.jsonl (will be read line-by-line)")
    p.add_argument("--from", dest="date_from", default="2026-04-21",
                   help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--to", dest="date_to", default="2026-06-07",
                   help="End date YYYY-MM-DD (exclusive)")
    p.add_argument("--output", type=Path,
                   default=PROJECT_ROOT / "data" / "reenrich_phase2" /
                            "gap_2026_04_21_to_today_input.parquet")
    p.add_argument("--include-non-session", action="store_true", default=True,
                   help="Include events outside MOEX trading hours (default: True)")
    return p.parse_args()


def is_moex_session(dt_msk: datetime) -> bool:
    """Crude MOEX session check (matches Phase 2 prompt logic)."""
    if dt_msk.weekday() >= 5:
        return False
    h, m = dt_msk.hour, dt_msk.minute
    minutes = h * 60 + m
    # 09:50 → 23:50 with mini-break around 14:00 — keep loose
    return 590 <= minutes <= 1430


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    args = parse_args()
    if not args.jsonl.exists():
        log.error("jsonl not found: %s", args.jsonl)
        return 1

    ts_from = datetime.strptime(args.date_from, "%Y-%m-%d")
    ts_to = datetime.strptime(args.date_to, "%Y-%m-%d")
    log.info("Window: %s -> %s", ts_from.isoformat(), ts_to.isoformat())
    log.info("Source: %s (%.1f MB)", args.jsonl, args.jsonl.stat().st_size / 1024**2)

    rows: list[dict] = []
    seen_text_hash: set[str] = set()
    n_seen = 0
    n_dup = 0
    n_short = 0
    n_skip_date = 0
    by_channel: dict[str, int] = {}

    with args.jsonl.open(encoding="utf-8") as f:
        for line in f:
            n_seen += 1
            if n_seen % 200_000 == 0:
                log.info("  read %d lines, kept %d", n_seen, len(rows))
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            dt_str = r.get("datetime")
            if not dt_str:
                continue
            try:
                dt = datetime.fromisoformat(dt_str)
            except ValueError:
                continue
            if dt < ts_from or dt >= ts_to:
                n_skip_date += 1
                continue

            full_text = (r.get("full_text") or "").strip()
            if len(full_text) < 30:
                n_short += 1
                continue
            text_for_hash = full_text[:TEXT_CAP]
            text_hash = hashlib.sha256(text_for_hash.encode("utf-8")).hexdigest()
            if text_hash in seen_text_hash:
                n_dup += 1
                continue
            seen_text_hash.add(text_hash)

            channel = r.get("channel") or "unknown"
            by_channel[channel] = by_channel.get(channel, 0) + 1

            headline = (r.get("headline") or full_text[:120]).strip()
            tg_msg_id = r.get("tg_msg_id")
            try:
                tg_msg_id_int = int(tg_msg_id)
            except (TypeError, ValueError):
                continue
            event_id = r.get("id") or hashlib.md5(
                f"{channel}_{tg_msg_id_int}_{headline[:50]}".encode()
            ).hexdigest()[:12]

            ts_utc = int(dt.timestamp())  # naive local treated as MSK
            rows.append({
                "tg_msg_id": tg_msg_id_int,
                "id": event_id,
                "channel": channel,
                "datetime_msk": dt,
                "timestamp_utc": ts_utc,
                "headline": headline[:500],
                "full_text": text_for_hash,
                "text_hash": text_hash,
                "has_emoji": bool(EMOJI_PATTERN.search(text_for_hash)),
                "is_session": is_moex_session(dt),
                "stratum_month": dt.month,
                "stratum_channel": channel,
                "has_phase2_anchor": False,
                "phase2_trade_id": None,
                "has_legacy_enrichment": False,
                "sample_set": "GAP_2026_04_21",
                "sample_seed": 0,
            })

    log.info("Read %d lines total", n_seen)
    log.info("  out of date window: %d", n_skip_date)
    log.info("  too short (<30 chars): %d", n_short)
    log.info("  text-hash dupes: %d", n_dup)
    log.info("  KEPT: %d", len(rows))
    log.info("By channel:")
    for k, v in sorted(by_channel.items(), key=lambda x: -x[1]):
        log.info("  %-25s %d", k, v)

    if not rows:
        log.error("no rows kept; nothing to write")
        return 1

    # Build polars df with explicit schema
    df = pl.DataFrame(rows, schema={
        "tg_msg_id": pl.Int64,
        "id": pl.Utf8,
        "channel": pl.Utf8,
        "datetime_msk": pl.Datetime,
        "timestamp_utc": pl.Int64,
        "headline": pl.Utf8,
        "full_text": pl.Utf8,
        "text_hash": pl.Utf8,
        "has_emoji": pl.Boolean,
        "is_session": pl.Boolean,
        "stratum_month": pl.Int32,
        "stratum_channel": pl.Utf8,
        "has_phase2_anchor": pl.Boolean,
        "phase2_trade_id": pl.Utf8,
        "has_legacy_enrichment": pl.Boolean,
        "sample_set": pl.Utf8,
        "sample_seed": pl.Int32,
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.output)
    log.info("wrote %s (%d rows × %d cols, %.1f MB)",
             args.output, df.height, df.width,
             args.output.stat().st_size / 1024**2)

    # Cost estimate at DI 70B
    avg_in_tokens = 4500  # empirical from prior runs
    avg_out_tokens = 80
    cost = (df.height * avg_in_tokens / 1e6 * 0.23
            + df.height * avg_out_tokens / 1e6 * 0.40)
    log.info("DI 70B cost estimate: $%.2f (%d events × ~4500 in + 80 out tokens)",
             cost, df.height)
    return 0


if __name__ == "__main__":
    sys.exit(main())
