"""
sprint4/reenrich/select_v1_anchor_pad.py — Phase 2 anchor pad для V1 holdout backtest.

Выделяет события из telegram_news.jsonl, которые anchor'ят Phase 2 трейды в окне V1
(2026-01-01 → 2026-04-30). На результирующем подмножестве запускается 70b enrichment
для OOS factorial check (B-кандидат на 8b vs 70b на одних и тех же anchor trades).

Логика matching: для каждого Phase 2 trade в V1 окне (3,300 best_combo × pro-rata ≈ 330)
ищется news в [ts_open - 60s, ts_open]. Если несколько — берём ближайшую к ts_open.

Output schema совместима с scripts/deepinfra_runner.py input — same columns как у calibration_sample.parquet
(id, tg_msg_id, channel, datetime_msk, timestamp_utc, headline, full_text, text_hash,
 has_emoji, has_phase2_anchor=True, phase2_trade_id).

Usage:
    python sprint4\\reenrich\\select_v1_anchor_pad.py

Output: sprint4/reenrich/data/v1/anchor_pad_v1.parquet
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_TG_NEWS = PROJECT_ROOT / "docs" / "legacy promt" / "telegram_news.jsonl"
DEFAULT_TRADES = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)
DEFAULT_OUTPUT = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "v1" / "anchor_pad_v1.parquet"

# V1 window (Sprint 4.2.b plan)
V1_START_UTC = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
V1_END_UTC = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp())  # exclusive

# Phase 2 best combo (4.0 winner)
PHASE2_BEST_COMBO = {"horizon_min": 60, "rr_threshold": 2.0, "model_type": "mx_specific"}

ANCHOR_WINDOW_SEC = 60
TEXT_CAP = 8000
EMOJI_PATTERN_BYTES = (
    bytes(range(0xE2, 0xE3)) + bytes(range(0xF0, 0xF1))  # CYR/emoji UTF-8 starts
)

log = logging.getLogger("v1_anchor_pad")


def load_phase2_v1_trades(trades_path: Path) -> list[tuple[int, float, str]]:
    """Returns [(trade_idx, ts_open_utc_seconds, ticker)] for V1 window trades."""
    df = pl.read_parquet(str(trades_path))
    log.info("phase2_trades loaded: %d total", df.height)

    # Filter best combo
    df = df.filter(
        (pl.col("horizon_min") == PHASE2_BEST_COMBO["horizon_min"])
        & (pl.col("rr_threshold") == PHASE2_BEST_COMBO["rr_threshold"])
        & (pl.col("model_type") == PHASE2_BEST_COMBO["model_type"])
    )
    log.info("after best_combo filter: %d", df.height)

    # ts_open naive MSK → UTC seconds (MSK=UTC+3)
    s = df["ts_open"]
    if s.dtype == pl.Datetime:
        ts_ms = s.dt.timestamp(time_unit="ms").cast(pl.Float64)
        ts_utc = (ts_ms / 1000.0) - 3 * 3600.0
    else:
        log.error("unexpected ts_open dtype: %s", s.dtype)
        return []

    df = df.with_columns(ts_utc.alias("_ts_utc"))

    # V1 window filter
    v1 = df.filter(
        (pl.col("_ts_utc") >= V1_START_UTC) & (pl.col("_ts_utc") < V1_END_UTC)
    )
    log.info("Phase 2 best_combo trades в V1 окне [2026-01-01, 2026-05-01) UTC: %d", v1.height)

    return [
        (i, row["_ts_utc"], row["ticker"])
        for i, row in enumerate(v1.iter_rows(named=True))
    ]


def stream_telegram_lines(path: Path):
    """Stream lines из large jsonl с устойчивым к Windows buffering чтением."""
    BUF_SIZE = 8 * 1024 * 1024
    with open(path, "rb") as f:
        reader = io.BufferedReader(f, buffer_size=BUF_SIZE)
        buf = b""
        while True:
            chunk = reader.read(BUF_SIZE)
            if not chunk:
                if buf:
                    yield buf
                return
            buf += chunk
            lines = buf.split(b"\n")
            buf = lines[-1]
            for ln in lines[:-1]:
                yield ln


def find_anchor_news(
    trades: list[tuple[int, float, str]],
    tg_news_path: Path,
) -> list[dict]:
    """Stream telegram_news → для каждого trade найти news в окне → unique список."""
    log.info("streaming telegram_news.jsonl ...")
    t0 = time.perf_counter()

    # Collect events in V1 window from jsonl
    events: list[dict] = []
    n_lines = 0
    for line in stream_telegram_lines(tg_news_path):
        n_lines += 1
        if not line:
            continue
        try:
            rec = orjson.loads(line)
        except Exception:
            continue
        ts = rec.get("timestamp")
        if not isinstance(ts, (int, float)):
            continue
        if not (V1_START_UTC <= ts < V1_END_UTC):
            continue
        events.append({
            "tg_msg_id": rec.get("tg_msg_id"),
            "id": rec.get("id"),
            "channel": rec.get("channel"),
            "datetime_str": rec.get("datetime"),
            "timestamp_utc": int(ts),
            "headline": rec.get("headline") or "",
            "full_text": (rec.get("full_text") or "")[:TEXT_CAP],
        })
        if n_lines % 100_000 == 0:
            log.info("  scanned %d lines, kept %d events in V1 window", n_lines, len(events))

    log.info("V1 window events: %d (from %d lines, %.1fs)",
             len(events), n_lines, time.perf_counter() - t0)

    if not events:
        return []

    # Sort by timestamp for binary search
    events.sort(key=lambda e: e["timestamp_utc"])
    ts_arr = [e["timestamp_utc"] for e in events]

    # For each trade — find anchor news
    import bisect
    anchor_news_by_id: dict[str, dict] = {}
    n_matched = 0
    n_no_match = 0

    for trade_idx, trade_ts, ticker in trades:
        lo = trade_ts - ANCHOR_WINDOW_SEC
        hi = trade_ts
        i_lo = bisect.bisect_left(ts_arr, lo)
        i_hi = bisect.bisect_right(ts_arr, hi)
        if i_lo == i_hi:
            n_no_match += 1
            continue
        # Take latest in window (closest to trade entry)
        best_news = events[i_hi - 1]
        nid = best_news["id"]
        if nid not in anchor_news_by_id:
            anchor_news_by_id[nid] = {
                **best_news,
                "phase2_trade_id": f"v1_anchor_{trade_idx}",
            }
        n_matched += 1

    log.info("matched %d trades to news, %d trades без news; %d unique anchor events",
             n_matched, n_no_match, len(anchor_news_by_id))
    return list(anchor_news_by_id.values())


def has_emoji_check(text: str) -> bool:
    """Roughly detect emoji через UTF-8 byte signatures."""
    if not text:
        return False
    # Check for emoji ranges in code points
    for ch in text:
        cp = ord(ch)
        # Common emoji blocks
        if 0x2600 <= cp <= 0x27BF:
            return True
        if 0x1F000 <= cp <= 0x1FFFF:
            return True
    return False


def parse_datetime_msk(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def build_parquet(anchor_events: list[dict], output: Path) -> int:
    """Build deepinfra_runner-compatible parquet."""
    rows = []
    for ev in anchor_events:
        full_text = ev["full_text"]
        dt_obj = parse_datetime_msk(ev["datetime_str"])
        text_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        rows.append({
            "tg_msg_id": ev["tg_msg_id"],
            "id": ev["id"],
            "channel": ev["channel"],
            "datetime_msk": dt_obj,
            "timestamp_utc": ev["timestamp_utc"],
            "headline": ev["headline"],
            "full_text": full_text,
            "text_hash": text_hash,
            "has_emoji": has_emoji_check(full_text),
            "is_session": True,  # placeholder — для V1 anchor pad не критично
            "stratum_month": dt_obj.month if dt_obj else None,
            "stratum_channel": ev["channel"],
            "has_phase2_anchor": True,
            "phase2_trade_id": ev["phase2_trade_id"],
            "has_legacy_enrichment": False,  # unknown w/o cross-check, default False
            "sample_set": "V1_ANCHOR_PAD",
            "sample_seed": 42,
        })
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
    }, strict=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(output))
    log.info("written %s: %d rows × %d cols", output, df.height, df.width)
    return df.height


def main() -> int:
    parser = argparse.ArgumentParser(description="V1 anchor pad selection (Phase 2 2026 anchors)")
    parser.add_argument("--telegram-news", type=Path, default=DEFAULT_TG_NEWS)
    parser.add_argument("--phase2-trades", type=Path, default=DEFAULT_TRADES)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    trades = load_phase2_v1_trades(args.phase2_trades)
    if not trades:
        log.error("no Phase 2 trades in V1 window — aborting")
        return 2

    anchor_events = find_anchor_news(trades, args.telegram_news)
    if not anchor_events:
        log.error("no anchor events matched — aborting")
        return 2

    n = build_parquet(anchor_events, args.output)
    log.info("DONE. %d anchor events written. Run deepinfra_runner:", n)
    log.info("  python scripts\\deepinfra_runner.py --model meta-llama/Llama-3.3-70B-Instruct \\")
    log.info("      --input %s --output-dir sprint4\\reenrich\\data\\v1",
             args.output.relative_to(PROJECT_ROOT))
    return 0


if __name__ == "__main__":
    sys.exit(main())
