r"""
scripts/extract_phase2_fold13_for_reenrich.py — Sprint 5.6 / Option A pre-step.

Извлекает Fold 13 train events из Phase 2 features_mfe.parquet, join'ит с
telegram_news.jsonl для получения original full_text, и сохраняет parquet
в формате, совместимом с scripts/deepinfra_runner.py.

Цель: подготовить input для re-enrichment via Llama 3.3 70b,
чтобы устранить distribution shift между training (Phase 2 legacy 8b/Haiku)
и production (Sprint 5 70b).

Walk-forward логика идентична `scripts/train_predictor_fold13.py`:
  TRAIN_MONTHS=12, TEST_MONTHS=3, STEP_MONTHS=3, PURGE=30min.
  Fold 13 train ≈ 2025-01-03 → 2026-01-03 ≈ ~16,300 events.

ID matching: features_mfe._id ↔ telegram_news.id. ID format одинаков
(12-char hex hash) — Phase 2 был построен из того же news_pool / telegram_news.

Output schema mirrors `sprint4/sampling/data/calibration_sample.parquet`,
поэтому deepinfra_runner.py читает её drop-in.

Запуск:
    python scripts/extract_phase2_fold13_for_reenrich.py
    python scripts/extract_phase2_fold13_for_reenrich.py --full       # все 70k, не только Fold 13 train
    python scripts/extract_phase2_fold13_for_reenrich.py --dry-run    # только match-statistics, без write

После extract — запустить deepinfra_runner:
    python scripts/deepinfra_runner.py \
        --input data/reenrich_phase2/fold13_train_input.parquet \
        --output-dir data/reenrich_phase2/checkpoints
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import orjson
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FEATURES = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\features_mfe.parquet"
)
DEFAULT_TELEGRAM_NEWS = PROJECT_ROOT / "docs" / "legacy promt" / "telegram_news.jsonl"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "reenrich_phase2"

# Walk-forward params (mirror train_predictor_fold13.py)
TRAIN_MONTHS = 12
TEST_MONTHS = 3
STEP_MONTHS = 3
PURGE = timedelta(minutes=30)
MIN_TRAIN_SAMPLES = 500
MIN_TEST_SAMPLES = 30

# Text cap (matches sprint4/sampling/build_sample.py)
TEXT_CAP = 8000

# Emoji detection — same regex как sprint4/sampling/build_sample.py
EMOJI_PATTERN = r"[☀-➿\U0001F000-\U0001FFFF]"

# Output schema — deepinfra_runner compatible
OUTPUT_SCHEMA = {
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
}

log = logging.getLogger("extract_phase2_fold13")


def determine_fold13_train_window(
    features: pd.DataFrame,
) -> tuple[pd.Timestamp, pd.Timestamp, int]:
    """Replicate train_predictor_fold13.py walk-forward точно. Returns (train_start, train_end, fold_idx)."""
    first_dt = features["_dt"].iloc[0]
    last_dt = features["_dt"].iloc[-1]
    log.info("Phase 2 features date range: %s → %s", first_dt.date(), last_dt.date())

    folds = []
    test_start = first_dt + pd.DateOffset(months=TRAIN_MONTHS)
    while True:
        test_end = test_start + pd.DateOffset(months=TEST_MONTHS)
        if test_end > last_dt:
            break
        train_end = test_start - PURGE
        train_mask = features["_dt"] < train_end
        test_mask = (features["_dt"] >= test_start) & (features["_dt"] < test_end)
        if train_mask.sum() >= MIN_TRAIN_SAMPLES and test_mask.sum() >= MIN_TEST_SAMPLES:
            folds.append({
                "test_start": test_start,
                "test_end": test_end,
                "train_end": train_end,
            })
        test_start += pd.DateOffset(months=STEP_MONTHS)

    fold_13 = folds[-1]
    # Train window: от первого events ДО train_end (без purge buffer)
    train_start = first_dt
    train_end = fold_13["train_end"]
    log.info(
        "Fold 13 train window: %s → %s (test: %s → %s)",
        train_start.date(), train_end.date(),
        fold_13["test_start"].date(), fold_13["test_end"].date(),
    )
    return train_start, train_end, len(folds)


def stream_telegram_lines(path: Path):
    """8MB chunk reader устойчивый к Windows OSError 22 на >2GB jsonl."""
    BUF = 8 * 1024 * 1024
    with open(path, "rb") as f:
        reader = io.BufferedReader(f, buffer_size=BUF)
        buf = b""
        while True:
            chunk = reader.read(BUF)
            if not chunk:
                if buf:
                    yield buf
                return
            buf += chunk
            lines = buf.split(b"\n")
            buf = lines[-1]
            for ln in lines[:-1]:
                yield ln


def extract_matching_events(
    telegram_news_path: Path, target_ids: set[str],
) -> dict[str, dict]:
    """Stream telegram_news.jsonl → собрать только записи с id ∈ target_ids."""
    log.info("streaming %s for %d target ids...", telegram_news_path, len(target_ids))
    t0 = time.perf_counter()
    matched: dict[str, dict] = {}
    n_lines = 0
    for line in stream_telegram_lines(telegram_news_path):
        n_lines += 1
        if not line:
            continue
        try:
            rec = orjson.loads(line)
        except Exception:
            continue
        rid = rec.get("id")
        if rid in target_ids:
            matched[rid] = rec
        if n_lines % 100_000 == 0:
            log.info(
                "  scanned %d lines, matched %d / %d (%.1f%%)",
                n_lines, len(matched), len(target_ids),
                100.0 * len(matched) / max(len(target_ids), 1),
            )
    log.info(
        "match done: %d/%d (%.1f%%) in %.1fs",
        len(matched), len(target_ids),
        100.0 * len(matched) / max(len(target_ids), 1),
        time.perf_counter() - t0,
    )
    return matched


def parse_msk_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.split("+")[0].split("Z")[0])
    except Exception:
        return None


def build_output_rows(matched: dict[str, dict]) -> list[dict]:
    """Convert matched telegram_news records → deepinfra_runner-compatible row dicts."""
    import re
    emoji_re = re.compile(EMOJI_PATTERN)
    rows = []
    for nid, rec in matched.items():
        full_text = (rec.get("full_text") or "")[:TEXT_CAP]
        dt_obj = parse_msk_datetime(rec.get("datetime"))
        text_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
        ts_utc = rec.get("timestamp")
        if not isinstance(ts_utc, (int, float)):
            ts_utc = 0
        rows.append({
            "tg_msg_id": rec.get("tg_msg_id"),
            "id": nid,
            "channel": rec.get("channel"),
            "datetime_msk": dt_obj,
            "timestamp_utc": int(ts_utc),
            "headline": rec.get("headline") or "",
            "full_text": full_text,
            "text_hash": text_hash,
            "has_emoji": bool(emoji_re.search(full_text)) if full_text else False,
            "is_session": True,  # Phase 2 фильтровал по session window перед entry
            "stratum_month": dt_obj.month if dt_obj else None,
            "stratum_channel": rec.get("channel"),
            "has_phase2_anchor": True,  # вся выборка = Phase 2 trade anchors
            "phase2_trade_id": None,
            "has_legacy_enrichment": True,  # эти 70k все имеют legacy enrichment
            "sample_set": "PHASE2_FOLD13_TRAIN",
            "sample_seed": 0,
        })
    return rows


def report_match_stats(target_ids: set[str], matched: dict[str, dict]) -> None:
    """Diagnostics: какой % matched, distribution по каналам/месяцам."""
    unmatched = target_ids - set(matched.keys())
    log.info("")
    log.info("=== Match statistics ===")
    log.info("  Target IDs:    %d", len(target_ids))
    log.info("  Matched:       %d (%.1f%%)", len(matched), 100.0 * len(matched) / max(len(target_ids), 1))
    log.info("  Unmatched:     %d", len(unmatched))
    if unmatched:
        log.info("  Sample unmatched IDs: %s", list(unmatched)[:10])

    if matched:
        by_channel: dict[str, int] = {}
        by_month: dict[str, int] = {}
        for rec in matched.values():
            ch = rec.get("channel", "?")
            by_channel[ch] = by_channel.get(ch, 0) + 1
            dt_str = rec.get("datetime", "")
            ym = dt_str[:7] if dt_str else "?"
            by_month[ym] = by_month.get(ym, 0) + 1
        log.info("")
        log.info("  By channel:")
        for ch, n in sorted(by_channel.items(), key=lambda x: -x[1]):
            log.info("    %-15s  %d", ch, n)
        log.info("")
        log.info("  By month (top 15):")
        for ym, n in sorted(by_month.items())[-15:]:
            log.info("    %s  %d", ym, n)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract Phase 2 Fold 13 train events for 70b re-enrichment")
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES)
    parser.add_argument("--telegram-news", type=Path, default=DEFAULT_TELEGRAM_NEWS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--full", action="store_true",
                        help="Все 70k events, не только Fold 13 train (~10x cost)")
    parser.add_argument("--rolling-months", type=int, default=0,
                        help="If >0: take ТОЛЬКО последние N months of train (rolling). "
                             "Default 0 = expanding window (точная репродукция Phase 2)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Только match statistics, без write parquet")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    if not args.features.exists():
        log.error("features parquet not found: %s", args.features)
        return 2
    if not args.telegram_news.exists():
        log.error("telegram_news.jsonl not found: %s", args.telegram_news)
        return 2

    # 1. Load features + determine target IDs
    log.info("loading features_mfe.parquet ...")
    t0 = time.perf_counter()
    features = pd.read_parquet(args.features, columns=["_id", "_datetime", "_ticker"])
    features["_id"] = features["_id"].astype(str)
    features["_dt"] = pd.to_datetime(features["_datetime"])
    features = features.sort_values("_dt").reset_index(drop=True)
    log.info("  loaded %d rows in %.1fs", len(features), time.perf_counter() - t0)

    if args.full:
        target_ids = set(features["_id"])
        scope = "FULL_70K"
        out_name = "full_70k_input.parquet"
        log.info("FULL mode: %d target ids (~10× cost vs Fold 13 train)", len(target_ids))
    else:
        train_start, train_end, n_folds = determine_fold13_train_window(features)
        if args.rolling_months > 0:
            rolling_start = train_end - pd.DateOffset(months=args.rolling_months)
            sub = features[
                (features["_dt"] >= rolling_start) & (features["_dt"] < train_end)
            ]
            target_ids = set(sub["_id"])
            scope = f"FOLD13_ROLLING_{args.rolling_months}MO"
            out_name = f"fold13_rolling_{args.rolling_months}mo_input.parquet"
            log.info(
                "Fold 13 ROLLING %d months train: %s → %s",
                args.rolling_months, rolling_start.date(), train_end.date(),
            )
            log.info("Rolling target ids: %d (vs expanding ~67k)", len(target_ids))
            log.warning(
                "⚠ Rolling window MENЯЕТ Phase 2 methodology (Phase 2 = expanding). "
                "Train will use less data than original Phase 2 backtest."
            )
        else:
            sub = features[
                (features["_dt"] >= train_start) & (features["_dt"] < train_end)
            ]
            target_ids = set(sub["_id"])
            scope = "FOLD13_TRAIN_EXPANDING"
            out_name = "fold13_train_input.parquet"
            log.info(
                "Fold 13 EXPANDING train (of %d folds, matches Phase 2): %d target ids",
                n_folds, len(target_ids),
            )

    # 2. Stream telegram_news.jsonl, match by id
    matched = extract_matching_events(args.telegram_news, target_ids)
    report_match_stats(target_ids, matched)

    if args.dry_run:
        log.info("")
        log.info("DRY-RUN — skipping parquet write.")
        log.info("If match rate looks OK (>90%%), re-run without --dry-run.")
        return 0

    # 3. Build + write parquet
    rows = build_output_rows(matched)
    if not rows:
        log.error("no rows to write — aborting")
        return 3

    df = pl.DataFrame(rows, schema=OUTPUT_SCHEMA, strict=False)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / out_name
    df.write_parquet(str(output_path))
    log.info("")
    log.info("written %s: %d rows × %d cols", output_path, df.height, df.width)

    # 4. Token estimate
    n = df.height
    tokens = n * 4500
    log.info("")
    log.info("=== Re-enrichment token estimate ===")
    log.info("  Events:        %d", n)
    log.info("  ~Tokens:       %d M", tokens // 1_000_000)
    log.info("")
    log.info("Next: launch deepinfra_runner in separate window:")
    log.info("  cd D:\\quik_sber\\newsbot\\newsbot3")
    log.info("  $env:PYTHONIOENCODING='utf-8'")
    log.info("  .\\.venv\\Scripts\\python.exe scripts\\deepinfra_runner.py `")
    log.info("      --input %s `", output_path.relative_to(PROJECT_ROOT))
    log.info("      --output-dir %s", (args.output_dir / "checkpoints").relative_to(PROJECT_ROOT))

    return 0


if __name__ == "__main__":
    sys.exit(main())
