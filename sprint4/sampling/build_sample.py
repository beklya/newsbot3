"""
sprint4/sampling/build_sample.py — Sprint 4 / Commit 4.3 (stratified sampler)

См. sprint4/sampling/plan.md для обоснований стратегии.

Pipeline:
  1. Стримит telegram_news.jsonl через polars scan_ndjson (880MB, 860k records)
  2. Дедуплицирует по full_text (~0.62% drop)
  3. Вычисляет is_session (MOEX Mon-Fri 10:00-23:50 MSK), has_emoji, stratum_*
  4. Фильтрует окна C1 (2025-04..12) и V1 (2026-01..04)
  5. Стратифицированно сэмплит per (month × channel × session) с natural mix
  6. Force-include Phase 2 anchor news (только C1) поверх квот
  7. Помечает has_legacy_enrichment по news_pool.jsonl
  8. Пишет parquet + stratification_report.txt

Запуск:
  python sprint4\\sampling\\build_sample.py
  python sprint4\\sampling\\build_sample.py --seed 42

Ожидаемое время: ~1-2 минуты на полный проход.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

# ----------------------------------------------------------------------------
# Defaults
# ----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "docs" / "legacy promt" / "telegram_news.jsonl"
DEFAULT_PHASE2 = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)
DEFAULT_LEGACY_POOL = PROJECT_ROOT / "docs" / "legacy promt" / "news_pool.jsonl"
DEFAULT_OUTPUT = PROJECT_ROOT / "sprint4" / "sampling" / "data"

# Windows (UTC seconds) — упрощают сравнение с source.timestamp
C1_START = int(datetime(2025, 4, 1, tzinfo=timezone.utc).timestamp())
C1_END = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())  # exclusive
V1_START = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
V1_END = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp())  # exclusive

# Natural channel mix (4.2.a discovery)
CHANNEL_SHARES = {
    "tass_agency": 0.41,
    "rian_ru": 0.35,
    "rbc_news": 0.16,
    "interfaxonline": 0.08,
}

# Targets (см. plan.md)
C1_SESSION_PER_MONTH = 2444    # × 9 мес ≈ 22,000
C1_OFFSESSION_PER_MONTH = 889  # × 9 мес ≈ 8,000
V1_SESSION_PER_MONTH = 1375    # × 4 мес ≈ 5,500
V1_OFFSESSION_PER_MONTH = 500  # × 4 мес ≈ 2,000
V1_ANCHOR_PAD = 500            # резерв на 2026 backtests

# MOEX session bounds (MSK)
SESSION_START_H, SESSION_START_M = 10, 0
SESSION_END_H, SESSION_END_M = 23, 50

ANCHOR_WINDOW_SEC = 60   # окно поиска anchor news относительно entry_time

# Phase 2 best combo (Sprint 4.0): h=60 + rr=2.0 + mx_specific → 3,300 trades, Sharpe 4.98
PHASE2_BEST_COMBO = {"horizon_min": 60, "rr_threshold": 2.0, "model_type": "mx_specific"}

TEXT_CAP = 8000          # обрезка full_text (RawNewsPayload.text max=10000 минус headroom)

# Emoji range — основные Unicode-блоки эмодзи
EMOJI_PATTERN = r"[☀-➿\U0001F000-\U0001FFFF]"

log = logging.getLogger("build_sample")


# ----------------------------------------------------------------------------
# Source loading
# ----------------------------------------------------------------------------
def load_source(source_path: Path) -> pl.DataFrame:
    """Read telegram_news.jsonl into DataFrame with computed strata columns."""
    log.info("scanning %s ...", source_path)
    t0 = _time.perf_counter()

    # scan_ndjson + select — column projection pushdown skips 'analysis' и пр.
    lf = pl.scan_ndjson(str(source_path)).select([
        pl.col("tg_msg_id").cast(pl.Int64),
        pl.col("id").cast(pl.Utf8),
        pl.col("channel").cast(pl.Utf8),
        pl.col("datetime").cast(pl.Utf8),
        pl.col("timestamp").cast(pl.Int64),
        pl.col("headline").cast(pl.Utf8),
        pl.col("full_text").cast(pl.Utf8),
    ])

    # Парсим naive MSK datetime
    lf = lf.with_columns([
        pl.col("datetime")
        .str.to_datetime(format="%Y-%m-%dT%H:%M:%S", strict=False)
        .alias("dt_msk"),
        pl.col("full_text").str.slice(0, TEXT_CAP).alias("full_text"),
    ])

    lf = lf.with_columns([
        pl.col("dt_msk").dt.weekday().alias("weekday"),  # Mon=1..Sun=7
        pl.col("dt_msk").dt.hour().alias("hour"),
        pl.col("dt_msk").dt.minute().alias("minute"),
        pl.col("dt_msk").dt.month().alias("stratum_month"),
        pl.col("dt_msk").dt.year().alias("stratum_year"),
        pl.col("channel").alias("stratum_channel"),
        pl.col("full_text").str.contains(EMOJI_PATTERN).alias("has_emoji"),
    ])

    # is_session: будни (1..5) AND 10:00 ≤ time ≤ 23:50 MSK
    lf = lf.with_columns(
        (
            (pl.col("weekday") <= 5)
            & (
                (pl.col("hour") > SESSION_START_H)
                | ((pl.col("hour") == SESSION_START_H) & (pl.col("minute") >= SESSION_START_M))
            )
            & (
                (pl.col("hour") < SESSION_END_H)
                | ((pl.col("hour") == SESSION_END_H) & (pl.col("minute") <= SESSION_END_M))
            )
        ).alias("is_session")
    )

    df = lf.collect()
    log.info("source loaded: %d records in %.1fs", df.height, _time.perf_counter() - t0)
    return df


# ----------------------------------------------------------------------------
# Phase 2 anchor timestamps
# ----------------------------------------------------------------------------
def load_phase2_anchors(
    trades_path: Path, window_start: int, window_end: int
) -> list[float]:
    """Return list of trade entry timestamps (UTC seconds) within [start, end)."""
    if not trades_path.exists():
        log.warning("phase2_trades not found at %s — anchor force-include disabled", trades_path)
        return []
    df = pl.read_parquet(str(trades_path))
    log.info("phase2_trades loaded: n=%d cols=%s", df.height, df.columns)

    # Filter на Phase 2 best combo (Sprint 4.0) — иначе anchors взорвут C1
    combo_cols = set(PHASE2_BEST_COMBO).intersection(df.columns)
    if combo_cols == set(PHASE2_BEST_COMBO):
        filter_expr = None
        for k, v in PHASE2_BEST_COMBO.items():
            cond = pl.col(k) == v
            filter_expr = cond if filter_expr is None else filter_expr & cond
        df = df.filter(filter_expr)
        log.info("phase2_trades: filtered to best combo %s -> n=%d", PHASE2_BEST_COMBO, df.height)
    else:
        log.warning(
            "phase2_trades: missing best-combo columns %s — using ALL trades (anchors may blow up sample)",
            set(PHASE2_BEST_COMBO) - combo_cols,
        )

    candidates = [
        "ts_open", "entry_time", "entry_dt", "entry_timestamp", "entry_ts",
        "signal_time", "news_time", "datetime", "dt", "ts",
    ]
    entry_col = next((c for c in candidates if c in df.columns), None)
    if entry_col is None:
        log.warning(
            "phase2_trades: no recognized entry-time column among %s — anchor disabled",
            candidates,
        )
        return []

    s = df[entry_col]
    if s.dtype == pl.Datetime:
        # Naive datetime — assume MSK; MSK = UTC+3
        ts = (s.dt.timestamp(time_unit="ms").cast(pl.Float64) / 1000.0) - (3 * 3600.0)
    elif s.dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32):
        first = next((v for v in s.head(5).to_list() if v is not None), None)
        if first is not None and first > 1e12:
            ts = s.cast(pl.Float64) / 1000.0  # ms → s
        else:
            ts = s.cast(pl.Float64)
    elif s.dtype == pl.Utf8:
        ts = (
            s.str.to_datetime(strict=False)
             .dt.timestamp(time_unit="ms")
             .cast(pl.Float64) / 1000.0
        ) - (3 * 3600.0)
    else:
        log.warning("phase2_trades.%s: unsupported dtype %s — anchor disabled", entry_col, s.dtype)
        return []

    arr = ts.to_list()
    in_window = [t for t in arr if t is not None and window_start <= t < window_end]
    log.info(
        "phase2_trades: %d / %d entries fall in window [%d, %d)",
        len(in_window), len(arr), window_start, window_end,
    )
    return in_window


def find_anchor_news(
    df: pl.DataFrame, anchor_timestamps: list[float], window_sec: int
) -> set[str]:
    """Return set of news ids whose timestamp falls in (trade_ts - window, trade_ts]."""
    if not anchor_timestamps:
        return set()
    log.info(
        "matching %d anchor timestamps against %d news in C1 pool (window=%ds)...",
        len(anchor_timestamps), df.height, window_sec,
    )
    df_sorted = df.sort("timestamp")
    ts_arr = df_sorted["timestamp"].to_numpy()
    id_arr = df_sorted["id"].to_list()

    matches: set[str] = set()
    for trade_ts in anchor_timestamps:
        lo = trade_ts - window_sec
        hi = trade_ts
        i_lo = int(np.searchsorted(ts_arr, lo, side="left"))
        i_hi = int(np.searchsorted(ts_arr, hi, side="right"))
        if i_lo == i_hi:
            continue
        # Берём ближайшую к trade_ts новость (последнюю в окне)
        matches.add(id_arr[i_hi - 1])
    log.info("anchors matched: %d unique news ids", len(matches))
    return matches


# ----------------------------------------------------------------------------
# Legacy ids — для has_legacy_enrichment flag
# ----------------------------------------------------------------------------
def load_legacy_ids(legacy_pool_path: Path) -> set[str]:
    """Stream-parse news_pool.jsonl, extract `id` field only (file ~2.5GB)."""
    if not legacy_pool_path.exists():
        log.warning(
            "legacy news_pool not found at %s — has_legacy_enrichment всегда False",
            legacy_pool_path,
        )
        return set()
    log.info("streaming legacy news_pool ids from %s ...", legacy_pool_path)
    t0 = _time.perf_counter()

    ids: set[str] = set()
    n_lines = 0
    with open(legacy_pool_path, "r", encoding="utf-8") as f:
        for line in f:
            # Быстрое извлечение "id":"<12-char hex>"
            i = line.find('"id":')
            if i < 0:
                continue
            j = line.find('"', i + 5)
            if j < 0:
                continue
            k = line.find('"', j + 1)
            if k < 0:
                continue
            ids.add(line[j + 1:k])
            n_lines += 1
            if n_lines % 200_000 == 0:
                log.info("  legacy ids streamed: %d", n_lines)
    log.info(
        "legacy ids loaded: %d unique (%d lines) in %.1fs",
        len(ids), n_lines, _time.perf_counter() - t0,
    )
    return ids


# ----------------------------------------------------------------------------
# Stratified sampling
# ----------------------------------------------------------------------------
def build_target_table(
    months: list[int],
    session_per_month: int,
    offsession_per_month: int,
) -> dict[tuple[int, str, bool], int]:
    """(month, channel, is_session) → target n."""
    targets: dict[tuple[int, str, bool], int] = {}
    for month in months:
        for channel, share in CHANNEL_SHARES.items():
            targets[(month, channel, True)] = round(session_per_month * share)
            targets[(month, channel, False)] = round(offsession_per_month * share)
    return targets


def stratified_sample(
    df: pl.DataFrame,
    target_per_stratum: dict[tuple[int, str, bool], int],
    seed: int,
) -> tuple[pl.DataFrame, list[dict]]:
    """Sample per (month, channel, is_session) stratum. Returns (df, report)."""
    chunks: list[pl.DataFrame] = []
    report: list[dict] = []
    for (month, channel, is_session), target in sorted(target_per_stratum.items()):
        pool = df.filter(
            (pl.col("stratum_month") == month)
            & (pl.col("stratum_channel") == channel)
            & (pl.col("is_session") == is_session)
        )
        available = pool.height
        take = min(target, available)
        if take > 0:
            stratum_seed = seed + month * 1000 + (hash(channel) % 100) + (1 if is_session else 0)
            chunk = pool.sample(n=take, seed=stratum_seed)
            chunks.append(chunk)
        report.append({
            "month": month,
            "channel": channel,
            "is_session": is_session,
            "target": target,
            "available": available,
            "sampled": take,
            "shortfall": max(0, target - available),
        })
    sampled = pl.concat(chunks) if chunks else df.head(0)
    return sampled, report


# ----------------------------------------------------------------------------
# Output assembly
# ----------------------------------------------------------------------------
def add_text_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Compute SHA256(full_text) — дёшево на ~40k записей."""
    hashes = [hashlib.sha256(t.encode("utf-8")).hexdigest() for t in df["full_text"].to_list()]
    return df.with_columns(pl.Series("text_hash", hashes))


def finalize_columns(df: pl.DataFrame, sample_set: str, seed: int) -> pl.DataFrame:
    """Project to final parquet schema (см. plan.md)."""
    return df.select([
        pl.col("tg_msg_id"),
        pl.col("id"),
        pl.col("channel"),
        pl.col("dt_msk").alias("datetime_msk"),
        pl.col("timestamp").alias("timestamp_utc"),
        pl.col("headline"),
        pl.col("full_text"),
        pl.col("text_hash"),
        pl.col("has_emoji"),
        pl.col("is_session"),
        pl.col("stratum_month").cast(pl.Int32),
        pl.col("stratum_channel"),
        pl.col("has_phase2_anchor"),
        pl.col("phase2_trade_id"),
        pl.col("has_legacy_enrichment"),
        pl.lit(sample_set).alias("sample_set"),
        pl.lit(seed).cast(pl.Int32).alias("sample_seed"),
    ])


# ----------------------------------------------------------------------------
# Report
# ----------------------------------------------------------------------------
def write_report(path: Path, **kw) -> None:
    lines: list[str] = []
    lines.append("=" * 72)
    lines.append("  Sprint 4 / Commit 4.3 — Stratified Sample Report")
    lines.append("=" * 72)
    lines.append("")
    lines.append(f"Seed:                       {kw['seed']}")
    lines.append(f"Source records (pre-dedup): {kw['source_total']:>10,}")
    lines.append(f"After dedup full_text:      {kw['after_dedup']:>10,}")
    lines.append(f"Dropped as duplicates:      {kw['source_total'] - kw['after_dedup']:>10,}")
    lines.append("")
    lines.append("-- C1 (calibration) --")
    lines.append(f"  Window pool 2025-04..12:  {kw['c1_pool']:>10,}")
    lines.append(f"  Final sample:             {kw['c1_total']:>10,}")
    lines.append(f"  Emoji records:            {kw['c1_emoji']:>10,}")
    lines.append(f"  Has legacy enrichment:    {kw['c1_legacy']:>10,}")
    lines.append(f"  Phase 2 anchors found:    {kw['anchors_total']:>10,}")
    lines.append(f"  Anchors in random sample: {kw['anchors_in_random']:>10,}")
    lines.append(f"  Anchors force-added:      {kw['anchors_added']:>10,}")
    lines.append("")
    lines.append("-- C1 strata --")
    lines.append("  month  channel        session    target   avail   sampled  short")
    for r in kw["c1_strat_report"]:
        sess = "session" if r["is_session"] else "offsess"
        flag = " ⚠" if r["shortfall"] > 0 else ""
        lines.append(
            f"   {r['month']:2d}    {r['channel']:14s}  {sess}   "
            f"{r['target']:6d}  {r['available']:6d}   {r['sampled']:6d}  "
            f"{r['shortfall']:5d}{flag}"
        )
    lines.append("")
    lines.append("-- V1 (validation holdout) --")
    lines.append(f"  Window pool 2026-01..04:  {kw['v1_pool']:>10,}")
    lines.append(f"  Final sample:             {kw['v1_total']:>10,}")
    lines.append(f"  Emoji records:            {kw['v1_emoji']:>10,}")
    lines.append(f"  Has legacy enrichment:    {kw['v1_legacy']:>10,}")
    lines.append("")
    lines.append("-- V1 strata --")
    lines.append("  month  channel        session    target   avail   sampled  short")
    for r in kw["v1_strat_report"]:
        sess = "session" if r["is_session"] else "offsess"
        flag = " ⚠" if r["shortfall"] > 0 else ""
        lines.append(
            f"   {r['month']:2d}    {r['channel']:14s}  {sess}   "
            f"{r['target']:6d}  {r['available']:6d}   {r['sampled']:6d}  "
            f"{r['shortfall']:5d}{flag}"
        )
    lines.append("")
    lines.append("-- Cross-set safety --")
    lines.append(f"  C1 ↔ V1 id overlap:       {kw['c1_v1_overlap']:>10,}  (must be 0)")
    lines.append("")
    lines.append("=" * 72)
    path.write_text("\n".join(lines), encoding="utf-8")
    log.info("report written to %s", path)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4 / 4.3 stratified sampler")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--phase2-trades", type=Path, default=DEFAULT_PHASE2)
    parser.add_argument("--legacy-pool", type=Path, default=DEFAULT_LEGACY_POOL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    args.output.mkdir(parents=True, exist_ok=True)

    # 1. Load source
    df_full = load_source(args.source)
    source_total = df_full.height

    # 2. Dedup by full_text
    df = df_full.unique(subset=["full_text"], keep="first", maintain_order=True)
    log.info(
        "dedup: %d -> %d (-%d, %.2f%%)",
        source_total, df.height, source_total - df.height,
        100.0 * (source_total - df.height) / max(source_total, 1),
    )

    # 3. Load legacy ids
    legacy_ids = load_legacy_ids(args.legacy_pool)

    # 4. C1
    c1_pool = df.filter(
        (pl.col("timestamp") >= C1_START) & (pl.col("timestamp") < C1_END)
    )
    log.info("C1 window pool: %d records", c1_pool.height)

    c1_targets = build_target_table(
        months=list(range(4, 13)),
        session_per_month=C1_SESSION_PER_MONTH,
        offsession_per_month=C1_OFFSESSION_PER_MONTH,
    )
    c1_random, c1_strat = stratified_sample(c1_pool, c1_targets, seed=args.seed)
    log.info("C1 random stratified: %d records", c1_random.height)

    # 5. Anchors
    anchor_ts = load_phase2_anchors(args.phase2_trades, C1_START, C1_END)
    anchor_ids = find_anchor_news(c1_pool, anchor_ts, ANCHOR_WINDOW_SEC)
    in_random = set(c1_random["id"].to_list()) & anchor_ids
    to_add = anchor_ids - in_random
    log.info(
        "anchors: matched=%d in_random=%d to_add=%d",
        len(anchor_ids), len(in_random), len(to_add),
    )
    anchor_extras = c1_pool.filter(pl.col("id").is_in(list(to_add))) if to_add else c1_pool.head(0)

    c1 = pl.concat([c1_random, anchor_extras])
    c1 = c1.with_columns([
        pl.col("id").is_in(list(anchor_ids)).alias("has_phase2_anchor"),
        pl.lit(None, dtype=pl.Utf8).alias("phase2_trade_id"),
        pl.col("id").is_in(list(legacy_ids)).alias("has_legacy_enrichment"),
    ])
    c1 = add_text_hash(c1)
    c1_final = finalize_columns(c1, "C1", args.seed)
    c1_path = args.output / "calibration_sample.parquet"
    c1_final.write_parquet(c1_path)
    log.info("written %s: %d rows", c1_path, c1_final.height)

    # 6. V1
    v1_pool = df.filter(
        (pl.col("timestamp") >= V1_START) & (pl.col("timestamp") < V1_END)
    )
    log.info("V1 window pool: %d records", v1_pool.height)

    v1_targets = build_target_table(
        months=list(range(1, 5)),
        session_per_month=V1_SESSION_PER_MONTH,
        offsession_per_month=V1_OFFSESSION_PER_MONTH,
    )
    v1_random, v1_strat = stratified_sample(v1_pool, v1_targets, seed=args.seed)
    log.info("V1 random stratified: %d records", v1_random.height)

    # V1 pad
    sampled_ids = set(v1_random["id"].to_list())
    pad_pool = v1_pool.filter(~pl.col("id").is_in(list(sampled_ids)))
    pad_n = min(V1_ANCHOR_PAD, pad_pool.height)
    v1_pad = pad_pool.sample(n=pad_n, seed=args.seed + 99) if pad_n > 0 else pad_pool.head(0)
    log.info("V1 pad: %d records", v1_pad.height)

    v1 = pl.concat([v1_random, v1_pad])
    v1 = v1.with_columns([
        pl.lit(False).alias("has_phase2_anchor"),
        pl.lit(None, dtype=pl.Utf8).alias("phase2_trade_id"),
        pl.col("id").is_in(list(legacy_ids)).alias("has_legacy_enrichment"),
    ])
    v1 = add_text_hash(v1)
    v1_final = finalize_columns(v1, "V1", args.seed)
    v1_path = args.output / "validation_sample.parquet"
    v1_final.write_parquet(v1_path)
    log.info("written %s: %d rows", v1_path, v1_final.height)

    # 7. Cross-set safety
    overlap = set(c1_final["id"].to_list()) & set(v1_final["id"].to_list())
    log.info("C1 ↔ V1 id overlap: %d (must be 0)", len(overlap))
    if overlap:
        log.warning("OVERLAP DETECTED — sample dates likely cross window boundary; investigate")

    # 8. Report
    write_report(
        args.output / "stratification_report.txt",
        seed=args.seed,
        source_total=source_total,
        after_dedup=df.height,
        c1_pool=c1_pool.height,
        v1_pool=v1_pool.height,
        c1_strat_report=c1_strat,
        v1_strat_report=v1_strat,
        anchors_total=len(anchor_ids),
        anchors_in_random=len(in_random),
        anchors_added=len(to_add),
        c1_total=c1_final.height,
        v1_total=v1_final.height,
        c1_emoji=int(c1_final["has_emoji"].sum()),
        v1_emoji=int(v1_final["has_emoji"].sum()),
        c1_legacy=int(c1_final["has_legacy_enrichment"].sum()),
        v1_legacy=int(v1_final["has_legacy_enrichment"].sum()),
        c1_v1_overlap=len(overlap),
    )

    log.info("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
