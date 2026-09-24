"""
sprint4/reenrich/sample_for_4_6.py — Sprint 4 / Commit 4.6 stratified subset (2,000 events).

Берёт C1 sample (30,518 events) и стратифицированно sample'ит 2,000 по category,
oversample'я tradable categories (cbr, corporate) и undersample'я noise (other).

Priority категории:
  1. category из 4.5 enrichment (если event уже обработан) — наиболее точная
  2. legacy_category из news_pool.jsonl (fallback, ~83% C1 coverage)
  3. "unknown" — для не-enriched и без legacy

Targets (~2,000 total):
  cbr=350, corporate=400, commodity=250, currency=250,
  geopolitics=250, macro=250, market=100, other=150

При нехватке в страте: max возможное + warning в логе.
При избытке в "unknown": sample uniformly до удержания общего total ≈ 2000.

Запуск:
  python sprint4\\reenrich\\sample_for_4_6.py
  python sprint4\\reenrich\\sample_for_4_6.py --seed 42

Перед запуском: должны существовать:
  - sprint4/sampling/data/calibration_sample.parquet (от 4.3)
  - sprint4/reenrich/data/legacy_categories.parquet (от extract_legacy_categories.py)

Output: sprint4/reenrich/data/c1_subset_for_4_6.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_C1 = PROJECT_ROOT / "sprint4" / "sampling" / "data" / "calibration_sample.parquet"
DEFAULT_LEGACY_CAT = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "legacy_categories.parquet"
DEFAULT_4_5_AGG = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_llama_3_1_8b_instant_v1_0_0.parquet"
DEFAULT_OUTPUT = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_subset_for_4_6.parquet"

# Per-category targets (см. docstring)
TARGETS: dict[str, int] = {
    "cbr": 350,
    "corporate": 400,
    "commodity": 250,
    "currency": 250,
    "geopolitics": 250,
    "macro": 250,
    "market": 100,
    "other": 150,
}
TARGET_TOTAL = sum(TARGETS.values())  # = 2,000

# Legacy categories тот же набор плюс одну новую — "market". Legacy "market" не имеет,
# поэтому legacy other может содержать события которые new prompt классифицирует как market.
# Для стратификации это терпимо.

log = logging.getLogger("sample_for_4_6")


def load_inputs(
    c1_path: Path, legacy_cat_path: Path, agg_4_5_path: Path | None,
) -> pl.DataFrame:
    """Load C1 + join legacy categories + (optional) join 4.5 aggregate."""
    c1 = pl.read_parquet(str(c1_path))
    log.info("C1 sample: %d rows × %d cols", c1.height, c1.width)

    if not legacy_cat_path.exists():
        log.warning(
            "legacy categories not found at %s — запусти extract_legacy_categories.py",
            legacy_cat_path,
        )
        legacy = pl.DataFrame(
            {"id": [], "legacy_category": []},
            schema={"id": pl.Utf8, "legacy_category": pl.Utf8},
        )
    else:
        legacy = pl.read_parquet(str(legacy_cat_path)).select(["id", "legacy_category"])
        log.info("legacy categories: %d rows", legacy.height)

    df = c1.join(legacy, on="id", how="left")

    # 4.5 aggregate (если есть) — override legacy_category
    if agg_4_5_path and agg_4_5_path.exists():
        agg = pl.read_parquet(str(agg_4_5_path)).select(["id", "category", "is_enriched"])
        df = df.join(agg, on="id", how="left")
        log.info(
            "4.5 aggregate: %d total, %d enriched",
            agg.height, int(agg["is_enriched"].fill_null(False).sum()),
        )
        # Effective category: 4.5 priority, fallback to legacy, fallback to "unknown"
        df = df.with_columns(
            pl.when(pl.col("is_enriched") & pl.col("category").is_not_null())
              .then(pl.col("category"))
              .when(pl.col("legacy_category").is_not_null())
              .then(pl.col("legacy_category"))
              .otherwise(pl.lit("unknown"))
              .alias("strat_category"),
            pl.when(pl.col("is_enriched") & pl.col("category").is_not_null())
              .then(pl.lit("4.5"))
              .when(pl.col("legacy_category").is_not_null())
              .then(pl.lit("legacy"))
              .otherwise(pl.lit("unknown"))
              .alias("strat_source"),
        )
    else:
        log.info("4.5 aggregate not found at %s — using legacy only", agg_4_5_path)
        df = df.with_columns(
            pl.col("legacy_category").fill_null("unknown").alias("strat_category"),
            pl.when(pl.col("legacy_category").is_not_null())
              .then(pl.lit("legacy"))
              .otherwise(pl.lit("unknown"))
              .alias("strat_source"),
        )

    return df


def stratified_sample(
    df: pl.DataFrame, targets: dict[str, int], seed: int,
) -> tuple[pl.DataFrame, list[dict]]:
    """Sample per strat_category. Returns (sample_df, report)."""
    chunks: list[pl.DataFrame] = []
    report: list[dict] = []
    for category, target in sorted(targets.items()):
        pool = df.filter(pl.col("strat_category") == category)
        available = pool.height
        take = min(target, available)
        if take > 0:
            chunk = pool.sample(n=take, seed=seed + (hash(category) % 10_000))
            chunks.append(chunk)
        report.append({
            "category": category,
            "target": target,
            "available": available,
            "sampled": take,
            "shortfall": max(0, target - available),
        })

    sampled = pl.concat(chunks) if chunks else df.head(0)
    return sampled, report


def topup_targets(existing_sample: pl.DataFrame, c1_with_strat: pl.DataFrame) -> dict[str, int]:
    """Compute per-category gaps based on existing sample re-classified via 4.5 categories.

    Использует ТЕКУЩЕЕ распределение strat_category (с приоритетом 4.5),
    а не legacy при котором был построен оригинальный sample.
    """
    # Re-classify existing sample by current strat_category
    existing_ids = existing_sample["id"]
    existing_with_strat = c1_with_strat.filter(pl.col("id").is_in(existing_ids))
    cur_counts = dict(
        existing_with_strat
        .group_by("strat_category")
        .agg(pl.len().alias("n"))
        .iter_rows()
    )
    gaps: dict[str, int] = {}
    for cat, target in TARGETS.items():
        cur = cur_counts.get(cat, 0)
        gap = max(0, target - cur)
        if gap > 0:
            gaps[cat] = gap
    return gaps


def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4.6 stratified subset")
    parser.add_argument("--c1", type=Path, default=DEFAULT_C1)
    parser.add_argument("--legacy-categories", type=Path, default=DEFAULT_LEGACY_CAT)
    parser.add_argument("--agg-4-5", type=Path, default=DEFAULT_4_5_AGG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-4-5-override", action="store_true",
                        help="Игнорировать 4.5 aggregate, использовать только legacy")
    parser.add_argument("--topup", action="store_true",
                        help="Top-up mode: дочерпать недостающие категории из 4.5-классифицированных events, "
                             "исключая уже-просэмплированные ID")
    parser.add_argument("--existing-sample", type=Path, default=DEFAULT_OUTPUT,
                        help="(topup mode) уже-просэмплированные events для exclude")
    parser.add_argument("--topup-output", type=Path,
                        default=PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_subset_for_4_6_topup.parquet",
                        help="(topup mode) куда сохранять topup events")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    df = load_inputs(
        args.c1,
        args.legacy_categories,
        None if args.no_4_5_override else args.agg_4_5,
    )
    log.info("joined: %d rows × %d cols", df.height, df.width)

    # --- TOPUP MODE ---
    if args.topup:
        log.info("")
        log.info("=== TOPUP MODE ===")
        if not args.existing_sample.exists():
            log.error("existing sample not found at %s", args.existing_sample)
            return 2
        existing = pl.read_parquet(str(args.existing_sample))
        log.info("existing sample: %d rows", existing.height)

        # Compute per-category gaps based on 4.5 (new) classification
        gaps = topup_targets(existing, df)
        if not gaps:
            log.info("no gaps — all categories already met TARGETS via current strat_category")
            return 0
        log.info("topup gaps (4.5-categorized):")
        for cat, gap in sorted(gaps.items()):
            log.info("  %-15s  +%d", cat, gap)
        log.info("  TOTAL topup target = %d", sum(gaps.values()))

        # Exclude already-sampled IDs
        excl = set(existing["id"].to_list())
        pool = df.filter(~pl.col("id").is_in(excl))
        log.info("pool after exclude: %d rows", pool.height)

        # ONLY use rows with 4.5 enrichment (strat_source == "4.5") для topup —
        # 4.5 categories более надёжны чем legacy. Если 4.5 ещё не покрывает категорию —
        # fallback на legacy с warning.
        if "strat_source" in pool.columns:
            pool_4_5 = pool.filter(pl.col("strat_source") == "4.5")
            log.info("pool with 4.5 classification: %d rows", pool_4_5.height)
            sample_pool = pool_4_5
        else:
            sample_pool = pool

        sample, report = stratified_sample(sample_pool, gaps, args.seed + 7)

        # Если 4.5 покрытие не закрыло gap — добираем из legacy
        actual_per_cat = dict(
            sample.group_by("strat_category").agg(pl.len().alias("n")).iter_rows()
        )
        legacy_pool = pool.filter(pl.col("strat_source") == "legacy") if "strat_source" in pool.columns else pl.DataFrame()
        legacy_chunks = []
        for cat, gap in gaps.items():
            cur = actual_per_cat.get(cat, 0)
            remaining = gap - cur
            if remaining <= 0:
                continue
            legacy_for_cat = legacy_pool.filter(pl.col("strat_category") == cat) if legacy_pool.height > 0 else pl.DataFrame()
            if legacy_for_cat.height == 0:
                log.warning("topup %s: gap=%d, 4.5_done=%d, no legacy fallback — short %d",
                            cat, gap, cur, remaining)
                continue
            take = min(remaining, legacy_for_cat.height)
            chunk = legacy_for_cat.sample(n=take, seed=args.seed + (hash(cat) % 100))
            legacy_chunks.append(chunk)
            log.info("topup %s: 4.5=%d + legacy_fallback=%d (gap was %d)",
                     cat, cur, take, gap)

        if legacy_chunks:
            sample = pl.concat([sample] + legacy_chunks)

        # Write
        args.topup_output.parent.mkdir(parents=True, exist_ok=True)
        sample.write_parquet(str(args.topup_output))
        log.info("")
        log.info("topup written %s: %d rows × %d cols", args.topup_output, sample.height, sample.width)

        by_src = (sample.group_by("strat_source").agg(pl.len().alias("n")).sort("n", descending=True))
        log.info("Topup composition by source:")
        for row in by_src.iter_rows(named=True):
            log.info("  %-15s  %d", row["strat_source"], row["n"])

        log.info("")
        log.info("Next: запусти deepinfra_runner на topup-файле:")
        log.info("  python scripts\\deepinfra_runner.py --model meta-llama/Llama-3.3-70B-Instruct --input %s "
                 "--output-dir sprint4\\reenrich\\data",
                 args.topup_output.relative_to(PROJECT_ROOT))
        return 0

    # --- ORIGINAL FULL SAMPLE MODE ---
    # Pre-sample distribution
    log.info("")
    log.info("=== Pre-sample strat_category distribution ===")
    cats = df.group_by("strat_category").agg(pl.len().alias("n")).sort("n", descending=True)
    for row in cats.iter_rows(named=True):
        log.info("  %-15s  %6d", row["strat_category"], row["n"])

    log.info("")
    log.info("=== Strat source breakdown ===")
    srcs = df.group_by("strat_source").agg(pl.len().alias("n")).sort("n", descending=True)
    for row in srcs.iter_rows(named=True):
        log.info("  %-15s  %6d  (%.1f%%)",
                 row["strat_source"], row["n"], 100.0 * row["n"] / df.height)

    # Sample
    log.info("")
    log.info("=== Sampling with targets ===")
    for c, t in sorted(TARGETS.items()):
        log.info("  %-15s  target=%d", c, t)
    log.info("  TOTAL target=%d", TARGET_TOTAL)

    sample, report = stratified_sample(df, TARGETS, args.seed)

    log.info("")
    log.info("=== Sample report ===")
    log.info("  %-15s  %6s  %6s  %6s  %6s",
             "category", "target", "avail", "sampled", "short")
    for r in report:
        flag = " ⚠" if r["shortfall"] > 0 else ""
        log.info(
            "  %-15s  %6d  %6d  %6d  %6d%s",
            r["category"], r["target"], r["available"], r["sampled"], r["shortfall"], flag,
        )
    log.info("  TOTAL sampled = %d (target %d)", sample.height, TARGET_TOTAL)

    # Write — drop вспомогательные strat_* columns? Оставлю — пригодятся для 4.7 анализа.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    sample.write_parquet(str(args.output))
    log.info("")
    log.info("written %s: %d rows × %d cols", args.output, sample.height, sample.width)

    # Sample composition
    log.info("")
    log.info("=== Sample composition ===")
    by_src = (sample.group_by("strat_source").agg(pl.len().alias("n"))
              .sort("n", descending=True))
    for row in by_src.iter_rows(named=True):
        pct = 100.0 * row["n"] / max(sample.height, 1)
        log.info("  strat_source=%-10s  %d (%.1f%%)", row["strat_source"], row["n"], pct)

    by_ch = (sample.group_by("channel").agg(pl.len().alias("n"))
             .sort("n", descending=True))
    log.info("")
    log.info("Channel distribution in sample:")
    for row in by_ch.iter_rows(named=True):
        pct = 100.0 * row["n"] / max(sample.height, 1)
        log.info("  %-15s  %5d  (%.1f%%)", row["channel"], row["n"], pct)

    by_anchor = sample.filter(pl.col("has_phase2_anchor")).height
    log.info("")
    log.info("Phase 2 anchors in sample: %d", by_anchor)

    return 0


if __name__ == "__main__":
    sys.exit(main())
