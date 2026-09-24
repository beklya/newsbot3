"""
sprint4/analysis/run_4_7_factorial.py — Sprint 4.7 factorial promt analysis.

3-way comparison: {8b × legacy_prompt × prices} vs {8b × new_prompt} vs {70b × new_prompt}.

Pipeline:
  1. Inner join legacy_full_analysis ⋈ c1_8b ⋈ c1_70b on event_id (common subset)
  2. Normalize legacy → comparable schema:
       - confidence: 0-100 -> 0-1
       - sentiment(bullish/bearish/neutral) -> direction(long/short/neutral)
       - ticker → "top_ticker" (legacy has 1, new picks top-1 by impact_strength)
       - n_tickers: from tickers_affected list
  3. Compute agreement matrices: 3x3 на category/direction/urgency
  4. Compute new-only metrics: is_financial, impact_strength, expected_timeframe, sell_the_news
  5. Compute predictive power: preview against realized 15m/60m sign (full в 4.8)
  6. Write parquet + Excel report

Usage:
  python sprint4\\analysis\\run_4_7_factorial.py
  python sprint4\\analysis\\run_4_7_factorial.py --price-moves path\\to\\news_pool_price_moves.parquet

Перед запуском:
  - sprint4/analysis/data/legacy_full_analysis.parquet (от extract_legacy_full_analysis.py)
  - sprint4/reenrich/data/c1_llama_3_1_8b_instant_v1_0_0.parquet (4.5 aggregate)
  - sprint4/reenrich/data/c1_subset_70b_v1_0_0.parquet (4.6 aggregate)
  - sprint4/analysis/data/news_pool_price_moves.parquet (опционально, для preview predictive power)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LEGACY = PROJECT_ROOT / "sprint4" / "analysis" / "data" / "legacy_full_analysis.parquet"
DEFAULT_8B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_llama_3_1_8b_instant_v1_0_0.parquet"
DEFAULT_70B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_subset_70b_v1_0_0.parquet"
DEFAULT_PM = PROJECT_ROOT / "sprint4" / "analysis" / "data" / "news_pool_price_moves.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sprint4" / "analysis" / "data"

# Valid new-prompt categories (от EnrichedNewsPayload schema)
VALID_NEW_CATS = {"cbr", "geopolitics", "macro", "corporate", "commodity", "currency", "market", "other"}

# Legacy sentiment -> direction (rough mapping, lossy на sell-the-news cases)
SENTIMENT_TO_DIRECTION = {
    "bullish": "long",
    "bearish": "short",
    "neutral": "neutral",
}

log = logging.getLogger("4_7_factorial")


# ----------------------------------------------------------------------------
# Normalization helpers
# ----------------------------------------------------------------------------
def normalize_legacy(legacy: pl.DataFrame) -> pl.DataFrame:
    """Project legacy enrichment to common-schema columns prefixed `legacy_norm_*`."""
    return legacy.with_columns([
        # confidence: 0-100 -> 0-1
        (pl.col("legacy_confidence").cast(pl.Float64) / 100.0).clip(0.0, 1.0).alias("legacy_norm_confidence"),
        # sentiment -> direction
        pl.col("legacy_sentiment").replace_strict(SENTIMENT_TO_DIRECTION, default=None).alias("legacy_norm_direction"),
        # n_tickers: ticker + tickers_affected
        (
            pl.col("legacy_ticker").is_not_null().cast(pl.Int32)
            + pl.col("legacy_tickers_affected").list.len().cast(pl.Int32)
        ).alias("legacy_norm_n_tickers"),
        # is_financial proxy: ticker != null
        pl.col("legacy_ticker").is_not_null().alias("legacy_norm_is_financial"),
        # category: rename для consistency
        pl.col("legacy_category").alias("legacy_norm_category"),
        pl.col("legacy_urgency").alias("legacy_norm_urgency"),
        pl.col("legacy_ticker").alias("legacy_norm_top_ticker"),
    ])


def extract_new_top_ticker(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Extract top-1 ticker by impact_strength from new prompt's tickers list."""
    # `tickers` is List(Struct({ticker, direction, sentiment, confidence, impact_strength, rationale}))
    df = df.with_columns([
        pl.col("tickers").list.len().alias(f"{prefix}_n_tickers"),
        # First ticker if any
        pl.when(pl.col("tickers").list.len() > 0)
          .then(pl.col("tickers").list.first().struct.field("ticker"))
          .otherwise(None)
          .alias(f"{prefix}_top_ticker"),
        # First ticker's direction
        pl.when(pl.col("tickers").list.len() > 0)
          .then(pl.col("tickers").list.first().struct.field("direction"))
          .otherwise(None)
          .alias(f"{prefix}_top_direction"),
        # First ticker's confidence
        pl.when(pl.col("tickers").list.len() > 0)
          .then(pl.col("tickers").list.first().struct.field("confidence"))
          .otherwise(None)
          .alias(f"{prefix}_top_confidence"),
        # First ticker's impact_strength
        pl.when(pl.col("tickers").list.len() > 0)
          .then(pl.col("tickers").list.first().struct.field("impact_strength"))
          .otherwise(None)
          .alias(f"{prefix}_top_impact_strength"),
        # First ticker's sentiment
        pl.when(pl.col("tickers").list.len() > 0)
          .then(pl.col("tickers").list.first().struct.field("sentiment"))
          .otherwise(None)
          .alias(f"{prefix}_top_sentiment"),
    ])
    # Sell-the-news flag: sentiment positive while direction short OR vice versa
    df = df.with_columns(
        (
            ((pl.col(f"{prefix}_top_sentiment") == "positive") & (pl.col(f"{prefix}_top_direction") == "short"))
            | ((pl.col(f"{prefix}_top_sentiment") == "negative") & (pl.col(f"{prefix}_top_direction") == "long"))
        ).alias(f"{prefix}_sell_the_news_flag")
    )
    return df


def normalize_new(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """Add top_ticker and rename fields with prefix."""
    df = extract_new_top_ticker(df, prefix)
    df = df.rename({
        "category": f"{prefix}_category",
        "urgency": f"{prefix}_urgency",
        "is_financial": f"{prefix}_is_financial",
        "is_actionable": f"{prefix}_is_actionable",
        "expected_timeframe": f"{prefix}_expected_timeframe",
        "summary": f"{prefix}_summary",
        "enrich_error": f"{prefix}_error",
        "enrich_total_tokens": f"{prefix}_total_tokens",
    })
    # Clean category — replace hallucinated values with "other"
    df = df.with_columns(
        pl.when(pl.col(f"{prefix}_category").is_in(list(VALID_NEW_CATS)))
          .then(pl.col(f"{prefix}_category"))
          .otherwise(pl.lit("other"))
          .alias(f"{prefix}_category"),
    )
    return df


# ----------------------------------------------------------------------------
# Agreement metrics
# ----------------------------------------------------------------------------
def agreement_matrix(df: pl.DataFrame, col_a: str, col_b: str) -> pl.DataFrame:
    """Build N×N contingency table for two categorical columns."""
    return (
        df.filter(pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null())
          .group_by([col_a, col_b])
          .agg(pl.len().alias("n"))
          .sort([col_a, col_b])
    )


def agreement_rate(df: pl.DataFrame, col_a: str, col_b: str) -> tuple[int, int, float]:
    """Returns (n_agree, n_total, agreement_pct)."""
    sub = df.filter(pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null())
    total = sub.height
    if total == 0:
        return 0, 0, 0.0
    agree = sub.filter(pl.col(col_a) == pl.col(col_b)).height
    return agree, total, 100.0 * agree / total


# ----------------------------------------------------------------------------
# Predictive power preview (against realized 15m/60m sign)
# ----------------------------------------------------------------------------
def predictive_power_preview(
    df: pl.DataFrame, prefix: str, price_moves: pl.DataFrame, horizons: list[str],
) -> pl.DataFrame:
    """Compute hit-rate: (direction == sign(price_move)) для top_ticker at given horizon."""
    rows = []
    df_with_pm = df.join(price_moves, on="id", how="inner")
    log.info("  %s: %d events with price_moves", prefix, df_with_pm.height)

    for horizon in horizons:
        # Build per-row pm value for top_ticker × horizon dynamically
        # Direct expression: we need to lookup column `pm_{top_ticker}_{horizon}` per row
        # Polars не позволяет dynamic column names напрямую — делаем через pivot или iter_rows
        sub = df_with_pm.filter(
            pl.col(f"{prefix}_top_ticker").is_not_null()
            & pl.col(f"{prefix}_top_direction").is_not_null()
            & (pl.col(f"{prefix}_top_direction") != "neutral")
        )
        if sub.height == 0:
            rows.append({"model": prefix, "horizon": horizon, "n": 0, "n_agree": 0, "hit_rate": 0.0})
            continue

        # Iterate (cost OK на ~1500 rows)
        n_total = 0
        n_agree = 0
        for r in sub.iter_rows(named=True):
            ticker = r[f"{prefix}_top_ticker"]
            direction = r[f"{prefix}_top_direction"]
            col_name = f"pm_{ticker}_{horizon}"
            if col_name not in price_moves.columns:
                continue
            pm_val = r.get(col_name)
            if pm_val is None:
                continue
            sign = 1 if pm_val > 0 else (-1 if pm_val < 0 else 0)
            pred = 1 if direction == "long" else (-1 if direction == "short" else 0)
            if sign == 0 or pred == 0:
                continue
            n_total += 1
            if sign == pred:
                n_agree += 1

        hit = 100.0 * n_agree / max(n_total, 1)
        rows.append({"model": prefix, "horizon": horizon, "n": n_total, "n_agree": n_agree, "hit_rate": hit})

    return pl.DataFrame(rows)


# ----------------------------------------------------------------------------
# Excel report
# ----------------------------------------------------------------------------
def write_excel_report(
    output_path: Path,
    joined: pl.DataFrame,
    agreement_cat: dict,
    agreement_dir: dict,
    agreement_urg: dict,
    legacy_category_dist: pl.DataFrame,
    new_8b_category_dist: pl.DataFrame,
    new_70b_category_dist: pl.DataFrame,
    sell_the_news: pl.DataFrame,
    new_only_uplift: pl.DataFrame,
    predictive: pl.DataFrame | None,
) -> None:
    import xlsxwriter

    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook = xlsxwriter.Workbook(str(output_path), {"nan_inf_to_errors": True})

    def _write_df(sheet_name: str, df: pl.DataFrame, header_note: str = ""):
        ws = workbook.add_worksheet(sheet_name[:31])
        cols = df.columns
        if header_note:
            ws.write(0, 0, header_note)
            row_offset = 2
        else:
            row_offset = 0
        for j, c in enumerate(cols):
            ws.write(row_offset, j, c)
        for i, row in enumerate(df.iter_rows()):
            for j, v in enumerate(row):
                if isinstance(v, (int, float, str, bool)):
                    ws.write(i + row_offset + 1, j, v)
                elif v is None:
                    ws.write(i + row_offset + 1, j, "")
                else:
                    ws.write(i + row_offset + 1, j, str(v))

    # Summary sheet
    summary = pl.DataFrame([
        {"metric": "total joined events (legacy ∩ 8b ∩ 70b)", "value": joined.height},
        {"metric": "agreement legacy↔8b on category (%)", "value": round(agreement_cat["legacy_8b"][2], 2)},
        {"metric": "agreement 8b↔70b on category (%)", "value": round(agreement_cat["8b_70b"][2], 2)},
        {"metric": "agreement legacy↔70b on category (%)", "value": round(agreement_cat["legacy_70b"][2], 2)},
        {"metric": "agreement legacy↔8b on direction (%)", "value": round(agreement_dir["legacy_8b"][2], 2)},
        {"metric": "agreement 8b↔70b on direction (%)", "value": round(agreement_dir["8b_70b"][2], 2)},
        {"metric": "agreement legacy↔8b on urgency (%)", "value": round(agreement_urg["legacy_8b"][2], 2)},
        {"metric": "agreement 8b↔70b on urgency (%)", "value": round(agreement_urg["8b_70b"][2], 2)},
    ])
    _write_df("summary", summary, "Headline agreement on common-schema columns")

    # Category distributions
    _write_df("category_drift", legacy_category_dist, "Legacy category distribution")
    _write_df("8b_category", new_8b_category_dist, "8b (new prompt) category distribution")
    _write_df("70b_category", new_70b_category_dist, "70b (new prompt) category distribution")

    # Agreement matrices
    for key, df in [("agree_cat_legacy_8b", agreement_cat["legacy_8b_matrix"]),
                    ("agree_cat_8b_70b", agreement_cat["8b_70b_matrix"]),
                    ("agree_dir_legacy_8b", agreement_dir["legacy_8b_matrix"]),
                    ("agree_dir_8b_70b", agreement_dir["8b_70b_matrix"])]:
        _write_df(key, df)

    # Sell-the-news
    _write_df("sell_the_news", sell_the_news, "Sell-the-news flag distribution (new-only metric)")

    # New-only uplift (8b vs 70b)
    _write_df("new_only_8b_vs_70b", new_only_uplift,
              "8b vs 70b mean of new-only metrics (impact_strength, is_actionable)")

    # Predictive power
    if predictive is not None:
        _write_df("predictive_preview", predictive,
                  "Direction vs sign(price_move) hit-rate per horizon — preview for 4.8")

    workbook.close()
    log.info("Excel report written: %s", output_path)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4.7 factorial promt analysis")
    parser.add_argument("--legacy", type=Path, default=DEFAULT_LEGACY)
    parser.add_argument("--c1-8b", type=Path, default=DEFAULT_8B)
    parser.add_argument("--c1-70b", type=Path, default=DEFAULT_70B)
    parser.add_argument("--price-moves", type=Path, default=DEFAULT_PM,
                        help="(опционально) для predictive power preview")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    # 1. Load all 3 sources
    for path, name in [(args.legacy, "legacy"), (args.c1_8b, "8b"), (args.c1_70b, "70b")]:
        if not path.exists():
            log.error("missing %s: %s", name, path)
            return 2

    legacy = pl.read_parquet(str(args.legacy))
    log.info("legacy: %d rows × %d cols", legacy.height, legacy.width)

    df_8b = pl.read_parquet(str(args.c1_8b))
    df_70b = pl.read_parquet(str(args.c1_70b))
    log.info("8b: %d rows × %d cols", df_8b.height, df_8b.width)
    log.info("70b: %d rows × %d cols", df_70b.height, df_70b.width)

    # 2. Normalize each
    legacy_norm = normalize_legacy(legacy)
    df_8b_norm = normalize_new(df_8b, prefix="m8b")
    df_70b_norm = normalize_new(df_70b, prefix="m70b")

    # 3. Inner join — keep only events present in all 3
    # First: filter 8b/70b to enriched + no errors
    df_8b_clean = df_8b_norm.filter(pl.col("is_enriched") & pl.col("m8b_error").is_null())
    df_70b_clean = df_70b_norm.filter(pl.col("is_enriched") & pl.col("m70b_error").is_null())
    legacy_clean = legacy_norm.filter(pl.col("legacy_norm_category").is_not_null())

    log.info("after clean: legacy=%d, 8b=%d, 70b=%d",
             legacy_clean.height, df_8b_clean.height, df_70b_clean.height)

    # Join 8b ⋈ 70b first (subset where both exist)
    joined = (
        df_8b_clean.select([
            "id",
            "m8b_category", "m8b_urgency", "m8b_is_financial", "m8b_is_actionable",
            "m8b_expected_timeframe", "m8b_top_ticker", "m8b_top_direction",
            "m8b_top_confidence", "m8b_top_impact_strength", "m8b_top_sentiment",
            "m8b_sell_the_news_flag", "m8b_n_tickers", "m8b_summary",
        ])
        .join(
            df_70b_clean.select([
                "id",
                "m70b_category", "m70b_urgency", "m70b_is_financial", "m70b_is_actionable",
                "m70b_expected_timeframe", "m70b_top_ticker", "m70b_top_direction",
                "m70b_top_confidence", "m70b_top_impact_strength", "m70b_top_sentiment",
                "m70b_sell_the_news_flag", "m70b_n_tickers", "m70b_summary",
            ]),
            on="id", how="inner",
        )
        .join(
            legacy_clean.select([
                "id", "legacy_norm_category", "legacy_norm_direction", "legacy_norm_confidence",
                "legacy_norm_urgency", "legacy_norm_is_financial", "legacy_norm_top_ticker",
                "legacy_norm_n_tickers", "legacy_reason", "legacy_price_driven", "legacy_causal",
            ]),
            on="id", how="inner",
        )
    )
    log.info("3-way joined: %d rows × %d cols", joined.height, joined.width)

    if joined.height == 0:
        log.error("empty join — нет events с enrichment во всех 3 источниках. Check input файлы.")
        return 3

    # 4. Compute agreements (only on common-schema cols)
    log.info("")
    log.info("=== Agreement matrices ===")

    agreement_cat = {}
    for name, a, b in [("legacy_8b", "legacy_norm_category", "m8b_category"),
                        ("8b_70b", "m8b_category", "m70b_category"),
                        ("legacy_70b", "legacy_norm_category", "m70b_category")]:
        ag, tot, pct = agreement_rate(joined, a, b)
        agreement_cat[name] = (ag, tot, pct)
        agreement_cat[f"{name}_matrix"] = agreement_matrix(joined, a, b)
        log.info("category %s: %d/%d (%.1f%%)", name, ag, tot, pct)

    agreement_dir = {}
    for name, a, b in [("legacy_8b", "legacy_norm_direction", "m8b_top_direction"),
                        ("8b_70b", "m8b_top_direction", "m70b_top_direction")]:
        ag, tot, pct = agreement_rate(joined, a, b)
        agreement_dir[name] = (ag, tot, pct)
        agreement_dir[f"{name}_matrix"] = agreement_matrix(joined, a, b)
        log.info("direction %s: %d/%d (%.1f%%)", name, ag, tot, pct)

    agreement_urg = {}
    for name, a, b in [("legacy_8b", "legacy_norm_urgency", "m8b_urgency"),
                        ("8b_70b", "m8b_urgency", "m70b_urgency")]:
        ag, tot, pct = agreement_rate(joined, a, b)
        agreement_urg[name] = (ag, tot, pct)
        log.info("urgency %s: %d/%d (%.1f%%)", name, ag, tot, pct)

    # 5. Category distributions
    legacy_cat_dist = joined.group_by("legacy_norm_category").agg(pl.len().alias("n")).sort("n", descending=True)
    new_8b_cat_dist = joined.group_by("m8b_category").agg(pl.len().alias("n")).sort("n", descending=True)
    new_70b_cat_dist = joined.group_by("m70b_category").agg(pl.len().alias("n")).sort("n", descending=True)

    # 6. Sell-the-news (new-only)
    sell_8b = int(joined["m8b_sell_the_news_flag"].sum())
    sell_70b = int(joined["m70b_sell_the_news_flag"].sum())
    log.info("")
    log.info("Sell-the-news: 8b=%d, 70b=%d (out of %d)", sell_8b, sell_70b, joined.height)
    sell_the_news = pl.DataFrame([
        {"model": "8b", "sell_the_news_count": sell_8b, "total": joined.height,
         "rate_pct": round(100.0 * sell_8b / max(joined.height, 1), 2)},
        {"model": "70b", "sell_the_news_count": sell_70b, "total": joined.height,
         "rate_pct": round(100.0 * sell_70b / max(joined.height, 1), 2)},
    ])

    # 7. New-only uplift (impact_strength, is_actionable rates)
    new_only_rows = []
    for prefix in ["m8b", "m70b"]:
        sub = joined.filter(pl.col(f"{prefix}_top_impact_strength").is_not_null())
        mean_impact = float(sub[f"{prefix}_top_impact_strength"].mean() or 0)
        actionable_rate = float(joined[f"{prefix}_is_actionable"].cast(pl.Int32).mean() or 0)
        fin_rate = float(joined[f"{prefix}_is_financial"].cast(pl.Int32).mean() or 0)
        new_only_rows.append({
            "model": prefix,
            "mean_impact_strength": round(mean_impact, 4),
            "is_actionable_rate": round(actionable_rate, 4),
            "is_financial_rate": round(fin_rate, 4),
        })
    new_only_uplift = pl.DataFrame(new_only_rows)

    # 8. Predictive power preview (опционально)
    predictive = None
    if args.price_moves.exists():
        log.info("")
        log.info("=== Predictive power preview ===")
        pm = pl.read_parquet(str(args.price_moves))
        horizons_preview = ["15m", "60m"]
        rows = []
        for prefix in ["m8b", "m70b"]:
            sub = predictive_power_preview(joined, prefix, pm, horizons_preview)
            rows.extend(sub.iter_rows(named=True))
        predictive = pl.DataFrame(rows)
        for r in predictive.iter_rows(named=True):
            log.info("  %s @ %s: %d/%d = %.1f%% hit-rate", r["model"], r["horizon"], r["n_agree"], r["n"], r["hit_rate"])
    else:
        log.info("price_moves not found at %s — skipping predictive preview", args.price_moves)

    # 9. Write outputs
    args.output_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = args.output_dir / "4_7_factorial_metrics.parquet"
    joined.write_parquet(str(parquet_path))
    log.info("")
    log.info("written %s: %d rows × %d cols", parquet_path, joined.height, joined.width)

    excel_path = args.output_dir / "4_7_factorial_report.xlsx"
    try:
        write_excel_report(
            excel_path, joined,
            agreement_cat, agreement_dir, agreement_urg,
            legacy_cat_dist, new_8b_cat_dist, new_70b_cat_dist,
            sell_the_news, new_only_uplift, predictive,
        )
    except ImportError:
        log.warning("xlsxwriter not installed — Excel report skipped. pip install xlsxwriter")

    return 0


if __name__ == "__main__":
    sys.exit(main())
