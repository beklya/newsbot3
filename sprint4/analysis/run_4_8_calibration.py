"""
sprint4/analysis/run_4_8_calibration.py — Sprint 4.8 per-bin LLM calibration.

Bins LLM enriched events on (model × category × ticker × horizon × confidence_bucket)
и для каждой ячейки считает realized hit-rate (direction matches sign(price_move)).

Output:
  - 4_8_calibration_table.parquet — wide bin table с N, hit_rate, CI95
  - 4_8_high_precision_bins.json — list of (cat, horizon, ticker, conf) с hit_rate≥0.6, N≥30
  - 4_8_worst_bins.json — bins с hit_rate≤0.4 (where LLM systematically wrong)
  - 4_8_calibration_report.xlsx — sheets best/worst/uplift

Использует PriceMovesLookup для realized returns через prices_cache.

Usage:
  python sprint4\\analysis\\run_4_8_calibration.py
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from math import sqrt
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))  # для price_moves_lookup
from price_moves_lookup import PriceMovesLookup  # noqa: E402

DEFAULT_8B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_llama_3_1_8b_instant_v1_0_0.parquet"
DEFAULT_70B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_subset_70b_v1_0_0.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sprint4" / "analysis" / "data"

# Horizons (минуты)
HORIZONS_MIN = [5, 10, 15, 30, 60, 90, 120, 180]

# Confidence buckets
CONFIDENCE_BUCKETS = [(0.0, 0.3), (0.3, 0.5), (0.5, 0.7), (0.7, 1.01)]

# High-precision threshold
PRECISION_THRESHOLD = 0.60
PRECISION_MIN_N = 10  # снижено с 30 — на текущем масштабе данных (~9k rows, 200 bins) N>=30 даёт 0 cells
WORST_THRESHOLD = 0.40

log = logging.getLogger("4_8_calibration")


def bucket_label(conf: float) -> str:
    for lo, hi in CONFIDENCE_BUCKETS:
        if lo <= conf < hi:
            return f"{lo:.1f}-{hi:.1f}"
    return "out_of_range"


def explode_enrichment(df: pl.DataFrame, model_label: str) -> pl.DataFrame:
    """Explode tickers list → one row per (event, ticker) with direction/conf/category."""
    sub = df.filter(pl.col("is_enriched") & pl.col("enrich_error").is_null())
    sub = sub.with_columns(pl.col("tickers").list.len().alias("_n_t"))
    sub = sub.filter(pl.col("_n_t") > 0)
    if sub.height == 0:
        return pl.DataFrame()

    sub = sub.explode("tickers")
    sub = sub.with_columns([
        pl.col("tickers").struct.field("ticker").alias("ticker"),
        pl.col("tickers").struct.field("direction").alias("direction"),
        pl.col("tickers").struct.field("confidence").cast(pl.Float64).alias("confidence"),
        pl.col("tickers").struct.field("impact_strength").cast(pl.Float64).alias("impact_strength"),
        pl.col("tickers").struct.field("sentiment").alias("sentiment"),
        pl.lit(model_label).alias("model"),
    ])
    return sub.select([
        "id", "datetime_msk", "channel", "model",
        "category", "urgency", "is_financial", "is_actionable", "expected_timeframe",
        "ticker", "direction", "confidence", "impact_strength", "sentiment",
    ])


def compute_realized(
    df: pl.DataFrame, pml: PriceMovesLookup, horizons: list[int],
) -> pl.DataFrame:
    """For each (event, ticker) row → add pm_<h> columns с realized pct_change."""
    rows = list(df.iter_rows(named=True))
    log.info("computing realized price_moves for %d (event, ticker) rows × %d horizons...",
             len(rows), len(horizons))
    t0 = time.perf_counter()

    realized_data: dict[int, list[float | None]] = {h: [] for h in horizons}
    for i, r in enumerate(rows):
        ticker = r["ticker"]
        ts_msk = r["datetime_msk"]
        if not isinstance(ts_msk, datetime):
            ts_msk = datetime.fromisoformat(str(ts_msk))
        deltas = pml.compute_multi(ticker, ts_msk, horizons)
        for h in horizons:
            realized_data[h].append(deltas.get(h))

        if (i + 1) % 10_000 == 0:
            log.info("  lookup %d/%d (%.0f rows/s)",
                     i + 1, len(rows), (i + 1) / max(time.perf_counter() - t0, 0.001))

    log.info("realized computed in %.1fs", time.perf_counter() - t0)

    # Build new dataframe with extra columns
    new_cols = {f"pm_{h}m": realized_data[h] for h in horizons}
    df_with_realized = df.with_columns([pl.Series(name, vals) for name, vals in new_cols.items()])
    return df_with_realized


def compute_bin_calibration(
    df: pl.DataFrame, horizons: list[int],
) -> pl.DataFrame:
    """Group by (model, category, conf_bucket, horizon) → hit_rate, N, CI95.

    Per-ticker dimension dropped — too granular (9k rows ÷ 7300 cells = 1.3/cell).
    Per-ticker analytics доступны через отдельный output (см. compute_per_ticker_calibration).
    """
    bins: list[dict] = []
    for h in horizons:
        col = f"pm_{h}m"
        sub = df.filter(
            pl.col(col).is_not_null()
            & pl.col("direction").is_not_null()
            & (pl.col("direction") != "neutral")
            & pl.col("category").is_not_null()
            & pl.col("ticker").is_not_null()
            & pl.col("confidence").is_not_null()
        )
        if sub.height == 0:
            continue
        # Compute sign matches
        sub = sub.with_columns([
            pl.when(pl.col("direction") == "long").then(1)
              .when(pl.col("direction") == "short").then(-1)
              .otherwise(0).alias("dir_sign"),
            pl.when(pl.col(col) > 0).then(1)
              .when(pl.col(col) < 0).then(-1)
              .otherwise(0).alias("price_sign"),
            pl.col("confidence").map_elements(bucket_label, return_dtype=pl.Utf8).alias("conf_bucket"),
        ])
        sub = sub.filter(pl.col("price_sign") != 0)  # 0% changes excluded
        sub = sub.with_columns((pl.col("dir_sign") == pl.col("price_sign")).cast(pl.Int32).alias("hit"))

        grouped = sub.group_by(["model", "category", "conf_bucket"]).agg([
            pl.len().alias("n"),
            pl.col("hit").sum().alias("n_hits"),
        ]).with_columns([
            pl.lit(h).alias("horizon_min"),
            (pl.col("n_hits") / pl.col("n")).alias("hit_rate"),
        ])

        # CI95 = 1.96 × sqrt(p(1-p)/n)
        grouped = grouped.with_columns(
            (1.96 * ((pl.col("hit_rate") * (1 - pl.col("hit_rate"))) / pl.col("n")).sqrt()).alias("ci95")
        )
        bins.extend(grouped.iter_rows(named=True))

    if not bins:
        return pl.DataFrame()
    return pl.DataFrame(bins).sort(["model", "category", "horizon_min", "conf_bucket"])


def compute_per_ticker_calibration(
    df: pl.DataFrame, horizons: list[int],
) -> pl.DataFrame:
    """Per-ticker × horizon, без category/conf — для диагностики SBER-like systematic biases.

    Скоро используется в 4.10 для per-ticker excluded list.
    """
    bins: list[dict] = []
    for h in horizons:
        col = f"pm_{h}m"
        sub = df.filter(
            pl.col(col).is_not_null()
            & pl.col("direction").is_not_null()
            & (pl.col("direction") != "neutral")
            & pl.col("ticker").is_not_null()
        )
        if sub.height == 0:
            continue
        sub = sub.with_columns([
            pl.when(pl.col("direction") == "long").then(1)
              .when(pl.col("direction") == "short").then(-1)
              .otherwise(0).alias("dir_sign"),
            pl.when(pl.col(col) > 0).then(1)
              .when(pl.col(col) < 0).then(-1)
              .otherwise(0).alias("price_sign"),
        ])
        sub = sub.filter(pl.col("price_sign") != 0)
        sub = sub.with_columns((pl.col("dir_sign") == pl.col("price_sign")).cast(pl.Int32).alias("hit"))

        grouped = sub.group_by(["model", "ticker"]).agg([
            pl.len().alias("n"),
            pl.col("hit").sum().alias("n_hits"),
        ]).with_columns([
            pl.lit(h).alias("horizon_min"),
            (pl.col("n_hits") / pl.col("n")).alias("hit_rate"),
        ])
        grouped = grouped.with_columns(
            (1.96 * ((pl.col("hit_rate") * (1 - pl.col("hit_rate"))) / pl.col("n")).sqrt()).alias("ci95")
        )
        bins.extend(grouped.iter_rows(named=True))
    if not bins:
        return pl.DataFrame()
    return pl.DataFrame(bins).sort(["model", "ticker", "horizon_min"])


def write_excel_report(
    path: Path, calibration: pl.DataFrame, high_prec: pl.DataFrame, worst: pl.DataFrame,
    uplift_by_conf: pl.DataFrame,
) -> None:
    import xlsxwriter
    wb = xlsxwriter.Workbook(str(path), {"nan_inf_to_errors": True})

    def _write(sheet_name: str, df: pl.DataFrame, note: str = ""):
        ws = wb.add_worksheet(sheet_name[:31])
        offset = 0
        if note:
            ws.write(0, 0, note)
            offset = 2
        for j, c in enumerate(df.columns):
            ws.write(offset, j, c)
        for i, row in enumerate(df.iter_rows()):
            for j, v in enumerate(row):
                if isinstance(v, (int, float, str, bool)):
                    ws.write(i + offset + 1, j, v)
                elif v is None:
                    ws.write(i + offset + 1, j, "")
                else:
                    ws.write(i + offset + 1, j, str(v))

    _write("best_bins", high_prec,
           f"High-precision bins: hit_rate≥{PRECISION_THRESHOLD}, N≥{PRECISION_MIN_N}")
    _write("worst_bins", worst,
           f"Worst bins: hit_rate≤{WORST_THRESHOLD}, N≥{PRECISION_MIN_N} (LLM systematically wrong)")
    _write("calibration_table", calibration, "Full per-bin calibration")
    _write("uplift_by_conf", uplift_by_conf, "Hit-rate vs confidence_bucket (per model × category)")

    wb.close()
    log.info("Excel report: %s", path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4.8 calibration")
    parser.add_argument("--c1-8b", type=Path, default=DEFAULT_8B)
    parser.add_argument("--c1-70b", type=Path, default=DEFAULT_70B)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    if not args.c1_8b.exists():
        log.error("missing 8b: %s", args.c1_8b); return 2
    df_8b = pl.read_parquet(str(args.c1_8b))
    log.info("8b: %d rows × %d cols", df_8b.height, df_8b.width)

    if args.c1_70b.exists():
        df_70b = pl.read_parquet(str(args.c1_70b))
        log.info("70b: %d rows × %d cols", df_70b.height, df_70b.width)
    else:
        df_70b = pl.DataFrame()
        log.warning("70b not found at %s — calibration только для 8b", args.c1_70b)

    # Explode tickers per model
    log.info("exploding 8b tickers...")
    df_8b_exp = explode_enrichment(df_8b, "8b")
    log.info("8b exploded: %d (event, ticker) rows", df_8b_exp.height)

    if df_70b.height > 0:
        log.info("exploding 70b tickers...")
        df_70b_exp = explode_enrichment(df_70b, "70b")
        log.info("70b exploded: %d (event, ticker) rows", df_70b_exp.height)
        combined = pl.concat([df_8b_exp, df_70b_exp])
    else:
        combined = df_8b_exp

    log.info("combined: %d rows", combined.height)

    # Warmup price cache
    log.info("warming up PricesCache (all 19 tickers, может занять 1-2 мин при first run)...")
    pml = PriceMovesLookup()
    pml.warmup()

    # Compute realized returns
    df_with_realized = compute_realized(combined, pml, HORIZONS_MIN)

    # Compute calibration bins (model × category × conf_bucket × horizon)
    log.info("computing calibration bins...")
    calibration = compute_bin_calibration(df_with_realized, HORIZONS_MIN)
    log.info("calibration: %d bins (avg N=%d)", calibration.height,
             int(calibration["n"].mean() or 0))

    # Per-ticker calibration (model × ticker × horizon) — для 4.10 excluded list
    log.info("computing per-ticker calibration...")
    per_ticker = compute_per_ticker_calibration(df_with_realized, HORIZONS_MIN)
    log.info("per-ticker: %d bins (avg N=%d)", per_ticker.height,
             int(per_ticker["n"].mean() or 0) if per_ticker.height else 0)

    # High-precision filter
    high_prec = calibration.filter(
        (pl.col("n") >= PRECISION_MIN_N) & (pl.col("hit_rate") >= PRECISION_THRESHOLD)
    ).sort("hit_rate", descending=True)
    worst = calibration.filter(
        (pl.col("n") >= PRECISION_MIN_N) & (pl.col("hit_rate") <= WORST_THRESHOLD)
    ).sort("hit_rate", descending=False)

    log.info("high-precision bins (hit≥%.2f, N≥%d): %d", PRECISION_THRESHOLD, PRECISION_MIN_N, high_prec.height)
    log.info("worst bins (hit≤%.2f, N≥%d): %d", WORST_THRESHOLD, PRECISION_MIN_N, worst.height)

    # Uplift by confidence (aggregate)
    uplift = (
        calibration
        .filter(pl.col("n") >= PRECISION_MIN_N)
        .group_by(["model", "category", "conf_bucket"])
        .agg([
            pl.col("n").sum().alias("total_n"),
            (pl.col("hit_rate") * pl.col("n")).sum().alias("_weighted_hits"),
        ])
        .with_columns((pl.col("_weighted_hits") / pl.col("total_n")).alias("weighted_hit_rate"))
        .drop("_weighted_hits")
        .sort(["model", "category", "conf_bucket"])
    )

    # Outputs
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cal_path = args.output_dir / "4_8_calibration_table.parquet"
    calibration.write_parquet(str(cal_path))
    log.info("written %s", cal_path)

    if per_ticker.height > 0:
        pt_path = args.output_dir / "4_8_per_ticker_calibration.parquet"
        per_ticker.write_parquet(str(pt_path))
        log.info("written %s", pt_path)

    high_path = args.output_dir / "4_8_high_precision_bins.json"
    high_path.write_text(
        json.dumps([r for r in high_prec.iter_rows(named=True)], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    worst_path = args.output_dir / "4_8_worst_bins.json"
    worst_path.write_text(
        json.dumps([r for r in worst.iter_rows(named=True)], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    log.info("written %s, %s", high_path, worst_path)

    excel_path = args.output_dir / "4_8_calibration_report.xlsx"
    try:
        write_excel_report(excel_path, calibration, high_prec, worst, uplift)
    except ImportError:
        log.warning("xlsxwriter not installed — Excel skipped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
