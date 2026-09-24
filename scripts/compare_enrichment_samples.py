r"""
scripts/compare_enrichment_samples.py
======================================
Distribution diff utility — сравнивает два enrichment parquet'а side-by-side.

Use case: после Stage 1 small-sample test с новым prompt'ом сравнить
distribution с legacy reference (features_mfe.parquet).

Inputs:
  --reference  : reference enrichment parquet (e.g. legacy features_mfe.parquet)
  --candidate  : candidate enrichment parquet (e.g. new prompt v2.1.0 small sample)
  --output     : optional output file для report (default: stdout only)

Output:
  Side-by-side distribution для cat_*, sent_*, urg_*, confidence, n_tickers.
  χ² drift test для categorical columns.

Acceptance criteria (Stage 1 plan, Sprint 5.7):
  cat_other ≤ 50% (vs current 60%)
  sent_neutral ≤ 40% (vs current 64%, legacy ~33%)
  confidence mean ≥ 0.60 (vs current 0.51, legacy 0.66)
  cat_sanctions/earnings/dividends/ma nonzero (≥1% each — current = 0%)

Schema requirements:
  Both parquet must have either:
    - One-hot columns (cat_geopolitics, sent_bullish, urg_high, confidence float, ...)
      — works for features_mfe.parquet style
    OR
    - "Wide" enrichment columns (category string, sentiment string, urgency string,
      confidence float, tickers list/null)
      — works for aggregate_checkpoint.py output (e.g. full_70k_70b.parquet)

Script auto-detects schema. For mixed comparison (one-hot vs wide), columns are
normalized to a common form before diff.

Usage:
  python scripts/compare_enrichment_samples.py \\
      --reference D:\\quik_sber\\newsbot\\newsbot2\\решение проблем\\Проблема 5 - новое начало\\phase2_mfe\\features_mfe.parquet \\
      --candidate data\\reenrich_phase2\\full_70k_70b.parquet

  # с фильтром по date window
  python scripts/compare_enrichment_samples.py --reference ... --candidate ... \\
      --date-from 2025-01-01 --date-to 2026-05-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("compare_enrichment")

# Mapping для wide schema → одинаковая семантика
LEGACY_CAT_COLS = [
    "cat_geopolitics", "cat_macro", "cat_cbr", "cat_corporate",
    "cat_commodity", "cat_currency", "cat_sanctions", "cat_earnings",
    "cat_dividends", "cat_ma", "cat_regulation", "cat_other",
]
LEGACY_SENT_COLS = ["sent_bullish", "sent_bearish", "sent_neutral"]
LEGACY_URG_COLS = ["urg_high", "urg_medium", "urg_low"]


def detect_schema(df: pd.DataFrame) -> str:
    """one-hot (features_mfe style) или wide (aggregate_checkpoint style)?"""
    if "cat_geopolitics" in df.columns and "sent_bullish" in df.columns:
        return "one_hot"
    if "category" in df.columns and "tickers" in df.columns:
        return "wide"
    raise ValueError(
        f"Unknown schema. Columns sample: {list(df.columns)[:20]}"
    )


def normalize_to_buckets(df: pd.DataFrame, schema: str) -> dict:
    """Конвертирует df в один формат: dict label → series of values."""
    if schema == "one_hot":
        cat_series = df[LEGACY_CAT_COLS].idxmax(axis=1).str.replace("cat_", "", regex=False)
        sent_series = df[LEGACY_SENT_COLS].idxmax(axis=1).str.replace("sent_", "", regex=False)
        urg_series = df[LEGACY_URG_COLS].idxmax(axis=1).str.replace("urg_", "", regex=False)
        conf_series = df["confidence"] if "confidence" in df.columns else pd.Series(dtype=float)
        # Confidence in legacy features_mfe is 0-1 (scaled) — verify or rescale
        if conf_series.max() > 1.5:
            log.info("  reference confidence appears 0-100 scale, normalizing to 0-1")
            conf_series = conf_series / 100.0
        n_tickers = df["n_tickers_aff"] if "n_tickers_aff" in df.columns else pd.Series(dtype=float)
        is_fin = pd.Series(dtype=bool)
    elif schema == "wide":
        cat_series = df["category"].fillna("other")
        # Sentiment from per-ticker structure: use top-impact ticker
        if "tickers" in df.columns:
            sent_vals = []
            for tickers in df["tickers"]:
                if not isinstance(tickers, (list, np.ndarray)):
                    sent_vals.append("neutral")
                    continue
                if len(tickers) == 0:
                    sent_vals.append("neutral")
                    continue
                top = max(tickers, key=lambda t: (t.get("impact_strength") or 0) if isinstance(t, dict) else 0)
                s = top.get("sentiment") if isinstance(top, dict) else None
                # map positive/negative/neutral → bullish/bearish/neutral
                mapping = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
                sent_vals.append(mapping.get(s, "neutral"))
            sent_series = pd.Series(sent_vals, index=df.index)
        else:
            sent_series = pd.Series(["neutral"] * len(df), index=df.index)
        urg_series = df["urgency"].fillna("low")
        # Confidence from top-impact ticker
        conf_vals = []
        for tickers in df["tickers"]:
            if not isinstance(tickers, (list, np.ndarray)) or len(tickers) == 0:
                conf_vals.append(0.5)
                continue
            top = max(tickers, key=lambda t: (t.get("impact_strength") or 0) if isinstance(t, dict) else 0)
            c = top.get("confidence") if isinstance(top, dict) else 0.5
            try:
                conf_vals.append(float(c) if c is not None else 0.5)
            except (ValueError, TypeError):
                conf_vals.append(0.5)
        conf_series = pd.Series(conf_vals, index=df.index)
        n_tickers = pd.Series([
            len(t) if isinstance(t, (list, np.ndarray)) else 0
            for t in df["tickers"]
        ], index=df.index)
        is_fin = df["is_financial"] if "is_financial" in df.columns else pd.Series(dtype=bool)
    else:
        raise ValueError(f"Unknown schema: {schema}")

    return {
        "category": cat_series,
        "sentiment": sent_series,
        "urgency": urg_series,
        "confidence": conf_series,
        "n_tickers": n_tickers,
        "is_financial": is_fin,
    }


def print_categorical_dist(label: str, ref: pd.Series, cand: pd.Series, lines: list):
    """Side-by-side counts + percentages + delta."""
    # Convert None to string for consistent sorting
    ref_clean = ref.fillna("<null>").astype(str)
    cand_clean = cand.fillna("<null>").astype(str)
    ref_counts = ref_clean.value_counts()
    cand_counts = cand_clean.value_counts()
    all_keys = sorted(set(ref_counts.index) | set(cand_counts.index))
    n_ref = len(ref)
    n_cand = len(cand)

    lines.append(f"\n{'=' * 60}")
    lines.append(f"{label}")
    lines.append(f"{'=' * 60}")
    lines.append(f"  {'key':<15}  {'ref':>8} {'ref%':>7}  {'cand':>8} {'cand%':>7}  {'Δ%':>7}")
    for k in all_keys:
        r = ref_counts.get(k, 0)
        c = cand_counts.get(k, 0)
        rp = 100 * r / max(n_ref, 1)
        cp = 100 * c / max(n_cand, 1)
        d = cp - rp
        marker = ""
        if abs(d) >= 10:
            marker = " ⚠"
        elif abs(d) >= 5:
            marker = " ~"
        lines.append(f"  {str(k):<15}  {r:>8} {rp:>6.1f}%  {c:>8} {cp:>6.1f}%  {d:+6.1f}%{marker}")

    # Chi-squared test (categorical drift)
    try:
        from scipy.stats import chi2_contingency
        common = list(set(ref_counts.index) & set(cand_counts.index))
        if len(common) >= 2:
            contingency = np.array([
                [ref_counts.get(k, 0) for k in common],
                [cand_counts.get(k, 0) for k in common],
            ])
            chi2, pval, _, _ = chi2_contingency(contingency)
            sig = "***" if pval < 0.001 else "**" if pval < 0.01 else "*" if pval < 0.05 else "ns"
            lines.append(f"  χ²={chi2:.1f}  p={pval:.3g} ({sig})")
    except Exception as e:
        lines.append(f"  (chi² test failed: {e})")


def print_numeric_stats(label: str, ref: pd.Series, cand: pd.Series, lines: list):
    """Mean, std, percentiles."""
    lines.append(f"\n{'=' * 60}")
    lines.append(f"{label}")
    lines.append(f"{'=' * 60}")
    ref_arr = pd.to_numeric(ref, errors="coerce").dropna()
    cand_arr = pd.to_numeric(cand, errors="coerce").dropna()
    if len(ref_arr) == 0 or len(cand_arr) == 0:
        lines.append("  (no data)")
        return
    lines.append(f"  {'stat':<15}  {'ref':>10}  {'cand':>10}  {'Δ':>10}")
    for stat_name, fn in [
        ("mean", np.mean),
        ("std", np.std),
        ("min", np.min),
        ("p10", lambda x: np.percentile(x, 10)),
        ("p50 (median)", np.median),
        ("p90", lambda x: np.percentile(x, 90)),
        ("max", np.max),
    ]:
        r = fn(ref_arr)
        c = fn(cand_arr)
        d = c - r
        lines.append(f"  {stat_name:<15}  {r:>10.3f}  {c:>10.3f}  {d:>+10.3f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", required=True, help="Reference enrichment parquet")
    ap.add_argument("--candidate", required=True, help="Candidate enrichment parquet")
    ap.add_argument("--output", help="Optional output file (default: stdout)")
    ap.add_argument("--date-from", help="Filter to events with datetime ≥ YYYY-MM-DD")
    ap.add_argument("--date-to", help="Filter to events with datetime < YYYY-MM-DD")
    args = ap.parse_args()

    log.info("Loading reference: %s", args.reference)
    ref_df = pd.read_parquet(args.reference)
    log.info("  ref: %d rows × %d cols", len(ref_df), len(ref_df.columns))
    log.info("Loading candidate: %s", args.candidate)
    cand_df = pd.read_parquet(args.candidate)
    log.info("  cand: %d rows × %d cols", len(cand_df), len(cand_df.columns))

    # Optional date filter
    if args.date_from or args.date_to:
        for name, df in (("ref", ref_df), ("cand", cand_df)):
            dt_col = None
            for cand_col in ("_datetime", "datetime_msk", "datetime", "enrich_at"):
                if cand_col in df.columns:
                    dt_col = cand_col
                    break
            if dt_col:
                dt = pd.to_datetime(df[dt_col], errors="coerce")
                mask = pd.Series(True, index=df.index)
                if args.date_from:
                    mask &= dt >= pd.Timestamp(args.date_from)
                if args.date_to:
                    mask &= dt < pd.Timestamp(args.date_to)
                if name == "ref":
                    ref_df = ref_df[mask].copy()
                else:
                    cand_df = cand_df[mask].copy()
                log.info("  %s after date filter (%s): %d rows", name, dt_col, len(globals()[name + "_df"]))

    ref_schema = detect_schema(ref_df)
    cand_schema = detect_schema(cand_df)
    log.info("Schemas: ref=%s  cand=%s", ref_schema, cand_schema)

    ref_buckets = normalize_to_buckets(ref_df, ref_schema)
    cand_buckets = normalize_to_buckets(cand_df, cand_schema)

    lines = []
    lines.append("=" * 60)
    lines.append("ENRICHMENT DISTRIBUTION COMPARISON")
    lines.append("=" * 60)
    lines.append(f"Reference: {args.reference}")
    lines.append(f"  n_rows: {len(ref_df)}")
    lines.append(f"Candidate: {args.candidate}")
    lines.append(f"  n_rows: {len(cand_df)}")

    print_categorical_dist("CATEGORY", ref_buckets["category"], cand_buckets["category"], lines)
    print_categorical_dist("SENTIMENT", ref_buckets["sentiment"], cand_buckets["sentiment"], lines)
    print_categorical_dist("URGENCY", ref_buckets["urgency"], cand_buckets["urgency"], lines)
    print_numeric_stats("CONFIDENCE", ref_buckets["confidence"], cand_buckets["confidence"], lines)
    print_numeric_stats("N_TICKERS", ref_buckets["n_tickers"], cand_buckets["n_tickers"], lines)
    if len(cand_buckets["is_financial"]) > 0:
        print_categorical_dist("IS_FINANCIAL", ref_buckets["is_financial"], cand_buckets["is_financial"], lines)

    # Sprint 5.7 acceptance gate (REVISED based on actual legacy distribution discovered)
    lines.append("\n" + "=" * 60)
    lines.append("Sprint 5.7 ACCEPTANCE GATE (Stage 1) — Phase 2 distribution match")
    lines.append("=" * 60)
    lines.append("Legacy reference (features_mfe.parquet 2022-2026):")
    lines.append("  cat_geopolitics 48% | cat_other 39% | sent_bearish 41% | sent_bullish 6%")
    lines.append("  sent_neutral 54% | confidence mean 0.66 | n_tickers mean 1.16")
    lines.append("Targets (match within ±10% absolute):")
    cat_series = cand_buckets["category"]
    sent_series = cand_buckets["sentiment"]
    conf_series = pd.to_numeric(cand_buckets["confidence"], errors="coerce").dropna()
    n_tickers_series = pd.to_numeric(cand_buckets["n_tickers"], errors="coerce").dropna()

    cat_geopolitics_pct = 100 * (cat_series == "geopolitics").mean()
    cat_other_pct = 100 * (cat_series == "other").mean()
    sent_bullish_pct = 100 * (sent_series == "bullish").mean()
    sent_bearish_pct = 100 * (sent_series == "bearish").mean()
    sent_neutral_pct = 100 * (sent_series == "neutral").mean()
    conf_mean = conf_series.mean() if len(conf_series) else float("nan")
    n_tickers_mean = n_tickers_series.mean() if len(n_tickers_series) else float("nan")
    directional_pct = sent_bullish_pct + sent_bearish_pct  # non-neutral classifications

    pass_geo = cat_geopolitics_pct >= 35  # ≥35% (vs legacy 48.5)
    pass_other = cat_other_pct <= 50  # ≤50% (vs legacy 39%)
    pass_directional = directional_pct >= 40  # ≥40% non-neutral (vs legacy 46%)
    pass_conf = conf_mean >= 0.60  # ≥0.60 (vs legacy 0.66)
    pass_n_tickers = n_tickers_mean >= 1.0  # ≥1.0 (vs legacy 1.16)

    lines.append(f"  cat_geopolitics ≥ 35% :  {cat_geopolitics_pct:5.1f}%  {'✓' if pass_geo else '✗'}")
    lines.append(f"  cat_other ≤ 50%       :  {cat_other_pct:5.1f}%  {'✓' if pass_other else '✗'}")
    lines.append(f"  directional (bear+bull) ≥ 40%: {directional_pct:5.1f}%  {'✓' if pass_directional else '✗'}")
    lines.append(f"  confidence mean ≥ 0.60:  {conf_mean:5.3f}  {'✓' if pass_conf else '✗'}")
    lines.append(f"  n_tickers mean ≥ 1.0  :  {n_tickers_mean:5.2f}  {'✓' if pass_n_tickers else '✗'}")
    lines.append(f"")
    lines.append(f"  (FYI legacy bearish-dominance for 2022-2026 era: legacy bearish=41%, bullish=6%)")
    lines.append(f"   current bearish={sent_bearish_pct:.1f}% bullish={sent_bullish_pct:.1f}% neutral={sent_neutral_pct:.1f}%")

    overall = pass_geo and pass_other and pass_directional and pass_conf and pass_n_tickers
    lines.append(f"\n  OVERALL: {'✓ PASS' if overall else '✗ FAIL'}")

    output = "\n".join(lines)
    print(output)
    if args.output:
        Path(args.output).write_text(output, encoding="utf-8")
        log.info("Saved report: %s", args.output)


if __name__ == "__main__":
    main()
