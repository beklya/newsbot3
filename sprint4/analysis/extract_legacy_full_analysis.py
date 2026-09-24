"""
sprint4/analysis/extract_legacy_full_analysis.py — one-time pre-extract от news_pool.jsonl

Извлекает ПОЛНЫЕ analysis-поля legacy enrichment для 4.7 factorial:
  id, sentiment, confidence, ticker, tickers_affected (list), asset_class,
  urgency, category, reason, price_driven, causal

vs `extract_legacy_categories.py` (Sprint 4.6) — там только subset для stratification.
Этот скрипт нужен ОДИН раз перед 4.7. Output reuse'ит вся аналитика 4.7+.

Usage:
    python sprint4\\analysis\\extract_legacy_full_analysis.py
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "docs" / "legacy promt" / "news_pool.jsonl"
DEFAULT_OUT = PROJECT_ROOT / "sprint4" / "analysis" / "data" / "legacy_full_analysis.parquet"

log = logging.getLogger("extract_legacy_full")


def _coerce_list(x) -> list[str]:
    if isinstance(x, list):
        return [s for s in x if isinstance(s, str)]
    return []


def _coerce_bool(x) -> bool | None:
    if isinstance(x, bool):
        return x
    return None


def _coerce_float(x) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-extract legacy full analysis from news_pool.jsonl")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    if not args.source.exists():
        log.error("source not found: %s", args.source)
        return 2

    log.info("streaming %s ...", args.source)
    t0 = time.perf_counter()

    rows: list[dict] = []
    n_lines = 0
    n_malformed = 0
    n_no_analysis = 0

    with open(args.source, "rb") as f:
        for line in f:
            n_lines += 1
            try:
                rec = orjson.loads(line)
            except Exception:
                n_malformed += 1
                continue

            event_id = rec.get("id")
            if not event_id:
                n_malformed += 1
                continue

            analysis = rec.get("analysis") if isinstance(rec.get("analysis"), dict) else None
            if analysis is None:
                n_no_analysis += 1
                analysis = {}

            # Защитное приведение типов — legacy LLM иногда возвращал странные вещи
            rows.append({
                "id": event_id,
                "legacy_sentiment": analysis.get("sentiment") if isinstance(analysis.get("sentiment"), str) else None,
                "legacy_confidence": _coerce_float(analysis.get("confidence")),
                "legacy_ticker": analysis.get("ticker") if isinstance(analysis.get("ticker"), str) else None,
                "legacy_tickers_affected": _coerce_list(analysis.get("tickers_affected")),
                "legacy_asset_class": analysis.get("asset_class") if isinstance(analysis.get("asset_class"), str) else None,
                "legacy_urgency": analysis.get("urgency") if isinstance(analysis.get("urgency"), str) else None,
                "legacy_category": analysis.get("category") if isinstance(analysis.get("category"), str) else None,
                "legacy_reason": analysis.get("reason") if isinstance(analysis.get("reason"), str) else None,
                "legacy_price_driven": _coerce_bool(analysis.get("price_driven")),
                "legacy_causal": _coerce_bool(analysis.get("causal")),
            })

            if n_lines % 50_000 == 0:
                log.info("  scanned %d lines, kept %d records", n_lines, len(rows))

    elapsed = time.perf_counter() - t0
    log.info(
        "stream done: %d lines, %d records, %d malformed, %d no_analysis in %.1fs",
        n_lines, len(rows), n_malformed, n_no_analysis, elapsed,
    )

    df = pl.DataFrame(
        rows,
        schema={
            "id": pl.Utf8,
            "legacy_sentiment": pl.Utf8,
            "legacy_confidence": pl.Float64,
            "legacy_ticker": pl.Utf8,
            "legacy_tickers_affected": pl.List(pl.Utf8),
            "legacy_asset_class": pl.Utf8,
            "legacy_urgency": pl.Utf8,
            "legacy_category": pl.Utf8,
            "legacy_reason": pl.Utf8,
            "legacy_price_driven": pl.Boolean,
            "legacy_causal": pl.Boolean,
        },
        strict=False,
    )

    # Dedup by id (keep last — самое свежее обогащение)
    before = df.height
    df = df.unique(subset=["id"], keep="last", maintain_order=True)
    if before != df.height:
        log.info("dedup by id: %d -> %d (-%d)", before, df.height, before - df.height)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(args.output))
    log.info("written %s: %d rows × %d cols", args.output, df.height, df.width)

    # Quick stats
    log.info("")
    log.info("=== Legacy category distribution ===")
    cat_counts = (
        df.group_by("legacy_category").agg(pl.len().alias("n")).sort("n", descending=True)
    )
    total = df.height
    for row in cat_counts.iter_rows(named=True):
        cat = row["legacy_category"] or "<null>"
        log.info("  %-15s  %7d  (%.1f%%)", cat, row["n"], 100.0 * row["n"] / max(total, 1))

    log.info("")
    log.info("=== Legacy sentiment ===")
    sent_counts = (
        df.group_by("legacy_sentiment").agg(pl.len().alias("n")).sort("n", descending=True)
    )
    for row in sent_counts.iter_rows(named=True):
        s = row["legacy_sentiment"] or "<null>"
        log.info("  %-10s  %7d  (%.1f%%)", s, row["n"], 100.0 * row["n"] / max(total, 1))

    log.info("")
    log.info("=== Boolean flags rate ===")
    pd_true = int((df["legacy_price_driven"] == True).sum())
    cz_true = int((df["legacy_causal"] == True).sum())
    log.info("  price_driven=true:  %d (%.1f%%)", pd_true, 100.0 * pd_true / max(total, 1))
    log.info("  causal=true:        %d (%.1f%%)", cz_true, 100.0 * cz_true / max(total, 1))

    return 0


if __name__ == "__main__":
    sys.exit(main())
