"""
sprint4/reenrich/extract_legacy_categories.py — one-time stream-parse news_pool.jsonl
                                                 → {id, legacy_category, legacy_sentiment,
                                                    legacy_ticker} parquet.

Назначение:
  - 4.5 enrichment даёт категорию, но пока он не далеко (339/30518 = 1.1%) → стратификация
    4.6 по 4.5-категориям шумная.
  - Legacy news_pool.jsonl содержит analysis.category для 393k records → стабильный
    стратификационный ключ доступный СРАЗУ.
  - Этот скрипт извлекает только нужные поля в компактный parquet (~10-20MB vs 2.5GB jsonl)
    чтобы sample_for_4_6.py читал быстро.

Запуск:
  python sprint4\\reenrich\\extract_legacy_categories.py

Output: sprint4/reenrich/data/legacy_categories.parquet
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import orjson
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = PROJECT_ROOT / "docs" / "legacy promt" / "news_pool.jsonl"
DEFAULT_OUT = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "legacy_categories.parquet"

log = logging.getLogger("extract_legacy_categories")


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
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

            analysis = rec.get("analysis") or {}
            if not isinstance(analysis, dict):
                n_no_analysis += 1
                analysis = {}

            rows.append({
                "id": event_id,
                "legacy_category": analysis.get("category"),
                "legacy_sentiment": analysis.get("sentiment"),
                "legacy_ticker": analysis.get("ticker"),
                "legacy_urgency": analysis.get("urgency"),
                "legacy_confidence": analysis.get("confidence"),
            })

            if n_lines % 50_000 == 0:
                log.info("  scanned %d lines, kept %d records", n_lines, len(rows))

    elapsed = time.perf_counter() - t0
    log.info(
        "stream done: %d lines, %d records, %d malformed, %d no_analysis in %.1fs",
        n_lines, len(rows), n_malformed, n_no_analysis, elapsed,
    )

    # Schema explicit — confidence может быть int или float, нормализуем
    df = pl.DataFrame(
        rows,
        schema={
            "id": pl.Utf8,
            "legacy_category": pl.Utf8,
            "legacy_sentiment": pl.Utf8,
            "legacy_ticker": pl.Utf8,
            "legacy_urgency": pl.Utf8,
            "legacy_confidence": pl.Float64,
        },
        strict=False,
    )

    # Дедуп по id — Sprint 4.2.a показал 0.62% дублей по text, но dedup тут не по text,
    # а по id (в news_pool могут быть переобработки) — keep last (свежее обогащение).
    before = df.height
    df = df.unique(subset=["id"], keep="last", maintain_order=True)
    if before != df.height:
        log.info("dedup by id: %d -> %d (-%d)", before, df.height, before - df.height)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(str(args.output))
    log.info("written %s: %d rows × %d cols", args.output, df.height, df.width)

    # Summary by category
    log.info("")
    log.info("=== Legacy category distribution ===")
    cat_counts = (
        df.group_by("legacy_category")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
    total = df.height
    for row in cat_counts.iter_rows(named=True):
        cat = row["legacy_category"] or "<null>"
        pct = 100.0 * row["n"] / max(total, 1)
        log.info("  %-15s  %7d  (%.1f%%)", cat, row["n"], pct)

    return 0


if __name__ == "__main__":
    sys.exit(main())
