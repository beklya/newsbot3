"""
sprint4/reenrich/aggregate_checkpoint.py — append-only checkpoint.jsonl -> parquet,
                                            joined с исходным C1/V1 sample.

Запускается:
  - Промежуточно (в любой момент во время прогона 4.5) — даёт snapshot для аналитики
  - Финально (после 4.5 done) — производит c1_8b_v1_0_0.parquet для 4.7

Behaviour:
  - Read checkpoint.jsonl, dedup by event_id (keep latest by `at` timestamp)
  - Build polars DF из enrichment-полей + tickers как list<struct>
  - Left-join с sample (calibration_sample.parquet или validation_sample.parquet)
  - Add `is_enriched` column для удобства фильтрации
  - Write parquet + текстовый summary

Usage:
    python sprint4\\reenrich\\aggregate_checkpoint.py --model llama-3.1-8b-instant
    python sprint4\\reenrich\\aggregate_checkpoint.py --model llama-3.3-70b-versatile \\
        --sample sprint4\\reenrich\\data\\c1_subset_for_4_6.parquet \\
        --output sprint4\\reenrich\\data\\c1_subset_70b_v1_0_0.parquet
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SAMPLE = PROJECT_ROOT / "sprint4" / "sampling" / "data" / "calibration_sample.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sprint4" / "reenrich" / "data"
DEFAULT_PROMPT_VERSION = "1.0.0"

log = logging.getLogger("aggregate")


# ----------------------------------------------------------------------------
# I/O helpers
# ----------------------------------------------------------------------------
def model_filename_safe(model: str) -> str:
    """llama-3.1-8b-instant -> llama_3_1_8b_instant"""
    return model.replace("/", "_").replace("-", "_").replace(".", "_")


def load_checkpoint_dedup(path: Path) -> list[dict]:
    """Read checkpoint.jsonl, dedup by event_id keeping latest (max at)."""
    if not path.exists():
        raise FileNotFoundError(f"checkpoint not found: {path}")
    by_id: dict[str, dict] = {}
    n_lines = 0
    n_malformed = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n_lines += 1
            try:
                rec = json.loads(line)
            except Exception:
                n_malformed += 1
                continue
            event_id = rec.get("id")
            if not event_id:
                n_malformed += 1
                continue
            existing = by_id.get(event_id)
            # Latest по at (строковая лексикография работает для ISO 8601 Z-suffix)
            if existing is None or (rec.get("at") or "") > (existing.get("at") or ""):
                by_id[event_id] = rec
    log.info(
        "checkpoint: %d lines -> %d unique ids (%d malformed/skipped)",
        n_lines, len(by_id), n_malformed,
    )
    return list(by_id.values())


# ----------------------------------------------------------------------------
# DataFrame construction
# ----------------------------------------------------------------------------
TICKER_STRUCT = pl.Struct({
    "ticker": pl.Utf8,
    "direction": pl.Utf8,
    "sentiment": pl.Utf8,
    "confidence": pl.Float64,
    "impact_strength": pl.Float64,
    "rationale": pl.Utf8,
})

ENRICH_SCHEMA: dict[str, Any] = {
    "id": pl.Utf8,
    "enrich_model": pl.Utf8,
    "enrich_prompt_version": pl.Utf8,
    "enrich_key_id": pl.Int64,
    "enrich_at": pl.Utf8,
    "enrich_error": pl.Utf8,
    "enrich_error_message": pl.Utf8,
    "enrich_latency_ms": pl.Float64,
    "enrich_input_tokens": pl.Int64,
    "enrich_output_tokens": pl.Int64,
    "enrich_total_tokens": pl.Int64,
    "enrich_raw_response": pl.Utf8,
    "is_financial": pl.Boolean,
    "summary": pl.Utf8,
    "expected_timeframe": pl.Utf8,
    "urgency": pl.Utf8,
    "category": pl.Utf8,
    "is_actionable": pl.Boolean,
    "tickers": pl.List(TICKER_STRUCT),
}


def _safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _safe_int(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def _normalize_tickers(raw: Any) -> list[dict]:
    """Build list of dicts matching TICKER_STRUCT schema. Defensive against bad LLM output."""
    if not isinstance(raw, list):
        return []
    out = []
    for t in raw:
        if not isinstance(t, dict):
            continue
        out.append({
            "ticker": t.get("ticker") if isinstance(t.get("ticker"), str) else None,
            "direction": t.get("direction") if isinstance(t.get("direction"), str) else None,
            "sentiment": t.get("sentiment") if isinstance(t.get("sentiment"), str) else None,
            "confidence": _safe_float(t.get("confidence")),
            "impact_strength": _safe_float(t.get("impact_strength")),
            "rationale": t.get("rationale") if isinstance(t.get("rationale"), str) else None,
        })
    return out


def records_to_df(records: list[dict]) -> pl.DataFrame:
    """Convert dedup'd checkpoint records to polars DF with stable schema."""
    rows: list[dict] = []
    for r in records:
        parsed = r.get("parsed") if isinstance(r.get("parsed"), dict) else {}
        parsed = parsed or {}
        rows.append({
            "id": r.get("id"),
            "enrich_model": r.get("model"),
            "enrich_prompt_version": r.get("prompt_version"),
            "enrich_key_id": _safe_int(r.get("key_id")),
            "enrich_at": r.get("at"),
            "enrich_error": r.get("error"),
            "enrich_error_message": r.get("error_message"),
            "enrich_latency_ms": _safe_float(r.get("latency_ms")),
            "enrich_input_tokens": _safe_int(r.get("input_tokens")),
            "enrich_output_tokens": _safe_int(r.get("output_tokens")),
            "enrich_total_tokens": _safe_int(r.get("total_tokens")),
            "enrich_raw_response": r.get("raw_response"),
            "is_financial": parsed.get("is_financial") if isinstance(parsed.get("is_financial"), bool) else None,
            "summary": parsed.get("summary") if isinstance(parsed.get("summary"), str) else None,
            "expected_timeframe": parsed.get("expected_timeframe") if isinstance(parsed.get("expected_timeframe"), str) else None,
            "urgency": parsed.get("urgency") if isinstance(parsed.get("urgency"), str) else None,
            "category": parsed.get("category") if isinstance(parsed.get("category"), str) else None,
            "is_actionable": parsed.get("is_actionable") if isinstance(parsed.get("is_actionable"), bool) else None,
            "tickers": _normalize_tickers(parsed.get("tickers")),
        })
    return pl.DataFrame(rows, schema=ENRICH_SCHEMA)


# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
def print_summary(out_df: pl.DataFrame, enrich_df: pl.DataFrame) -> None:
    log.info("=" * 70)
    log.info("Aggregation summary")
    log.info("=" * 70)
    total = out_df.height
    enriched = int(out_df["is_enriched"].sum())
    log.info("Sample rows:    %d", total)
    log.info("Enriched:       %d  (%.1f%%)", enriched, 100.0 * enriched / max(total, 1))
    log.info("Pending:        %d", total - enriched)

    if enriched == 0:
        return

    # Error breakdown
    errs = out_df.filter(pl.col("is_enriched")).group_by("enrich_error").agg(pl.len().alias("n")).sort("n", descending=True)
    log.info("")
    log.info("Errors:")
    for row in errs.iter_rows(named=True):
        e = row["enrich_error"] or "ok"
        log.info("  %-25s  %d", e, row["n"])

    # Token aggregate
    enriched_df = out_df.filter(pl.col("is_enriched") & pl.col("enrich_total_tokens").is_not_null())
    if enriched_df.height > 0:
        in_sum = int(enriched_df["enrich_input_tokens"].sum())
        out_sum = int(enriched_df["enrich_output_tokens"].sum())
        tot_sum = int(enriched_df["enrich_total_tokens"].sum())
        log.info("")
        log.info("Tokens total:   in=%d out=%d total=%d", in_sum, out_sum, tot_sum)
        log.info("  per event mean: %d total (%d in + %d out)",
                 tot_sum // enriched_df.height,
                 in_sum // enriched_df.height,
                 out_sum // enriched_df.height)

    # Categories
    if "category" in out_df.columns:
        cats = (out_df.filter(pl.col("is_enriched") & pl.col("category").is_not_null())
                .group_by("category")
                .agg(pl.len().alias("n"))
                .sort("n", descending=True))
        if cats.height > 0:
            log.info("")
            log.info("Categories:")
            for row in cats.iter_rows(named=True):
                pct = 100.0 * row["n"] / enriched
                log.info("  %-15s  %5d  (%.1f%%)", row["category"], row["n"], pct)

    # is_financial breakdown
    fin = out_df.filter(pl.col("is_enriched")).group_by("is_financial").agg(pl.len().alias("n")).sort("n", descending=True)
    if fin.height > 0:
        log.info("")
        log.info("is_financial:")
        for row in fin.iter_rows(named=True):
            log.info("  %-10s  %d", str(row["is_financial"]), row["n"])

    # Empty_financial (is_financial=True ∧ no tickers) — known issue
    empty_fin_n = out_df.filter(
        pl.col("is_enriched") & pl.col("is_financial") & (pl.col("tickers").list.len() == 0)
    ).height
    log.info("")
    log.info("EMPTY_FINANCIAL (is_financial=True but tickers=[]):  %d  (%.1f%% of enriched)",
             empty_fin_n, 100.0 * empty_fin_n / max(enriched, 1))

    # Top tickers
    if enriched > 0:
        explode = out_df.filter(pl.col("is_enriched")).select(
            pl.col("tickers").list.eval(pl.element().struct.field("ticker")).alias("tickers_only")
        ).explode("tickers_only").drop_nulls()
        if explode.height > 0:
            top = explode.group_by("tickers_only").agg(pl.len().alias("n")).sort("n", descending=True).head(15)
            log.info("")
            log.info("Top tickers (top 15):")
            for row in top.iter_rows(named=True):
                log.info("  %-10s  %d", row["tickers_only"], row["n"])

    log.info("=" * 70)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate deepinfra_runner checkpoint -> parquet")
    parser.add_argument("--model", type=str, required=True,
                        help="Model name (used to locate default checkpoint path)")
    parser.add_argument("--prompt-version", type=str, default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--checkpoint", type=Path, default=None,
                        help="Override checkpoint path")
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE,
                        help="Original sample parquet (C1 or V1)")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output parquet path (default: derived from model name)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    model_tag = model_filename_safe(args.model)
    pv_tag = args.prompt_version.replace(".", "_")

    checkpoint_path = args.checkpoint or (
        DEFAULT_OUTPUT_DIR / f"checkpoint_{model_tag}_v{pv_tag}.jsonl"
    )
    output_path = args.output or (
        DEFAULT_OUTPUT_DIR / f"c1_{model_tag}_v{pv_tag}.parquet"
    )

    log.info("checkpoint: %s", checkpoint_path)
    log.info("sample:     %s", args.sample)
    log.info("output:     %s", output_path)

    # 1. Load checkpoint
    records = load_checkpoint_dedup(checkpoint_path)
    if not records:
        log.error("no records in checkpoint — nothing to aggregate")
        return 2

    # 2. To DataFrame
    enrich_df = records_to_df(records)
    log.info("enrich_df: %d rows × %d cols", enrich_df.height, enrich_df.width)

    # 3. Load sample
    sample_df = pl.read_parquet(str(args.sample))
    log.info("sample_df: %d rows × %d cols", sample_df.height, sample_df.width)

    # 4. Left join (sample keeps all rows; some may still be pending enrichment)
    out_df = sample_df.join(enrich_df, on="id", how="left")
    out_df = out_df.with_columns(
        pl.col("enrich_at").is_not_null().alias("is_enriched"),
    )

    # 5. Write
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.write_parquet(str(output_path))
    log.info("written %s: %d rows × %d cols", output_path, out_df.height, out_df.width)

    # 6. Summary
    print_summary(out_df, enrich_df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
