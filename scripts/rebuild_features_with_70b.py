r"""
scripts/rebuild_features_with_70b.py
====================================
Берёт Phase 2 features_mfe.parquet и пересобирает 22 LLM-derived колонки
из новой 70b enrichment.

Schema:
  In:  D:\...\phase2_mfe\features_mfe.parquet  (70,184 rows × 70 cols, legacy LLM)
       data/reenrich_phase2/fold13_rolling_12mo_70b.parquet  (19,642 events, new 70b)
  Out: data/reenrich_phase2/features_mfe_70b.parquet
       (~50k rows × 70 cols — те же multi-ticker строки, но LLM cols переписаны)

Что заменяется (22 cols):
  sent_bullish, sent_bearish, sent_neutral      — из top-impact ticker sentiment
  confidence                                     — из top-impact ticker confidence
  urg_high, urg_medium, urg_low                  — из event-level urgency
  cat_geopolitics ... cat_other (12 cat_*)       — из event-level category
  price_driven, causal                           — 0 (нет в новой схеме)
  n_tickers_aff                                  — max(0, len(tickers) - 1)

Что НЕ заменяется (legacy preserved):
  cum_sentiment_24h — rolling 24h aggregation, оставлен от Phase 2 (approximation для v1)
  Все технические/ценовые/временные cols — ret_*, atr_*, rsi_*, hour, dow, etc.

Зачем:
  XGBoost модели Phase 2 обучены на legacy LLM features (Ollama 8B + Anthropic Haiku),
  prod inference идёт на Llama 3.3 70b → distribution shift. Sprint 5.6 fix: переобучить
  модели на features.parquet, где LLM cols происходят из той же модели, что в prod.

Запуск:
  python scripts/rebuild_features_with_70b.py
"""

from __future__ import annotations

import argparse
import bisect
import logging
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_DIR = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe")
DEFAULT_FEATURES = PHASE2_DIR / "features_mfe.parquet"
DEFAULT_70B = PROJECT_ROOT / "data" / "reenrich_phase2" / "fold13_rolling_12mo_70b.parquet"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_70b.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rebuild_features")

# Категории: new prompt v1.0.0 → legacy Phase 2 columns
# Anything not in keys → cat_other
NEW_CAT_TO_LEGACY = {
    "cbr": "cat_cbr",
    "geopolitics": "cat_geopolitics",
    "macro": "cat_macro",
    "corporate": "cat_corporate",
    "commodity": "cat_commodity",
    "currency": "cat_currency",
    "market": "cat_other",            # market — новая категория, mapping to cat_other
    "other": "cat_other",
    "regulatory": "cat_regulation",   # rare/out-of-schema fallback
    "infrastructure": "cat_other",    # rare/out-of-schema fallback
}

LEGACY_CAT_COLS = [
    "cat_geopolitics", "cat_macro", "cat_cbr", "cat_corporate",
    "cat_commodity", "cat_currency", "cat_sanctions", "cat_earnings",
    "cat_dividends", "cat_ma", "cat_regulation", "cat_other",
]

REPLACED_COLS = LEGACY_CAT_COLS + [
    "sent_bullish", "sent_bearish", "sent_neutral",
    "confidence",
    "urg_high", "urg_medium", "urg_low",
    "price_driven", "causal", "n_tickers_aff",
]


def compute_event_features(row: dict) -> dict:
    """Maps one 70b-enrichment row to legacy feature columns (event-level)."""
    out = {c: 0.0 for c in LEGACY_CAT_COLS}
    out.update({
        "sent_bullish": 0.0,
        "sent_bearish": 0.0,
        "sent_neutral": 1.0,  # default: neutral
        "confidence": 0.5,
        "urg_high": 0.0,
        "urg_medium": 0.0,
        "urg_low": 1.0,       # default: low
        "price_driven": 0.0,  # not in new schema
        "causal": 0.0,        # not in new schema
        "n_tickers_aff": 0.0,
    })

    # invalid_json и прочие enrich_error → defaults (neutral/low/other)
    err = row.get("enrich_error")
    if err and err != "ok":
        out["cat_other"] = 1.0
        return out

    # Category
    cat = row.get("category")
    if cat:
        legacy_col = NEW_CAT_TO_LEGACY.get(cat, "cat_other")
        out[legacy_col] = 1.0
    else:
        out["cat_other"] = 1.0

    # Urgency
    urg = row.get("urgency")
    if urg == "high":
        out["urg_high"] = 1.0
        out["urg_low"] = 0.0
    elif urg == "medium":
        out["urg_medium"] = 1.0
        out["urg_low"] = 0.0
    # else: urg_low stays 1.0

    # Tickers list
    tickers = row.get("tickers") or []
    valid = [t for t in tickers if isinstance(t, dict) and t.get("ticker")]
    n_t = len(valid)
    out["n_tickers_aff"] = float(max(0, n_t - 1))

    if n_t > 0:
        # Top-impact ticker даёт event-level sentiment + confidence
        valid.sort(key=lambda t: (t.get("impact_strength") or 0.0), reverse=True)
        top = valid[0]

        sent = top.get("sentiment")
        if sent == "positive":
            out["sent_bullish"] = 1.0
            out["sent_neutral"] = 0.0
        elif sent == "negative":
            out["sent_bearish"] = 1.0
            out["sent_neutral"] = 0.0
        # neutral / None → keeps sent_neutral=1.0

        conf = top.get("confidence")
        if conf is not None:
            try:
                out["confidence"] = float(conf)
            except (ValueError, TypeError):
                pass

    return out


def _sentiment_value(sent: str | None) -> int:
    """Map sentiment string → signed integer for cum_sentiment_24h aggregation."""
    if sent == "positive":
        return 1
    if sent == "negative":
        return -1
    return 0


def build_per_ticker_timeline(enriched: pl.DataFrame) -> dict[str, list[tuple[pd.Timestamp, float]]]:
    """Из 70b enrichment строит {ticker → sorted [(ts, sentiment_value × confidence)]}.

    Каждое event входит в timeline ВСЕХ своих tickers (per-ticker sentiment, не event-level).
    Sentiment в новой схеме = "positive" / "negative" / "neutral" (per-ticker внутри tickers list).
    Использует datetime_msk напрямую из aggregate parquet.

    Returns: dict ticker → list sorted by ts ascending.
    """
    timeline: dict[str, list[tuple[pd.Timestamp, float]]] = defaultdict(list)

    if "datetime_msk" not in enriched.columns:
        raise ValueError(
            "Enriched parquet missing datetime_msk column. "
            "Re-run aggregate_checkpoint.py with --sample pointing at an input parquet that has it."
        )

    n_skip_no_ts = 0
    n_used = 0
    for row in enriched.iter_rows(named=True):
        ts = row.get("datetime_msk")
        if ts is None:
            n_skip_no_ts += 1
            continue
        # Errors → no signal
        if row.get("enrich_error") and row["enrich_error"] != "ok":
            continue
        tickers = row.get("tickers") or []
        for t in tickers:
            if not isinstance(t, dict):
                continue
            ticker_name = t.get("ticker")
            if not ticker_name:
                continue
            sv = _sentiment_value(t.get("sentiment"))
            conf = t.get("confidence")
            try:
                conf = float(conf) if conf is not None else 0.0
            except (ValueError, TypeError):
                conf = 0.0
            signed_val = sv * conf
            timeline[ticker_name].append((pd.Timestamp(ts), signed_val))
            n_used += 1

    # Sort each ticker timeline by ts
    for ticker_name, items in timeline.items():
        items.sort(key=lambda x: x[0])

    log.info("  per-ticker timeline: %d tickers, %d total entries (skipped %d w/o ts)",
             len(timeline), n_used, n_skip_no_ts)
    return timeline


def recompute_cum_sentiment_24h(sub: pd.DataFrame,
                                 timeline: dict[str, list[tuple[pd.Timestamp, float]]]) -> pd.Series:
    """Для каждой (event, ticker) строки в features: sum sentiment×conf за 24h до event_ts
    среди событий упоминающих этот ticker (per-ticker sentiment).
    """
    sub = sub.copy()
    sub["_dt"] = pd.to_datetime(sub["_datetime"])
    out = np.zeros(len(sub), dtype=np.float64)
    h24 = pd.Timedelta(hours=24)

    # Pre-extract sorted ts arrays per ticker for binary search
    per_ticker_ts: dict[str, list[pd.Timestamp]] = {}
    per_ticker_vals: dict[str, list[float]] = {}
    per_ticker_cum: dict[str, np.ndarray] = {}
    for ticker_name, items in timeline.items():
        per_ticker_ts[ticker_name] = [x[0] for x in items]
        vals = np.array([x[1] for x in items], dtype=np.float64)
        per_ticker_vals[ticker_name] = vals.tolist()
        # Cumulative sum for fast range sums
        per_ticker_cum[ticker_name] = np.concatenate([[0.0], np.cumsum(vals)])

    for i, (ts, ticker_name) in enumerate(zip(sub["_dt"], sub["_ticker"])):
        ts_arr = per_ticker_ts.get(ticker_name)
        if not ts_arr:
            continue
        # Window [ts - 24h, ts) — exclude self
        lo_ts = ts - h24
        # bisect: lo = first idx where ts_arr[lo] >= lo_ts; hi = first idx where ts_arr[hi] >= ts
        lo = bisect.bisect_left(ts_arr, lo_ts)
        hi = bisect.bisect_left(ts_arr, ts)
        if lo == hi:
            continue
        cum = per_ticker_cum[ticker_name]
        out[i] = cum[hi] - cum[lo]
    return pd.Series(out, index=sub.index)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default=str(DEFAULT_FEATURES))
    ap.add_argument("--enriched-70b", default=str(DEFAULT_70B))
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--recompute-cum-sent", action="store_true",
                    help="Recompute cum_sentiment_24h from full 70b enrichment "
                         "(replaces legacy approximation). Requires --enriched-70b "
                         "covering all events in the 24h pre-window of any feature row.")
    args = ap.parse_args()

    log.info("Loading 70b enrichment: %s", args.enriched_70b)
    enriched = pl.read_parquet(args.enriched_70b)
    log.info("  %d events", enriched.height)

    log.info("Building per-event LLM feature dicts...")
    new_features: dict[str, dict] = {}
    for row in enriched.iter_rows(named=True):
        new_features[row["id"]] = compute_event_features(row)
    log.info("  built %d feature dicts", len(new_features))

    log.info("Loading Phase 2 features: %s", args.features)
    features = pd.read_parquet(args.features)
    log.info("  %d rows × %d cols", len(features), len(features.columns))

    features["_id"] = features["_id"].astype(str)
    target_ids = set(new_features.keys())
    sub = features[features["_id"].isin(target_ids)].copy()
    n_events_covered = sub["_id"].nunique()
    log.info("  rows after filter: %d (covers %d / %d events)",
             len(sub), n_events_covered, len(target_ids))

    if n_events_covered < len(target_ids):
        missing = len(target_ids) - n_events_covered
        log.warning("  %d 70b events НЕ найдены в features (нет соответствующих trades в Phase 2)", missing)

    # Date range
    sub["_dt"] = pd.to_datetime(sub["_datetime"])
    log.info("  date range: %s → %s", sub["_dt"].min(), sub["_dt"].max())

    # Per-event row stats (how many tickers per event in features.parquet)
    per_event = sub.groupby("_id").size()
    log.info("  rows per event: mean=%.2f median=%.0f max=%d",
             per_event.mean(), per_event.median(), per_event.max())

    # === Replace 22 LLM-derived columns ===
    log.info("Replacing %d LLM-derived columns...", len(REPLACED_COLS))
    for col in REPLACED_COLS:
        sub[col] = sub["_id"].map(lambda i: new_features[i][col]).astype(np.float64)

    # Drop temp col
    sub = sub.drop(columns=["_dt"])

    if args.recompute_cum_sent:
        log.info("Recomputing cum_sentiment_24h from 70b enrichment...")
        timeline = build_per_ticker_timeline(enriched)
        new_cum = recompute_cum_sentiment_24h(sub, timeline)
        old = sub["cum_sentiment_24h"].copy()
        sub["cum_sentiment_24h"] = new_cum.values
        log.info("  cum_sentiment_24h replaced: old_mean=%.3f new_mean=%.3f  old_std=%.3f new_std=%.3f",
                 old.mean(), new_cum.mean(), old.std(), new_cum.std())
        # Quick distribution comparison
        n_zero_new = int((new_cum == 0).sum())
        n_zero_old = int((old == 0).sum())
        log.info("  zero values: old=%d (%.1f%%)  new=%d (%.1f%%)",
                 n_zero_old, 100.0 * n_zero_old / len(sub),
                 n_zero_new, 100.0 * n_zero_new / len(sub))
    else:
        log.info("  cum_sentiment_24h: KEPT FROM LEGACY (rolling 24h aggregation, v1 approximation)")

    # === Summaries ===
    log.info("")
    log.info("=== New category distribution ===")
    for col in LEGACY_CAT_COLS:
        n = int(sub[col].sum())
        if n > 0:
            log.info("  %-20s %6d (%.1f%%)", col, n, 100.0 * n / len(sub))

    log.info("")
    log.info("=== New sentiment distribution ===")
    for col in ["sent_bullish", "sent_bearish", "sent_neutral"]:
        n = int(sub[col].sum())
        log.info("  %-20s %6d (%.1f%%)", col, n, 100.0 * n / len(sub))

    log.info("")
    log.info("=== New urgency distribution ===")
    for col in ["urg_high", "urg_medium", "urg_low"]:
        n = int(sub[col].sum())
        log.info("  %-20s %6d (%.1f%%)", col, n, 100.0 * n / len(sub))

    log.info("")
    log.info("Confidence stats: mean=%.3f std=%.3f min=%.2f max=%.2f",
             sub["confidence"].mean(), sub["confidence"].std(),
             sub["confidence"].min(), sub["confidence"].max())
    log.info("n_tickers_aff stats: mean=%.2f max=%.0f",
             sub["n_tickers_aff"].mean(), sub["n_tickers_aff"].max())

    # Save
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    sub.to_parquet(args.output, index=False)
    log.info("")
    log.info("Saved: %s  (%d rows × %d cols)", args.output, len(sub), len(sub.columns))


if __name__ == "__main__":
    main()
