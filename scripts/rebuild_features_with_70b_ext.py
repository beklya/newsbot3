r"""Sprint 6.1 Y4 — Rebuild Phase 2 features.parquet with 70B + EXT 10-col extension.

Extends scripts/rebuild_features_with_70b.py by adding 10 70B-only feature
columns that Phase 2 corpus (Ollama 8B v2 prompt) could NOT produce:

  is_actionable_int, is_financial_int,
  tf_instant, tf_fast, tf_medium, tf_slow,
  impact_strength,
  dir_long, dir_short, dir_neutral,
  sell_the_news

Per-ticker fields (impact_strength, dir_*, sell_the_news) require looking up
the TickerImpact entry in enriched.tickers for the specific (event, ticker)
pair of each Phase 2 feature row.

Output: data/reenrich_phase2/features_mfe_70b_ext.parquet
        Same row count as features_mfe_70b.parquet, +10 columns -> ~80 total.

Usage:
    python scripts/rebuild_features_with_70b_ext.py
    python scripts/rebuild_features_with_70b_ext.py --recompute-cum-sent
"""
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_DIR = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe"
)
DEFAULT_FEATURES = PHASE2_DIR / "features_mfe.parquet"
DEFAULT_70B = PROJECT_ROOT / "data" / "reenrich_phase2" / "full_70k_70b.parquet"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_70b_ext.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rebuild_ext")

NEW_CAT_TO_LEGACY = {
    "cbr": "cat_cbr", "geopolitics": "cat_geopolitics", "macro": "cat_macro",
    "corporate": "cat_corporate", "commodity": "cat_commodity",
    "currency": "cat_currency", "market": "cat_other", "other": "cat_other",
    "regulatory": "cat_regulation", "infrastructure": "cat_other",
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

# Y4 NEW COLUMNS — same naming as feature_builder.py
EXT_EVENT_COLS = ["is_actionable_int", "is_financial_int",
                  "tf_instant", "tf_fast", "tf_medium", "tf_slow"]
EXT_PER_TICKER_COLS = ["impact_strength", "dir_long", "dir_short",
                        "dir_neutral", "sell_the_news"]
ALL_EXT_COLS = EXT_EVENT_COLS + EXT_PER_TICKER_COLS


def compute_event_features(row: dict) -> dict:
    """22 legacy LLM features + 6 event-level Y4 features (one row per event)."""
    out = {c: 0.0 for c in LEGACY_CAT_COLS}
    out.update({
        "sent_bullish": 0.0, "sent_bearish": 0.0, "sent_neutral": 1.0,
        "confidence": 0.5,
        "urg_high": 0.0, "urg_medium": 0.0, "urg_low": 1.0,
        "price_driven": 0.0, "causal": 0.0, "n_tickers_aff": 0.0,
        # Y4 event-level
        "is_actionable_int": 0.0, "is_financial_int": 0.0,
        "tf_instant": 0.0, "tf_fast": 0.0, "tf_medium": 1.0, "tf_slow": 0.0,
    })
    err = row.get("enrich_error")
    if err and err != "ok":
        out["cat_other"] = 1.0
        return out
    cat = row.get("category")
    if cat:
        out[NEW_CAT_TO_LEGACY.get(cat, "cat_other")] = 1.0
    else:
        out["cat_other"] = 1.0
    urg = row.get("urgency")
    if urg == "high":
        out["urg_high"] = 1.0; out["urg_low"] = 0.0
    elif urg == "medium":
        out["urg_medium"] = 1.0; out["urg_low"] = 0.0

    # Y4: event-level booleans
    out["is_actionable_int"] = 1.0 if row.get("is_actionable") else 0.0
    out["is_financial_int"] = 1.0 if row.get("is_financial") else 0.0
    out["causal"] = out["is_actionable_int"]  # keep legacy `causal` mapping

    # Y4: expected_timeframe one-hot — reset defaults then set actual
    tf = (row.get("expected_timeframe") or "medium").lower()
    out["tf_instant"] = out["tf_fast"] = out["tf_medium"] = out["tf_slow"] = 0.0
    if tf not in ("instant", "fast", "medium", "slow"):
        tf = "medium"
    out[f"tf_{tf}"] = 1.0

    tickers = row.get("tickers") or []
    valid = [t for t in tickers if isinstance(t, dict) and t.get("ticker")]
    out["n_tickers_aff"] = float(max(0, len(valid) - 1))
    if valid:
        valid.sort(key=lambda t: (t.get("impact_strength") or 0.0), reverse=True)
        top = valid[0]
        sent = top.get("sentiment")
        if sent == "positive":
            out["sent_bullish"] = 1.0; out["sent_neutral"] = 0.0
        elif sent == "negative":
            out["sent_bearish"] = 1.0; out["sent_neutral"] = 0.0
        conf = top.get("confidence")
        if conf is not None:
            try:
                out["confidence"] = float(conf)
            except (ValueError, TypeError):
                pass
    return out


def build_per_ticker_lookup(enriched: pl.DataFrame) -> dict[tuple[str, str], dict]:
    """{(event_id, ticker) -> {impact_strength, direction, sentiment}}"""
    lookup: dict[tuple[str, str], dict] = {}
    for row in enriched.iter_rows(named=True):
        eid = row.get("id")
        if not eid:
            continue
        for t in (row.get("tickers") or []):
            if not isinstance(t, dict):
                continue
            tk = t.get("ticker")
            if not tk:
                continue
            lookup[(eid, tk)] = {
                "impact_strength": t.get("impact_strength") or 0.0,
                "direction": (t.get("direction") or "neutral").lower(),
                "sentiment": (t.get("sentiment") or "neutral").lower(),
            }
    return lookup


def per_ticker_features(event_id: str, ticker: str,
                        lookup: dict[tuple[str, str], dict]) -> dict:
    """5 per-ticker Y4 features for a specific (event, ticker) pair."""
    info = lookup.get((event_id, ticker))
    if info is None:
        # Phase 2 had a trade on this ticker but 70B didn't tag it → neutral defaults
        return {
            "impact_strength": 0.0,
            "dir_long": 0.0, "dir_short": 0.0, "dir_neutral": 1.0,
            "sell_the_news": 0.0,
        }
    out = {"impact_strength": float(info["impact_strength"] or 0.0)}
    d = info["direction"]
    out["dir_long"] = 1.0 if d == "long" else 0.0
    out["dir_short"] = 1.0 if d == "short" else 0.0
    out["dir_neutral"] = 1.0 if d not in ("long", "short") else 0.0
    s = info["sentiment"]
    sell = (s == "positive" and d == "short") or (s == "negative" and d == "long")
    out["sell_the_news"] = 1.0 if sell else 0.0
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default=str(DEFAULT_FEATURES))
    ap.add_argument("--enriched-70b", default=str(DEFAULT_70B))
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    log.info("Loading 70b enrichment: %s", args.enriched_70b)
    enriched = pl.read_parquet(args.enriched_70b)
    log.info("  %d events", enriched.height)

    log.info("Building per-event feature dicts (with Y4 event-level cols)...")
    event_features: dict[str, dict] = {}
    for row in enriched.iter_rows(named=True):
        event_features[row["id"]] = compute_event_features(row)
    log.info("  built %d event-feature dicts", len(event_features))

    log.info("Building (event_id, ticker) -> per-ticker Y4 lookup...")
    pt_lookup = build_per_ticker_lookup(enriched)
    log.info("  %d (event, ticker) pairs", len(pt_lookup))

    log.info("Loading Phase 2 features: %s", args.features)
    features = pd.read_parquet(args.features)
    features["_id"] = features["_id"].astype(str)
    target_ids = set(event_features.keys())
    sub = features[features["_id"].isin(target_ids)].copy()
    log.info("  %d rows after filter, %d unique events", len(sub), sub["_id"].nunique())

    # Replace existing legacy LLM cols
    log.info("Replacing %d legacy LLM cols + 6 Y4 event cols...",
             len(REPLACED_COLS))
    all_event_cols = REPLACED_COLS + EXT_EVENT_COLS
    for col in all_event_cols:
        sub[col] = sub["_id"].map(lambda i: event_features[i][col]).astype(np.float64)

    # Add Y4 per-ticker cols (need _ticker)
    log.info("Adding 5 Y4 per-ticker cols (impact_strength, dir_*, sell_the_news)...")
    for col in EXT_PER_TICKER_COLS:
        sub[col] = 0.0
    for idx, row in sub.iterrows():
        feats = per_ticker_features(row["_id"], row["_ticker"], pt_lookup)
        for k, v in feats.items():
            sub.at[idx, k] = v
    # Default dir_neutral
    sub["dir_neutral"] = np.where(
        (sub["dir_long"] == 0) & (sub["dir_short"] == 0), 1.0, sub["dir_neutral"]
    )

    log.info("Output cols: %d (was %d, +10 Y4)", len(sub.columns),
             len(features.columns))

    # Summary
    log.info("\n=== Y4 feature distributions ===")
    for c in EXT_EVENT_COLS + EXT_PER_TICKER_COLS:
        log.info("  %-20s mean=%.3f std=%.3f", c, sub[c].mean(), sub[c].std())

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    sub.to_parquet(args.output, index=False)
    log.info("\nSaved: %s  (%d rows × %d cols)", args.output, len(sub), len(sub.columns))


if __name__ == "__main__":
    main()
