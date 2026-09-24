r"""Sprint 6.1 — batch-build features for Y6 NEW events (not in Phase 2).

For each (event, ticker) pair in targets_mfe_y6.parquet, reconstruct the
EnrichedNewsEvent + TickerImpact from y6_corpus_70b.parquet, then call the
LIVE production feature_builder.build_features() to compute the 78-feature
vector (Phase 2 67 + Y4 ext 11).

Output schema mirrors features_mfe_70b_ext.parquet so the two can be
concat'd for v7 training.

Usage:
    python scripts/build_features_for_y6_corpus.py \
        --enriched data/reenrich_phase2/y6_corpus_70b.parquet \
        --targets  data/reenrich_phase2/targets_mfe_y6.parquet \
        --output   data/reenrich_phase2/features_mfe_y6_70b_ext.parquet
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.contracts.enriched_news import EnrichedNewsEvent, EnrichedNewsPayload, TickerImpact  # noqa: E402
from src.services.predictor.feature_builder import build_features  # noqa: E402
from src.infra.candles import CandleCache  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("build_y6_features")

# Phase 2 ticker names that the Y6 enrichment may emit (need normalization)
TICKER_LEGACY_MAP = {
    "Si": "SI", "MX": "MIX", "YNDX": "YDEX", "GOLD": "GLDRUB",
}


class StubHistory:
    """Simple deque of recent events per ticker for NewsHistory features.

    feature_builder.py iterates history.for_ticker(ticker, now=ts) and for each
    prior event reads its produced_at + payload.tickers (for that ticker's
    sentiment+confidence).  We implement just enough surface to satisfy that.
    """
    def __init__(self, lookback_hours: int = 24):
        from collections import defaultdict, deque
        self._lookback = timedelta(hours=lookback_hours)
        self._by_ticker: dict[str, "deque"] = defaultdict(lambda: deque(maxlen=50))

    def append(self, ev: "EnrichedNewsEvent"):
        for t in ev.payload.tickers:
            self._by_ticker[t.ticker].append(ev)

    def for_ticker(self, ticker: str, now=None):
        if ticker not in self._by_ticker:
            return []
        cutoff = (now or datetime.now(timezone.utc)) - self._lookback
        out = []
        for ev in self._by_ticker[ticker]:
            try:
                ev_ts = datetime.fromisoformat(ev.produced_at)
            except Exception:
                continue
            if ev_ts >= cutoff:
                out.append(ev)
        return out


def normalize_ticker(t: str) -> str:
    t = (t or "").strip()
    return TICKER_LEGACY_MAP.get(t, t.upper() if t else t)


def reconstruct_event(row: dict) -> EnrichedNewsEvent | None:
    """Build a minimal EnrichedNewsEvent from a Y6 enrichment row."""
    try:
        ts_utc = pd.to_datetime(row["datetime_msk"]) - pd.Timedelta(hours=3)
        produced_at = ts_utc.tz_localize("UTC").isoformat() if ts_utc.tzinfo is None \
                      else ts_utc.tz_convert("UTC").isoformat()
    except Exception:
        return None

    raw_tickers = row.get("tickers") or []
    norm_tickers: list[TickerImpact] = []
    for t in raw_tickers:
        if not isinstance(t, dict):
            continue
        tk = normalize_ticker(t.get("ticker"))
        if not tk:
            continue
        try:
            ti = TickerImpact(
                ticker=tk,
                direction=t.get("direction") or "neutral",
                sentiment=t.get("sentiment") or "neutral",
                confidence=float(t.get("confidence") or 0.0),
                impact_strength=float(t.get("impact_strength") or 0.0),
                rationale=str(t.get("rationale") or "")[:500],
            )
        except Exception:
            continue
        norm_tickers.append(ti)
    if not norm_tickers:
        return None

    cat = (row.get("category") or "other").lower()
    if cat in ("regulatory", "regulation"):
        cat = "other"
    if cat == "infrastructure":
        cat = "other"
    if cat not in ("geopolitics","macro","cbr","corporate","commodity","currency","market","other"):
        cat = "other"

    tf = (row.get("expected_timeframe") or "medium").lower()
    if tf == "fast":
        tf = "short"
    elif tf == "long":
        tf = "slow"
    if tf not in ("instant","short","medium","slow"):
        tf = "medium"

    urg = (row.get("urgency") or "low").lower()
    if urg not in ("high","medium","low"):
        urg = "low"

    try:
        payload = EnrichedNewsPayload(
            raw_event_id=row["id"],
            tg_published_at=produced_at,
            llm_provider="groq",  # Literal limit; not used by features
            llm_model=row.get("enrich_model") or "meta-llama/Llama-3.3-70B-Instruct",
            llm_latency_ms=float(row.get("enrich_latency_ms") or 0.0),
            llm_input_tokens=int(row.get("enrich_input_tokens") or 0),
            llm_output_tokens=int(row.get("enrich_output_tokens") or 0),
            prompt_version=row.get("enrich_prompt_version") or "1.0.0",
            is_financial=bool(row.get("is_financial") or False),
            tickers=norm_tickers,
            summary=(row.get("summary") or "")[:300],
            expected_timeframe=tf,
            urgency=urg,
            category=cat,
            is_actionable=bool(row.get("is_actionable") or False),
            llm_raw_response="",
        )
        ev = EnrichedNewsEvent(producer="enricher", payload=payload, produced_at=produced_at)
        return ev
    except Exception as e:
        log.debug("reconstruct fail id=%s: %s", row.get("id"), e)
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enriched", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "y6_corpus_70b.parquet")
    ap.add_argument("--targets", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_y6.parquet")
    ap.add_argument("--prices-dir", type=Path,
                    default=Path(r"D:\quik_sber\newsbot\prices"))
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" /
                            "features_mfe_y6_70b_ext.parquet")
    args = ap.parse_args()

    log.info("Loading CandleCache from %s ...", args.prices_dir)
    cache = CandleCache(args.prices_dir)
    cache.load_all()

    log.info("Loading enrichment: %s", args.enriched)
    enr = pl.read_parquet(args.enriched).filter(pl.col("is_enriched"))
    enr_by_id: dict[str, dict] = {}
    for row in enr.iter_rows(named=True):
        enr_by_id[row["id"]] = row
    log.info("  enriched rows: %d", enr.height)

    log.info("Loading targets: %s", args.targets)
    tgt = pl.read_parquet(args.targets)
    log.info("  target (event,ticker) rows: %d", tgt.height)
    # Group target_ids and sort by datetime for chronological iteration
    tgt = tgt.with_columns(pl.col("datetime").str.to_datetime().alias("_dt"))
    tgt = tgt.sort("_dt")

    # Pre-sort enrichments by datetime for history walk
    all_events_sorted = enr.sort("datetime_msk")
    # Build (datetime_msk, id) chronological list
    id_to_dt = {row["id"]: pd.Timestamp(row["datetime_msk"])
                for row in all_events_sorted.iter_rows(named=True)}

    history = StubHistory(lookback_hours=24)
    rows: list[dict] = []
    target_ids_per_event = {}
    for row in tgt.iter_rows(named=True):
        eid = row["id"]
        target_ids_per_event.setdefault(eid, []).append(row)

    # Iterate enrichments chronologically; for each event, append to history
    # AND if any targets exist for this (event, ticker) — build features.
    n_built = 0
    n_skip_no_event = 0
    n_skip_no_ticker = 0
    n_total = 0

    for ev_row in all_events_sorted.iter_rows(named=True):
        eid = ev_row["id"]
        ev = reconstruct_event(ev_row)
        if ev is None:
            continue
        # Update history first (so the event itself doesn't appear in its own history)
        target_rows = target_ids_per_event.get(eid) or []
        if target_rows:
            for tgt_row in target_rows:
                n_total += 1
                ticker = tgt_row["ticker"]
                # Find the matching TickerImpact
                ti = next((t for t in ev.payload.tickers if t.ticker == ticker), None)
                if ti is None:
                    n_skip_no_ticker += 1
                    continue
                try:
                    fdict = build_features(ev, ti, cache, history)
                except Exception as e:
                    log.debug("build_features fail %s/%s: %s", eid, ticker, e)
                    continue
                # Add Phase 2-compatible meta cols + target merge keys
                fdict["_id"] = eid
                fdict["_ticker"] = ticker
                fdict["_datetime"] = id_to_dt[eid].isoformat() if eid in id_to_dt else ev.produced_at
                rows.append(fdict)
                n_built += 1
        history.append(ev)
        if n_total > 0 and n_total % 5000 == 0:
            log.info("  ... target processed=%d built=%d", n_total, n_built)

    log.info("=== summary ===")
    log.info("  target rows total:   %d", n_total)
    log.info("  features built:      %d", n_built)
    log.info("  skipped no_ticker:   %d", n_skip_no_ticker)

    if not rows:
        log.error("no features built")
        return 1

    df = pl.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.output)
    log.info("Saved %s (%d rows × %d cols)", args.output, df.height, df.width)
    return 0


if __name__ == "__main__":
    sys.exit(main())
