r"""Sprint 6.4 Phase A — MFE/MAE targets for horizons 120m / 240m / EOD / T+1.

Same entry convention as derive_targets_for_y6_corpus.py (next-min bar open
after news, 30-min gap guard).  New horizons:

  120m / 240m — fixed windows, columns {tgt}_120m / {tgt}_240m
  eod  — window ends at the entry day's 18:45 MSK (main TQBR session close;
         futures evening session deliberately ignored — conservative,
         uniform across classes).  News entering after 18:30 → NaN.
  t1   — window ends at the NEXT trading day's 18:45 MSK (next day with bars
         for this ticker — survives weekends/holidays).

Also emits ret_{h} = signed close return % (long convention) at each horizon
end — for time-only-exit analysis and possible direct-return models.

Run over BOTH corpora so rolling-12-month train windows are covered:
    python scripts/derive_targets_multi_horizon.py \
        --enriched data/reenrich_phase2/full_70k_70b.parquet \
                   data/reenrich_phase2/y6_corpus_70b.parquet \
        --output data/reenrich_phase2/targets_multi_horizon.parquet
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import time as dtime
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.derive_targets_for_y6_corpus import (  # noqa: E402
    TICKER_TO_PREFIX, load_all_candles,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("derive_mh")

EOD_CUTOFF = dtime(18, 45)
EOD_LAST_ENTRY = dtime(18, 30)   # news entering later than this → eod NaN
FIXED_HORIZONS_MIN = [120, 240]
TARGET_NAMES = ("mfe_long", "mae_long", "mfe_short", "mae_short")


def horizon_end_ts(name: str, entry_ts: pd.Timestamp,
                   candles: pd.DataFrame) -> pd.Timestamp | None:
    if name.endswith("m"):
        return entry_ts + pd.Timedelta(minutes=int(name[:-1]))
    eod = entry_ts.normalize() + pd.Timedelta(hours=EOD_CUTOFF.hour,
                                              minutes=EOD_CUTOFF.minute)
    if name == "eod":
        if entry_ts.time() > EOD_LAST_ENTRY:
            return None
        return eod
    if name == "t1":
        # first bar strictly after the entry DAY → that bar's day 18:45
        next_day_start = entry_ts.normalize() + pd.Timedelta(days=1)
        idx = candles.index.searchsorted(next_day_start)
        if idx >= len(candles):
            return None
        nd = candles.index[idx].normalize()
        return nd + pd.Timedelta(hours=EOD_CUTOFF.hour, minutes=EOD_CUTOFF.minute)
    raise ValueError(name)


def compute_targets(candles: pd.DataFrame, news_ts: pd.Timestamp,
                    horizons: list[str]) -> dict | None:
    next_min = (news_ts + pd.Timedelta(seconds=60)).floor("min")
    idx = candles.index.searchsorted(next_min)
    if idx >= len(candles):
        return None
    entry_ts = candles.index[idx]
    if (entry_ts - next_min).total_seconds() > 1800:
        return None  # gap > 30min
    entry_price = float(candles["open"].iloc[idx])
    if entry_price <= 0:
        return None

    out = {"_entry_price": entry_price, "_entry_ts": entry_ts.isoformat()}
    any_valid = False
    for h in horizons:
        end_ts = horizon_end_ts(h, entry_ts, candles)
        cols = [f"{t}_{h}" for t in TARGET_NAMES] + [f"ret_{h}"]
        if end_ts is None or end_ts <= entry_ts:
            for c in cols:
                out[c] = np.nan
            continue
        n = candles.index.searchsorted(end_ts, side="right")
        sub = candles.iloc[idx:n]
        if sub.empty:
            for c in cols:
                out[c] = np.nan
            continue
        h_max = float(sub["high"].max())
        h_min = float(sub["low"].min())
        last_close = float(sub["close"].iloc[-1])
        out[f"mfe_long_{h}"] = round((h_max - entry_price) / entry_price * 100, 4)
        out[f"mae_long_{h}"] = round((entry_price - h_min) / entry_price * 100, 4)
        out[f"mfe_short_{h}"] = round((entry_price - h_min) / entry_price * 100, 4)
        out[f"mae_short_{h}"] = round((h_max - entry_price) / entry_price * 100, 4)
        out[f"ret_{h}"] = round((last_close - entry_price) / entry_price * 100, 4)
        any_valid = True
    return out if any_valid else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enriched", type=Path, nargs="+",
                    default=[PROJECT_ROOT / "data" / "reenrich_phase2" / "full_70k_70b.parquet",
                             PROJECT_ROOT / "data" / "reenrich_phase2" / "y6_corpus_70b.parquet"])
    ap.add_argument("--prices-dir", type=Path,
                    default=Path(r"D:\quik_sber\newsbot\prices"))
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" /
                            "targets_multi_horizon.parquet")
    ap.add_argument("--horizons", default="120m,240m,eod,t1")
    args = ap.parse_args()

    horizons = [h.strip() for h in args.horizons.split(",") if h.strip()]
    candles = load_all_candles(args.prices_dir)
    if not candles:
        log.error("no candle series loaded")
        return 1

    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()
    n_skip = {"no_ticker": 0, "no_candle": 0, "no_window": 0, "dup": 0}

    for src in args.enriched:
        if not src.exists():
            log.error("enriched parquet not found: %s", src)
            return 1
        log.info("loading %s", src)
        enr = pl.read_parquet(src)
        enr = enr.filter(pl.col("is_enriched") & pl.col("is_financial"))
        log.info("  enriched+financial rows: %d", enr.height)

        for ev in enr.iter_rows(named=True):
            eid = ev.get("id")
            dt_msk = ev.get("datetime_msk")
            if dt_msk is None or not eid:
                continue
            try:
                news_ts = pd.Timestamp(dt_msk)
            except Exception:
                continue
            tickers = ev.get("tickers") or []
            if not tickers:
                n_skip["no_ticker"] += 1
                continue
            for t in tickers:
                if not isinstance(t, dict):
                    continue
                ticker_raw = (t.get("ticker") or "").strip()
                if not ticker_raw:
                    continue
                prefix = TICKER_TO_PREFIX.get(ticker_raw) or TICKER_TO_PREFIX.get(
                    ticker_raw.upper())
                if prefix is None or prefix not in candles:
                    n_skip["no_candle"] += 1
                    continue
                key = (str(eid), prefix)
                if key in seen:
                    n_skip["dup"] += 1
                    continue
                seen.add(key)
                tgt = compute_targets(candles[prefix], news_ts, horizons)
                if tgt is None:
                    n_skip["no_window"] += 1
                    continue
                rows.append({"id": str(eid), "datetime": news_ts.isoformat(),
                             "ticker": prefix, **tgt})
            if len(rows) and len(rows) % 20000 == 0:
                log.info("  ... %d target rows", len(rows))

    log.info("=== summary ===  rows=%d  skips=%s", len(rows), n_skip)
    if not rows:
        log.error("no targets built")
        return 1
    df = pd.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.output, index=False)
    log.info("Saved %s (%d × %d)", args.output, len(df), len(df.columns))
    return 0


if __name__ == "__main__":
    sys.exit(main())
