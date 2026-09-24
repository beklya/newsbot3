r"""Sprint 6.1 — derive MFE/MAE targets for the post-Y6 extended corpus.

For each (event, ticker) pair in `data/reenrich_phase2/full_corpus_70b.parquet`
(or any 70B enrichment parquet) where ticker is in PRODUCTION_WHITELIST and we
have prices coverage, compute MFE/MAE at horizons {30, 60} minutes.

Output schema mirrors Phase 2 `targets_mfe.parquet`:
    id, datetime, ticker, _entry_price, _entry_ts,
    mfe_long_30m, mae_long_30m, mfe_short_30m, mae_short_30m,
    mfe_long_60m, mae_long_60m, mfe_short_60m, mae_short_60m,
    ...

These targets are then merged with `features_mfe_70b_ext_v2.parquet` to train v7.

Usage:
    python scripts/derive_targets_for_y6_corpus.py \
        --enriched data/reenrich_phase2/full_corpus_70b.parquet \
        --output  data/reenrich_phase2/targets_mfe_v2.parquet
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("derive_targets")

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Phase 2 ticker -> prices CSV prefix (legacy names normalized)
TICKER_TO_PREFIX = {
    "SBER": "SBER", "GAZP": "GAZP", "LKOH": "LKOH",
    "YNDX": "YDEX", "YDEX": "YDEX", "ROSN": "ROSN", "NVTK": "NVTK",
    "VTBR": "VTBR", "GMKN": "GMKN", "MGNT": "MGNT",
    "MTSS": "MTSS", "TATN": "TATN", "PLZL": "PLZL",
    "Si": "SI", "SI": "SI",
    "MX": "MIX", "MIX": "MIX",
    "BR": "BR", "NG": "NG",
    "GOLD": "GLDRUB", "GLDRUB": "GLDRUB",
    "CNY": "CNY", "USDRUB": "USDRUB",
}

# Horizons that match Sprint 5 Predictor + Phase 2 best combo
HORIZONS_MIN = [30, 60]


def read_candles_csv(path: Path) -> pd.DataFrame | None:
    with open(path, encoding="utf-8") as fp:
        first = fp.readline().strip()
    sep = ";" if (";" in first and "," not in first) else ","
    df = pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False)
    df.columns = [c.strip("<>").lower() for c in df.columns]
    if "datetime" in df.columns:
        df["ts"] = pd.to_datetime(df["datetime"], errors="coerce")
    elif "date" in df.columns and "time" in df.columns:
        df["ts"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str).str.zfill(6),
            format="%Y%m%d %H%M%S", errors="coerce",
        )
    else:
        return None
    df = df.dropna(subset=["ts", "close"])
    if df.empty:
        return None
    if "vol" in df.columns:
        df = df.rename(columns={"vol": "volume"})
    if "volume" not in df.columns:
        df["volume"] = 0
    return df[["ts", "open", "high", "low", "close", "volume"]].drop_duplicates(
        subset=["ts"]).sort_values("ts").set_index("ts")


def load_all_candles(prices_dir: Path) -> dict[str, pd.DataFrame]:
    candles: dict[str, pd.DataFrame] = {}
    log.info("loading candles from %s", prices_dir)
    for ticker, prefix in TICKER_TO_PREFIX.items():
        if ticker in candles:
            continue
        path = prices_dir / f"prices_{prefix}.csv"
        if not path.exists():
            log.warning("  %s -> %s NOT FOUND", ticker, path.name)
            continue
        df = read_candles_csv(path)
        if df is None or df.empty:
            log.warning("  %s empty", ticker)
            continue
        candles[ticker] = df
        log.info("  %-7s %8d bars  %s -> %s",
                 ticker, len(df), df.index.min().date(), df.index.max().date())
    log.info("loaded %d ticker series", len(candles))
    return candles


def compute_mfe_mae(candles: pd.DataFrame, news_ts: pd.Timestamp,
                    horizons: list[int]) -> dict | None:
    next_min = (news_ts + pd.Timedelta(seconds=60)).floor("min")
    idx = candles.index.searchsorted(next_min)
    if idx >= len(candles):
        return None
    actual_ts = candles.index[idx]
    if (actual_ts - next_min).total_seconds() > 1800:
        return None  # gap > 30min — skip
    entry_price = float(candles["open"].iloc[idx])
    if entry_price <= 0:
        return None

    max_h = max(horizons)
    end_ts = actual_ts + pd.Timedelta(minutes=max_h)
    end_idx = candles.index.searchsorted(end_ts) + 1
    window = candles.iloc[idx:end_idx]
    if window.empty:
        return None

    out = {"_entry_price": entry_price, "_entry_ts": actual_ts.isoformat()}
    for H in horizons:
        h_end = actual_ts + pd.Timedelta(minutes=H)
        n = window.index.searchsorted(h_end, side="right")
        if n == 0:
            for tgt in ("mfe_long", "mae_long", "mfe_short", "mae_short"):
                out[f"{tgt}_{H}m"] = np.nan
            continue
        sub = window.iloc[:n]
        h_max = float(sub["high"].max())
        h_min = float(sub["low"].min())
        out[f"mfe_long_{H}m"] = round((h_max - entry_price) / entry_price * 100, 4)
        out[f"mae_long_{H}m"] = round((entry_price - h_min) / entry_price * 100, 4)
        out[f"mfe_short_{H}m"] = round((entry_price - h_min) / entry_price * 100, 4)
        out[f"mae_short_{H}m"] = round((h_max - entry_price) / entry_price * 100, 4)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--enriched", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "full_corpus_70b.parquet",
                    help="Aggregated 70B enrichment parquet")
    ap.add_argument("--prices-dir", type=Path,
                    default=Path(r"D:\quik_sber\newsbot\prices"))
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2.parquet")
    ap.add_argument("--horizons", default="30,60",
                    help="Comma-separated horizons in minutes")
    ap.add_argument("--whitelist", default=",".join([
        "SBER","GAZP","LKOH","YDEX","ROSN","NVTK","VTBR","GMKN","MGNT","MTSS","TATN","PLZL",
        "SI","MIX","BR","NG","GLDRUB","CNY","USDRUB",
    ]), help="Comma-separated canonical tickers")
    args = ap.parse_args()

    horizons = [int(h) for h in args.horizons.split(",")]
    whitelist = {t.strip().upper() for t in args.whitelist.split(",") if t.strip()}

    if not args.enriched.exists():
        log.error("enriched parquet not found: %s", args.enriched)
        return 1

    candles = load_all_candles(args.prices_dir)
    if not candles:
        log.error("no candle series loaded")
        return 1

    log.info("loading enriched parquet: %s", args.enriched)
    enr = pl.read_parquet(args.enriched)
    enr = enr.filter(pl.col("is_enriched") & pl.col("is_financial"))
    log.info("  enriched+financial rows: %d", enr.height)

    rows: list[dict] = []
    n_skip_no_ticker = 0
    n_skip_no_candle = 0
    n_skip_no_window = 0
    n_skip_off_whitelist = 0

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
            n_skip_no_ticker += 1
            continue
        for t in tickers:
            if not isinstance(t, dict):
                continue
            ticker_raw = (t.get("ticker") or "").strip()
            if not ticker_raw:
                continue
            ticker = ticker_raw.upper()
            # Normalize legacy
            if ticker in ("YNDX", "YDEX"): ticker = "YDEX"
            elif ticker in ("SI",): ticker = "SI"
            elif ticker in ("MX", "MIX"): ticker = "MIX"
            elif ticker == "GOLD": ticker = "GLDRUB"
            if ticker not in whitelist:
                n_skip_off_whitelist += 1
                continue
            cdf = candles.get(ticker)
            if cdf is None:
                n_skip_no_candle += 1
                continue
            mfe = compute_mfe_mae(cdf, news_ts, horizons)
            if mfe is None:
                n_skip_no_window += 1
                continue
            row = {
                "id": eid,
                "datetime": news_ts.isoformat(),
                "ticker": ticker,
            }
            row.update(mfe)
            rows.append(row)

    log.info("=== derivation summary ===")
    log.info("  rows produced:        %d", len(rows))
    log.info("  skipped no_ticker:    %d", n_skip_no_ticker)
    log.info("  skipped off_whitelist: %d", n_skip_off_whitelist)
    log.info("  skipped no_candle:    %d", n_skip_no_candle)
    log.info("  skipped no_window:    %d", n_skip_no_window)

    if not rows:
        log.error("no targets produced")
        return 2

    target_df = pl.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    target_df.write_parquet(args.output)
    log.info("wrote %s (%d rows × %d cols)", args.output, target_df.height, target_df.width)
    return 0


if __name__ == "__main__":
    sys.exit(main())
