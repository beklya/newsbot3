r"""Sprint 6.3 — how much of the news move happens BEFORE our entry?

Hypothesis (the author, 2026-06-10): with 1-minute bars we enter at the open of
the next minute (~25-85s after the news) and only catch the tail of the move.
If true, the price has already jumped in our direction by entry time.

Test on EXISTING 1m data, no tick data needed:
    pre_close = close of the last bar STRICTLY BEFORE the news minute
                (uncontaminated price ~news-60..-1s)
    jump_pct  = side_sign × (entry − pre_close) / pre_close × 100

Read on the result:
    jump >> 0 on selected trades  → market reacts within the first minute and
                                    we are late → speed (tick feed / faster
                                    pipeline) is worth investigating
    jump ≈ 0                      → no fast reaction to catch; second-level
                                    candles would change nothing

Also prints enrich latency quantiles from the Y6 corpus — the live-pipeline
budget that bounds any "be faster" plan.

Usage:
    python scripts/analyze_entry_jump.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from prices_cache import PricesCache  # noqa: E402
from scripts.costs_sber import WHITELIST_STRATEGIES  # noqa: E402
from scripts.walk_forward_y6_honest import select_trades  # noqa: E402

OUTCOMES = (PROJECT_ROOT / "data" / "walk_forward" / "y6_honest_costs_baseline"
            / "outcomes.parquet")
CORPUS = PROJECT_ROOT / "data" / "reenrich_phase2" / "y6_corpus_70b.parquet"

CLASS_OF = {**{t: "stock" for t in WHITELIST_STRATEGIES["stocks_only"]},
            **{t: "futures" for t in
               set(WHITELIST_STRATEGIES["stocks_futures"])
               - set(WHITELIST_STRATEGIES["stocks_only"])}}


def add_jump(df: pd.DataFrame, cache: PricesCache) -> pd.DataFrame:
    """Compute signed jump_pct per row (vectorized per ticker)."""
    out = []
    for ticker, g in df.groupby("ticker"):
        try:
            bars = cache.get_bars(
                ticker,
                pd.Timestamp(g["news_ts"].min()) - pd.Timedelta(minutes=15),
                pd.Timestamp(g["news_ts"].max()) + pd.Timedelta(minutes=2),
            )
        except FileNotFoundError:
            continue
        if bars is None or bars.empty:
            continue
        g = g.copy()
        news_min = pd.to_datetime(g["news_ts"]).dt.floor("min")
        # index of last bar strictly before the news minute
        pos = bars.index.searchsorted(news_min.values) - 1
        valid = pos >= 0
        g = g[valid]
        pos = pos[valid]
        g["pre_close"] = bars["close"].values[pos]
        g["pre_ts"] = bars.index.values[pos]
        # stale pre-bar (gap > 10 min) → price could be hours old; drop
        fresh = (pd.to_datetime(g["news_ts"]) - pd.to_datetime(g["pre_ts"])
                 ) <= pd.Timedelta(minutes=10)
        g = g[fresh]
        side_sign = np.where(g["side"] == "BUY", 1.0, -1.0)
        g["jump_pct"] = side_sign * (g["entry"] - g["pre_close"]) / g["pre_close"] * 100
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def report(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        print(f"\n--- {title}: EMPTY ---")
        return
    j = df["jump_pct"]
    print(f"\n--- {title} (n={len(df)}) ---")
    print(f"  mean {j.mean():+.4f}%  median {j.median():+.4f}%  "
          f"p25 {j.quantile(0.25):+.4f}%  p75 {j.quantile(0.75):+.4f}%  "
          f"p90 {j.quantile(0.90):+.4f}%")
    print(f"  share |jump| > 0.05%: {(j.abs() > 0.05).mean() * 100:.1f}%   "
          f"share jump > +0.05% (already moved our way): {(j > 0.05).mean() * 100:.1f}%")
    if "exit_reason" in df.columns:
        for ex, ge in df.groupby("exit_reason"):
            print(f"    exit={ex:<5} n={len(ge):>7}  mean jump {ge['jump_pct'].mean():+.4f}%")
    if "asset_class" in df.columns:
        for cls, gc in df.groupby("asset_class"):
            print(f"    class={cls:<8} n={len(gc):>7}  mean jump {gc['jump_pct'].mean():+.4f}%")


def main() -> int:
    df = pd.read_parquet(OUTCOMES)
    # the VTBR forensic showed duplicated rows — dedup for honest stats
    df = df.drop_duplicates(subset=["news_ts", "ticker", "side"])
    df["asset_class"] = df["ticker"].map(CLASS_OF).fillna("currency")

    cache = PricesCache()
    cache.warmup()

    df = add_jump(df, cache)
    report(df, "ALL candidate rows (both sides, dedup'd)")

    base = select_trades(df, rr_threshold=1.0, min_mfe_pct=0.0,
                         dir_conf=0.50, whitelist="full")
    report(base, "BASELINE-selected trades (rr>=1.0, conf>=0.5)")

    corner = select_trades(df, rr_threshold=2.0, min_mfe_pct=0.30,
                           dir_conf=0.55, whitelist="stocks_only")
    report(corner, "CORNER config trades (rr>=2.0, mfe>=0.3, stocks_only)")

    # --- live latency budget: enrichment step from the Y6 corpus ---
    try:
        lat = pd.read_parquet(CORPUS, columns=["enrich_latency_ms", "is_financial"])
        lat = lat["enrich_latency_ms"].dropna()
        lat = lat[lat > 0]
        print(f"\n--- Enrich latency (Y6 corpus, n={len(lat)}) ---")
        print(f"  p50 {lat.quantile(0.5) / 1000:.1f}s  p90 {lat.quantile(0.9) / 1000:.1f}s  "
              f"p99 {lat.quantile(0.99) / 1000:.1f}s")
        print("  (+ telegram posting lag + predictor/decision/bridge hops on top)")
    except Exception as e:  # column may be absent in older corpus builds
        print(f"\n(enrich latency stats unavailable: {e})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
