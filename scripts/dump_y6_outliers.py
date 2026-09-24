r"""Sprint 6.3 — forensic check of the extended-sweep "positive" configs.

Hypothesis: the entire positive PnL of the extended grid is one 8-trade
cluster (rr≥5, single fold, 100% win, +128k).  If those 8 trades sit inside
the headline config (rr=2.0, mfe≥1.0, conf≥0.55, stocks_futures, +121.8k),
the rest of that config is net-negative and the "Sharpe +1.93" is one news
day, not a strategy.

Usage:
    python scripts/dump_y6_outliers.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from scripts.walk_forward_y6_honest import select_trades  # noqa: E402

OUTCOMES = (PROJECT_ROOT / "data" / "walk_forward" / "y6_honest_costs_baseline"
            / "outcomes.parquet")

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 50)

COLS = ["fold", "news_ts", "ticker", "side", "entry", "exit_price",
        "exit_reason", "quantity", "notional_rub", "cost_rub", "realized_pnl",
        "pred_mfe", "pred_mae", "rr_long", "rr_short", "llm_dir", "confidence"]


def main() -> int:
    outcomes = pd.read_parquet(OUTCOMES)

    # --- B: the 8-trade rr>=5 cluster ---
    cluster = select_trades(outcomes, rr_threshold=5.0, min_mfe_pct=0.30,
                            dir_conf=0.55, whitelist="stocks_only")
    print("=" * 100)
    print(f"CLUSTER rr>=5.0 conf>=0.55 stocks_only: n={len(cluster)} "
          f"pnl={cluster['realized_pnl'].sum():+,.0f}")
    print(cluster[COLS].to_string(index=False))

    # --- A: headline config of the extended sweep ---
    best = select_trades(outcomes, rr_threshold=2.0, min_mfe_pct=1.00,
                         dir_conf=0.55, whitelist="stocks_futures")
    key = ["news_ts", "ticker", "side"]
    in_cluster = best.merge(cluster[key].drop_duplicates(), on=key, how="inner")
    rest = best.merge(cluster[key].drop_duplicates(), on=key, how="left",
                      indicator=True).query("_merge == 'left_only'")
    print()
    print("=" * 100)
    print(f"HEADLINE rr>=2.0 mfe>=1.0 conf>=0.55 stocks_futures: n={len(best)} "
          f"pnl={best['realized_pnl'].sum():+,.0f}")
    print(f"  overlap with cluster: n={len(in_cluster)} "
          f"pnl={in_cluster['realized_pnl'].sum():+,.0f}")
    print(f"  WITHOUT cluster:      n={len(rest)} "
          f"pnl={rest['realized_pnl'].sum():+,.0f}")

    # --- per-fold PnL concentration of the headline config ---
    by_fold = best.groupby("fold")["realized_pnl"].agg(["sum", "count"])
    by_fold = by_fold.sort_values("sum", ascending=False)
    total = best["realized_pnl"].sum()
    print()
    print("Headline config per-fold PnL (top 8):")
    for fold, r in by_fold.head(8).iterrows():
        print(f"  fold {fold:>2}: {r['sum']:+12,.0f} ₽  ({int(r['count'])} trades)"
              f"  {'<- ' + format(r['sum'] / total * 100, '.0f') + '% of total' if total else ''}")

    # --- same-minute duplicate news check inside cluster ---
    print()
    dup = cluster.assign(minute=pd.to_datetime(cluster["news_ts"]).dt.floor("min"))
    g = dup.groupby(["minute", "ticker", "side"]).size()
    multi = g[g > 1]
    print(f"Cluster same-minute (ticker,side) duplicates: {len(multi)}")
    if len(multi):
        print(multi.to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
