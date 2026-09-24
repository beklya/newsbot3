r"""Sprint 6.3 — how broad is the 4x row duplication seen in the VTBR cluster?

Checks outcomes / features / targets for duplicate keys.
Usage: python scripts/check_y6_duplication.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

o = pd.read_parquet(PROJECT_ROOT / "data/walk_forward/y6_honest_costs_baseline/outcomes.parquet")
print(f"outcomes rows:                 {len(o)}")
g = o.groupby(["news_ts", "ticker", "side"]).size()
print(f"unique (news_ts,ticker,side):  {len(g)}")
print(f"groups with >1 row:            {(g > 1).sum()}  (max group size {g.max()})")
print("group-size distribution:")
print(g.value_counts().sort_index().to_string())
print()

f = pd.read_parquet(PROJECT_ROOT / "data/reenrich_phase2/features_mfe_y6_70b_ext.parquet",
                    columns=["_id", "_ticker", "_datetime"])
print(f"features rows:                 {len(f)}")
print(f"unique (_id,_ticker):          {len(f[['_id', '_ticker']].drop_duplicates())}")
print(f"unique (_datetime,_ticker):    {len(f[['_datetime', '_ticker']].drop_duplicates())}")
print()

t = pd.read_parquet(PROJECT_ROOT / "data/reenrich_phase2/targets_mfe_y6.parquet")
print(f"targets rows:                  {len(t)}")
print(f"unique (id,ticker):            {len(t[['id', 'ticker']].drop_duplicates())}")

# sample of a duplicated group with different ids? (channel cross-posts vs true dups)
dup_keys = g[g > 1].head(3).index
for k in dup_keys:
    sub = o[(o["news_ts"] == k[0]) & (o["ticker"] == k[1]) & (o["side"] == k[2])]
    if "event_id" in sub.columns:
        print(f"\nsample dup group {k}: event_ids = {sub['event_id'].tolist()}")
sys.exit(0)
