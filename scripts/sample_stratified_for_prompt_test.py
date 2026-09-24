r"""
scripts/sample_stratified_for_prompt_test.py
=============================================
Берёт stratified-by-year sample из full_70k_input.parquet для prompt testing.

Use case: deepinfra_runner --limit N берёт FIRST N events (которые могут быть
одного года). Для адекватной distribution comparison нужен sample
распределённый по всему диапазону 2022-2026.

Usage:
  python scripts/sample_stratified_for_prompt_test.py --n 500 --seed 42 \
      --output data/reenrich_phase2/sample500_stratified.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "reenrich_phase2" / "full_70k_input.parquet"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--output", required=True)
    ap.add_argument("--n", type=int, default=500, help="Total sample size")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    df = pd.read_parquet(args.input)
    print(f"Input: {len(df)} rows × {len(df.columns)} cols")

    df["_dt"] = pd.to_datetime(df["datetime_msk"])
    df["_year"] = df["_dt"].dt.year
    years = sorted(df["_year"].unique())
    print(f"Years: {years}")
    print(df["_year"].value_counts().sort_index())

    # Stratify proportional to year — but cap each year at n/len(years) minimum
    # Simple: pick n/len(years) from each year
    per_year = args.n // len(years)
    print(f"\nSampling {per_year} per year × {len(years)} years")

    parts = []
    for y in years:
        sub = df[df["_year"] == y]
        take = min(per_year, len(sub))
        parts.append(sub.sample(n=take, random_state=args.seed))
    sample = pd.concat(parts, ignore_index=True)
    # Sort by date
    sample = sample.sort_values("_dt").reset_index(drop=True)
    # Drop temp cols
    sample = sample.drop(columns=["_dt", "_year"])
    print(f"\nSample: {len(sample)} rows")
    print(f"Date range: {pd.to_datetime(sample['datetime_msk']).min()} → {pd.to_datetime(sample['datetime_msk']).max()}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    sample.to_parquet(args.output, index=False)
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
