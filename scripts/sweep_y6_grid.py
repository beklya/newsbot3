r"""Sprint 6.3 Task 3 — 240-point parameter grid over cached walk-forward outcomes.

Requires scripts/walk_forward_y6_honest.py to have produced outcomes.parquet
(both-sides simulated trades, sweep-invariant).  Each grid point is then just a
vectorized selection + aggregation — the whole 240-combo sweep runs in seconds,
no model retraining.

Grid (per Sprint 6.3 brief):
    rr_threshold:  1.0, 1.25, 1.5, 1.75, 2.0                      (×5)
    min_mfe_pct:   0.0, 0.10, 0.20, 0.30  — PERCENT units,        (×4)
                   same units as DecisionSettings.min_mfe_pct and the
                   predicted_mfe_*_pct fields (0.10 == 0.10%).
    dir_conf:      0.45, 0.50, 0.55, 0.60                          (×4)
    whitelist:     full(19) / stocks_only(12) / stocks_futures(16) (×3)

Usage:
    python scripts/sweep_y6_grid.py \
        --outcomes data/walk_forward/y6_honest_costs_baseline/outcomes.parquet \
        --out-dir  data/walk_forward/y6_sweep
"""
from __future__ import annotations

import argparse
import itertools
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from scripts.walk_forward_y6_honest import select_trades, aggregate_trades  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sweep_y6")

GRID = {
    "rr_threshold": [1.0, 1.25, 1.5, 1.75, 2.0],
    "min_mfe_pct": [0.0, 0.10, 0.20, 0.30],
    "dir_conf": [0.45, 0.50, 0.55, 0.60],
    "whitelist": ["full", "stocks_only", "stocks_futures"],
}

# Sprint 6.3 finding: the base-grid optimum sits at the corner (rr=2.0,
# mfe=0.30, stocks_only) and improves monotonically toward it → extend past
# the corner to see whether economics ever cross zero (n_trades will shrink;
# report it honestly instead of hard-filtering).
EXTENDED_GRID = {
    "rr_threshold": [2.0, 2.5, 3.0, 4.0, 5.0],
    "min_mfe_pct": [0.30, 0.50, 0.75, 1.00, 1.50],
    "dir_conf": [0.50, 0.55, 0.60, 0.70],
    "whitelist": ["stocks_only", "stocks_futures"],
}

# Robustness / significance constraints from the brief
MIN_TRADES = 200
MAX_TRADES = 5000          # reported, not enforced as hard filter
MIN_FOLD_POSITIVE_SHARE = 0.70


def run_combo(outcomes: pd.DataFrame, n_folds_total: int, combo: dict) -> dict:
    trades = select_trades(outcomes, **combo)
    agg = aggregate_trades(trades, n_folds_total=n_folds_total)
    per_fold = agg.pop("per_fold", [])
    n_pos = agg.get("n_folds_positive_pnl", 0)
    row = {**combo, **{k: v for k, v in agg.items()
                       if k not in ("exit_reasons", "per_ticker")}}
    row["pct_folds_positive"] = (
        round(n_pos / n_folds_total * 100, 1) if n_folds_total else 0.0
    )
    row["robust"] = (
        row["n_trades"] >= MIN_TRADES
        and n_folds_total > 0
        and (n_pos / n_folds_total) >= MIN_FOLD_POSITIVE_SHARE
    )
    return row


def write_report(results: pd.DataFrame, out_dir: Path, n_folds_total: int,
                 suffix: str = "") -> None:
    lines = ["# Sprint 6.3 — Y6 walk-forward parameter sweep (honest Sber costs)", ""]
    lines.append(f"Grid points: {len(results)}; folds: {n_folds_total}; "
                 f"min_mfe units = percent (0.10 == 0.10%).")
    lines.append("")

    def fmt_table(df: pd.DataFrame, title: str) -> None:
        lines.append(f"## {title}")
        lines.append("")
        cols = ["rr_threshold", "min_mfe_pct", "dir_conf", "whitelist",
                "n_trades", "total_pnl_rub", "mean_pnl_per_trade_rub",
                "win_rate_pct", "sharpe_overall", "mean_fold_sharpe",
                "pct_folds_positive"]
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("|" + "---|" * len(cols))
        for _, r in df.iterrows():
            lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
        lines.append("")

    significant = results[results["n_trades"] >= MIN_TRADES]
    fmt_table(significant.nlargest(5, "sharpe_overall"),
              f"Top-5 by overall Sharpe (n_trades ≥ {MIN_TRADES})")
    fmt_table(significant.nlargest(5, "mean_pnl_per_trade_rub"),
              f"Top-5 by mean PnL per trade (n_trades ≥ {MIN_TRADES})")

    robust = results[results["robust"]]
    lines.append(f"## Robust configs (n ≥ {MIN_TRADES}, positive PnL in "
                 f"≥ {MIN_FOLD_POSITIVE_SHARE:.0%} folds): {len(robust)}")
    lines.append("")
    if len(robust):
        fmt_table(robust.nlargest(10, "sharpe_overall"), "Top-10 robust by Sharpe")
    else:
        lines.append("**NONE** — no configuration passes the robustness gate "
                     "with honest costs.")
        lines.append("")

    n_pos_sharpe = int((results["sharpe_overall"] > 0).sum())
    lines.append("## Aggregate")
    lines.append("")
    lines.append(f"- combos with positive overall Sharpe: {n_pos_sharpe}/{len(results)}")
    lines.append(f"- best overall Sharpe: {results['sharpe_overall'].max()}")
    lines.append(f"- best mean fold Sharpe: {results['mean_fold_sharpe'].max()}")
    if suffix:  # extended grid: show ALL combos incl. n<200 — corner hunting
        lines.append("")
        fmt_table(results.nlargest(15, "mean_pnl_per_trade_rub"),
                  "Top-15 by mean PnL per trade (NO n_trades filter — extended)")
    (out_dir / f"top10_report{suffix}.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outcomes", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" /
                            "y6_honest_costs_baseline" / "outcomes.parquet")
    ap.add_argument("--out-dir", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" / "y6_sweep")
    ap.add_argument("--extended", action="store_true",
                    help="Use EXTENDED_GRID (past the base-grid corner: rr→5, "
                         "mfe→1.5). Outputs get an _ext suffix.")
    args = ap.parse_args()

    outcomes = pd.read_parquet(args.outcomes)
    n_folds_total = int(outcomes["fold"].nunique())
    log.info("outcomes: %d rows, %d folds", len(outcomes), n_folds_total)

    args.out_dir.mkdir(parents=True, exist_ok=True)

    grid = EXTENDED_GRID if args.extended else GRID
    suffix = "_ext" if args.extended else ""
    keys = list(grid)
    combos = [dict(zip(keys, vals)) for vals in itertools.product(*grid.values())]
    log.info("grid: %d combos%s", len(combos), " (EXTENDED)" if args.extended else "")

    t0 = time.time()
    rows = []
    for i, combo in enumerate(combos, 1):
        rows.append(run_combo(outcomes, n_folds_total, combo))
        if i % 40 == 0:
            log.info("  ... %d/%d (%.1fs)", i, len(combos), time.time() - t0)

    results = pd.DataFrame(rows).sort_values("sharpe_overall", ascending=False)
    results.to_csv(args.out_dir / f"grid_results{suffix}.csv", index=False)
    log.info("Wrote %s (%.1fs total)", args.out_dir / f"grid_results{suffix}.csv",
             time.time() - t0)

    write_report(results, args.out_dir, n_folds_total, suffix=suffix)
    log.info("Wrote %s", args.out_dir / f"top10_report{suffix}.md")

    best = results.iloc[0]
    log.info("BEST: rr=%.2f mfe>=%.2f conf>=%.2f wl=%s -> sharpe=%.2f "
             "trades=%d pnl=%+.0f",
             best["rr_threshold"], best["min_mfe_pct"], best["dir_conf"],
             best["whitelist"], best["sharpe_overall"], best["n_trades"],
             best["total_pnl_rub"])
    (args.out_dir / f"best_combo{suffix}.json").write_text(
        json.dumps(best.to_dict(), indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
