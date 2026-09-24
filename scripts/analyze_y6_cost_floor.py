r"""Sprint 6.3 Task 4 — predicted-MFE distribution vs honest cost floor.

Question: is ANY part of the universe structurally able to out-earn its
round-trip cost?  Break-even condition for a TP exit:
    TP distance = 0.7 × pred_mfe ≥ RT_cost  →  pred_mfe ≥ RT_cost / 0.7
    stocks  ≥ 0.19% / 0.7 = 0.271%
    futures ≥ 0.08% / 0.7 = 0.114%
    currencies ≥ 0.50% / 0.7 = 0.714%

Reads outcomes.parquet from walk_forward_y6_honest.py (both-sides simulated,
chosen-side pred_mfe per row) and reports per asset class + per ticker:
  - pred_mfe quantiles (chosen side)
  - share of rows above break-even pred_mfe
  - realized GROSS pnl per trade vs cost per trade (the honest economics)
  - net pnl of the above-break-even subset (does selectivity even help?)

Usage:
    python scripts/analyze_y6_cost_floor.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.costs_sber import (  # noqa: E402
    STOCK_TICKERS, FUTURES_TICKERS, CURRENCY_TICKERS, rt_cost_pct,
)

TP_FRACTION = 0.7
CLASS_OF = {**{t: "stock" for t in STOCK_TICKERS},
            **{t: "futures" for t in FUTURES_TICKERS},
            **{t: "currency" for t in CURRENCY_TICKERS}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outcomes", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" /
                            "y6_honest_costs_baseline" / "outcomes.parquet")
    ap.add_argument("--out", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" / "y6_sweep" /
                            "cost_floor_analysis.md")
    args = ap.parse_args()

    df = pd.read_parquet(args.outcomes)
    df["asset_class"] = df["ticker"].map(CLASS_OF).fillna("unknown")
    df["rt_cost_pct"] = df["ticker"].map(rt_cost_pct) * 100  # in % units
    df["breakeven_mfe"] = df["rt_cost_pct"] / TP_FRACTION
    df["gross_pnl"] = df["realized_pnl"] + df["cost_rub"]
    df["above_floor"] = df["pred_mfe"] >= df["breakeven_mfe"]

    lines = ["# Sprint 6.3 — pred_MFE vs honest cost floor", ""]
    lines.append(f"Rows (both sides simulated): {len(df)}; "
                 f"TP_FRACTION={TP_FRACTION}; mfe units = percent.")
    lines.append("")

    # ---- per asset class ----
    lines.append("## Per asset class")
    lines.append("")
    lines.append("| class | n | RT cost % | break-even mfe % | "
                 "mfe p50 | mfe p90 | mfe p99 | % above floor | "
                 "gross/trade ₽ | cost/trade ₽ | net/trade ₽ | "
                 "net/trade (above-floor subset) ₽ | n above floor |")
    lines.append("|" + "---|" * 13)
    for cls, g in df.groupby("asset_class"):
        sub = g[g["above_floor"]]
        lines.append(
            f"| {cls} | {len(g)} | {g['rt_cost_pct'].iloc[0]:.3f} | "
            f"{g['breakeven_mfe'].iloc[0]:.3f} | "
            f"{g['pred_mfe'].quantile(0.5):.3f} | "
            f"{g['pred_mfe'].quantile(0.9):.3f} | "
            f"{g['pred_mfe'].quantile(0.99):.3f} | "
            f"{g['above_floor'].mean() * 100:.2f}% | "
            f"{g['gross_pnl'].mean():+.0f} | {g['cost_rub'].mean():.0f} | "
            f"{g['realized_pnl'].mean():+.0f} | "
            f"{sub['realized_pnl'].mean():+.0f} | {len(sub)} |"
        )
    lines.append("")

    # ---- per ticker ----
    lines.append("## Per ticker")
    lines.append("")
    lines.append("| ticker | class | n | mfe p50 | mfe p90 | % above floor | "
                 "gross/trade ₽ | cost/trade ₽ | net/trade ₽ | "
                 "net/trade above-floor ₽ | n above floor |")
    lines.append("|" + "---|" * 11)
    rows_t = []
    for tk, g in df.groupby("ticker"):
        sub = g[g["above_floor"]]
        rows_t.append({
            "ticker": tk, "class": CLASS_OF.get(tk, "?"), "n": len(g),
            "p50": g["pred_mfe"].quantile(0.5),
            "p90": g["pred_mfe"].quantile(0.9),
            "above": g["above_floor"].mean() * 100,
            "gross": g["gross_pnl"].mean(), "cost": g["cost_rub"].mean(),
            "net": g["realized_pnl"].mean(),
            "net_af": sub["realized_pnl"].mean() if len(sub) else np.nan,
            "n_af": len(sub),
        })
    for r in sorted(rows_t, key=lambda x: -x["net"]):
        lines.append(
            f"| {r['ticker']} | {r['class']} | {r['n']} | {r['p50']:.3f} | "
            f"{r['p90']:.3f} | {r['above']:.2f}% | {r['gross']:+.0f} | "
            f"{r['cost']:.0f} | {r['net']:+.0f} | "
            f"{(f'{r['net_af']:+.0f}' if not np.isnan(r['net_af']) else 'n/a')} | "
            f"{r['n_af']} |"
        )
    lines.append("")

    # ---- realized-MFE reality check: even at perfect prediction, is the
    # realized move big enough? Use gross_pnl of TP exits as proxy. ----
    lines.append("## Exit-reason economics (all rows)")
    lines.append("")
    lines.append("| exit | n | gross/trade ₽ | cost/trade ₽ | net/trade ₽ |")
    lines.append("|" + "---|" * 5)
    for ex, g in df.groupby("exit_reason"):
        lines.append(f"| {ex} | {len(g)} | {g['gross_pnl'].mean():+.0f} | "
                     f"{g['cost_rub'].mean():.0f} | {g['realized_pnl'].mean():+.0f} |")
    lines.append("")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nSaved: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
