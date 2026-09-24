r"""Sprint 6.4 Phase A — Gate A report: gross vs cost per horizon × asset class.

Loads data/walk_forward/y6_honest_<horizon>/ artifacts for every completed
horizon run and tabulates the baseline-selection economics.

Gate A: a horizon exists where gross/trade ≥ 2× cost/trade on ≥1 asset class.

Usage: python scripts/report_phase_a.py [--horizons 60m,120m,240m,eod,t1]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.costs_sber import ASSET_CLASS, apply_costs_v2  # noqa: E402

EQUITY_RUB = 500_000.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizons", default="60m,120m,240m,eod,t1")
    ap.add_argument("--out", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" / "phase_a_gate_report.md")
    args = ap.parse_args()

    lines = ["# Sprint 6.4 Phase A — Gate A report (baseline rr=1.0/conf=0.5/full)", ""]
    lines.append("| horizon | class | n | gross/trade ₽ | cost/trade ₽ | gross/cost | "
                 "net/trade ₽ | win% | exit tp/sl/time % |")
    lines.append("|" + "---|" * 9)
    overall_rows = []
    gate_pass: list[str] = []

    for h in [x.strip() for x in args.horizons.split(",") if x.strip()]:
        d = PROJECT_ROOT / "data" / "walk_forward" / f"y6_honest_{h}"
        trades_path = d / "all_trades.parquet"
        summary_path = d / "summary.json"
        if not trades_path.exists():
            lines.append(f"| {h} | — | run missing | | | | | | |")
            continue
        t = apply_costs_v2(pd.read_parquet(trades_path), equity_rub=EQUITY_RUB)
        t["cls"] = t["ticker"].map(ASSET_CLASS).fillna("?")
        for cls, g in t.groupby("cls"):
            gross = g["gross_pnl"].mean()
            cost = g["cost_total_rub"].mean()
            ratio = gross / cost if cost > 0 else float("nan")
            ex = g["exit_reason"].value_counts(normalize=True) * 100
            lines.append(
                f"| {h} | {cls} | {len(g)} | {gross:+.0f} | {cost:.0f} | "
                f"**{ratio:+.2f}** | {g['net_pnl'].mean():+.0f} | "
                f"{(g['net_pnl'] > 0).mean() * 100:.1f} | "
                f"{ex.get('tp', 0):.0f}/{ex.get('sl', 0):.0f}/{ex.get('time', 0):.0f} |"
            )
            if ratio >= 2.0 and len(g) >= 50:
                gate_pass.append(f"{h}/{cls} (ratio {ratio:.2f}, n={len(g)})")
        s = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
        overall_rows.append({
            "horizon": h, "n": s.get("n_trades"),
            "gross": s.get("total_gross_rub"), "cost": s.get("total_cost_rub"),
            "net": s.get("total_pnl_rub"), "sharpe": s.get("sharpe_overall"),
            "folds_pos": f"{s.get('n_folds_positive_pnl')}/{s.get('n_folds_total')}",
        })

    lines.append("")
    lines.append("## Overall per horizon (baseline combo)")
    lines.append("")
    lines.append("| horizon | n | gross ₽ | cost ₽ | net ₽ | Sharpe | folds+ |")
    lines.append("|" + "---|" * 7)
    for r in overall_rows:
        lines.append(f"| {r['horizon']} | {r['n']} | {r['gross']:+,.0f} | {r['cost']:,.0f} | "
                     f"{r['net']:+,.0f} | {r['sharpe']} | {r['folds_pos']} |"
                     if r["n"] is not None else f"| {r['horizon']} | — | | | | | |")

    lines.append("")
    if gate_pass:
        lines.append(f"## ✅ GATE A PASSED: {'; '.join(gate_pass)}")
    else:
        lines.append("## ❌ GATE A NOT PASSED — no horizon×class with gross ≥ 2×cost")

    out = "\n".join(lines)
    args.out.write_text(out, encoding="utf-8")
    print(out)
    print(f"\nSaved: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
