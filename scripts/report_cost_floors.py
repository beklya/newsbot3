r"""Sprint 6.4 Phase 0 — break-even floors table (Gate 0 deliverable).

For each asset class × trading scenario, total RT cost % and the break-even
predicted move, for both exit geometries:
  - TP-geometry (move must reach TP = 0.7 × pred): floor = cost / 0.7
  - time-only exit (capture the whole move): floor = cost

Scenarios:
  intraday_single  — 1 trade this day (day turnover = 2×notional)
  intraday_active  — day turnover > 50M (max tier discount), e.g. ≥36 RT/day @700k
  t1_long_own      — hold 1 night, long on own funds (no funding)
  t1_long_leveraged— hold 1 night, long, 50% borrowed
  t1_short         — hold 1 night, short (always pays funding on full notional)

Usage: python scripts/report_cost_floors.py [--notional 700000]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.costs_sber import (  # noqa: E402
    round_trip_cost_rub, FUNDING_LONG_ANNUAL, FUNDING_SHORT_ANNUAL,
)

CLASS_REPR = {  # representative ticker per class
    "stock": "SBER", "futures": "MIX", "currency": "USDRUB", "metal": "GLDRUB",
}

SCENARIOS = [
    ("intraday_single", dict(holding_nights=0, side="BUY"), "own_turnover"),
    ("intraday_active_50M", dict(holding_nights=0, side="BUY"), 60_000_000.0),
    ("t1_long_own", dict(holding_nights=1, side="BUY", borrowed_share_long=0.0), "own_turnover"),
    ("t1_long_50pct_borrowed", dict(holding_nights=1, side="BUY", borrowed_share_long=0.5), "own_turnover"),
    ("t1_short", dict(holding_nights=1, side="SELL"), "own_turnover"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--notional", type=float, default=700_000.0,
                    help="Typical per-trade notional RUB (prod sizing median ~700k)")
    ap.add_argument("--out", type=Path,
                    default=PROJECT_ROOT / "data" / "walk_forward" / "sber_cost_floors_v2.md")
    args = ap.parse_args()
    n = args.notional

    lines = ["# Sber cost model v2.1 — break-even floors (Phase 0, Gate 0)", ""]
    lines.append(f"Notional per trade: {n:,.0f} ₽. Funding: long {FUNDING_LONG_ANNUAL:.1%}/год, "
                 f"short {FUNDING_SHORT_ANNUAL:.1%}/год, +0.0045%/перенос. "
                 "Slippage per-leg: entry/time market ×1, TP limit ×0, SL stop ×2.")
    lines.append("")
    lines.append("| class | scenario | brokerage ₽ | moex ₽ | slip tp/time/sl ₽ | funding ₽ | "
                 "total(time) ₽ | total(time) % | total(tp) % | floor TP-geom % | floor time-only % |")
    lines.append("|" + "---|" * 11)

    for cls, ticker in CLASS_REPR.items():
        for name, kw, turnover in SCENARIOS:
            day_turnover = 2 * n if turnover == "own_turnover" else turnover
            cb_time = round_trip_cost_rub(ticker, n, day_turnover_rub=day_turnover,
                                          exit_reason="time", **kw)
            cb_tp = round_trip_cost_rub(ticker, n, day_turnover_rub=day_turnover,
                                        exit_reason="tp", **kw)
            cb_sl = round_trip_cost_rub(ticker, n, day_turnover_rub=day_turnover,
                                        exit_reason="sl", **kw)
            pct_time = cb_time.total_rub / n * 100
            pct_tp = cb_tp.total_rub / n * 100
            lines.append(
                f"| {cls} | {name} | {cb_time.brokerage_rub:,.0f} | {cb_time.moex_rub:,.0f} | "
                f"{cb_tp.slippage_rub:,.0f}/{cb_time.slippage_rub:,.0f}/{cb_sl.slippage_rub:,.0f} | "
                f"{cb_time.funding_rub:,.0f} | "
                f"**{cb_time.total_rub:,.0f}** | {pct_time:.3f}% | {pct_tp:.3f}% | "
                f"{pct_tp / 0.7:.3f}% | {pct_time:.3f}% |"
            )
        lines.append("|  |  |  |  |  |  |  |  |  |  |  |")

    lines.append("")
    lines.append("Сравнение с v1 (Sprint 6.3): стоки 0.19% / валюты 0.50% / фьючерсы 0.08% RT.")
    out = "\n".join(lines)
    args.out.write_text(out, encoding="utf-8")
    print(out)
    print(f"\nSaved: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
