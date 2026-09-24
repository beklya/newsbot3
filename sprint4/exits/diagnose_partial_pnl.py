"""
Узкая диагностика partial PnL формулы.

Берём одну прибыльную сделку из baseline (где TP hit, partial должен был
сработать на 1R до TP).

Считаем вручную, что должно получиться, и сравниваем с тем что выдал partial.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from base import Trade, compute_pnl_pseudo_rub
from baseline import BaselineFixedTpSl
from partial_at_levels import Partial50_50AtLevels
from prices_cache import PricesCache
from trades_loader import load_trades_best_combo


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    trades = load_trades_best_combo()
    cache = PricesCache()
    cache.warmup(sorted({t.ticker for t in trades}))

    baseline = BaselineFixedTpSl()
    partial = Partial50_50AtLevels()

    # Берём первые 10 сделок где baseline даёт TP
    tp_trades = []
    for trade in trades:
        bars = cache.get_bars(
            trade.ticker,
            trade.ts_open,
            trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
        )
        if bars.empty:
            continue
        r = baseline.simulate(trade, bars)
        if r.exit_reason == "tp" and trade.net_pnl_rub_phase2 > 1000:
            tp_trades.append((trade, r))
            if len(tp_trades) >= 5:
                break

    print("=" * 70)
    print(f"  Detailed partial PnL diagnostic on {len(tp_trades)} TP trades")
    print("=" * 70)

    for i, (trade, baseline_r) in enumerate(tp_trades):
        bars = cache.get_bars(
            trade.ticker, trade.ts_open,
            trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
        )
        partial_r = partial.simulate(trade, bars)

        print(f"\n  [{i+1}] {trade.ticker} {'long' if trade.side==1 else 'short'} "
              f"@ {trade.entry}  size={trade.size_lots} lot_size={trade.lot_size} cost={trade.cost_rub}")
        print(f"      sl={trade.sl_price:.4f}  tp={trade.tp_price:.4f}  1R={trade.one_r_level:.4f}")
        print(f"      Baseline: {baseline_r.exit_reason} @ {baseline_r.exit_price}  pnl={baseline_r.realized_pnl:.2f}")
        print(f"      Partial:  {partial_r.exit_reason} @ {partial_r.exit_price}  pnl={partial_r.realized_pnl:.2f}")

        if not partial_r.partial_exits:
            print(f"      (partial не активировался — выход как baseline)")
            continue

        # Ручной расчёт
        p1 = partial_r.partial_exits[0]
        p2 = partial_r.partial_exits[1]
        print(f"      Partial parts:")
        print(f"        part 1: {p1['price']} (weight {p1['weight']})")
        print(f"        part 2: {p2['price']} (weight {p2['weight']})")

        # Ручной расчёт
        manual_gross_1 = trade.side * (p1["price"] - trade.entry) * trade.lot_size * trade.size_lots * p1["weight"]
        manual_gross_2 = trade.side * (p2["price"] - trade.entry) * trade.lot_size * trade.size_lots * p2["weight"]
        manual_total = manual_gross_1 + manual_gross_2 - trade.cost_rub * 1.5
        print(f"      Manual: gross_1={manual_gross_1:.2f}  gross_2={manual_gross_2:.2f}  total={manual_total:.2f}")

        # Текущая формула в коде partial
        from base import compute_pnl_pseudo_rub
        code_gross_1 = compute_pnl_pseudo_rub(trade, p1["price"]) * p1["weight"]
        code_gross_2 = compute_pnl_pseudo_rub(trade, p2["price"]) * p2["weight"]
        code_total = code_gross_1 + code_gross_2 - trade.cost_rub - trade.cost_rub * 0.5
        print(f"      Code:   gross_1={code_gross_1:.2f}  gross_2={code_gross_2:.2f}  total={code_total:.2f}")

        # Сходится ли manual с code?
        diff = abs(manual_total - code_total)
        print(f"      Manual vs Code: diff = {diff:.4f}")
        # Сходится ли code с realized_pnl?
        diff2 = abs(code_total - partial_r.realized_pnl)
        print(f"      Code vs realized_pnl: diff = {diff2:.4f}")


if __name__ == "__main__":
    main()
