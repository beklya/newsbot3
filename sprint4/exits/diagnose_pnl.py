"""
Sprint 4 / Commit 4.0 — Диагностика PnL формулы
=================================================

Гипотеза: расхождение -85% не объясняется exit-логикой (double-hit < 0.1%).
Значит проблема в одной из:
  1. Position sizing — size_lots интерпретируется иначе
  2. Currency conversion — pseudo-RUB для BR/NG/GLDRUB
  3. cost_rub — учитывается не там
  4. Что-то ещё в формуле

Проверяем напрямую: берём сделки, где наш exit_reason СОВПАДАЕТ с Phase 2,
и exit_price тоже совпадает. Если PnL ещё расходится — проблема ТОЛЬКО в формуле.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from baseline import BaselineFixedTpSl
from prices_cache import PricesCache
from trades_loader import load_trades_best_combo


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    print("=" * 70)
    print("  PnL formula diagnostic")
    print("=" * 70)

    trades = load_trades_best_combo()
    cache = PricesCache()
    cache.warmup(sorted({t.ticker for t in trades}))
    strategy = BaselineFixedTpSl()

    # Прогоняем все, ищем сделки где exit_reason И exit_price совпали
    matched_same_price = []
    matched_diff_price = []
    diff_reason = []
    no_bars = 0

    for trade in trades:
        bars = cache.get_bars(
            trade.ticker,
            trade.ts_open + timedelta(minutes=1),
            trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
        )
        if bars.empty:
            no_bars += 1
            continue
        sim = strategy.simulate(trade, bars)

        if sim.exit_reason != trade.exit_reason_phase2:
            diff_reason.append((trade, sim))
            continue

        # Reason тот же — сравниваем exit_price
        if abs(sim.exit_price - trade.exit_price_phase2) < 0.01:
            matched_same_price.append((trade, sim))
        else:
            matched_diff_price.append((trade, sim))

    print(f"\n  Total trades:                {len(trades)}")
    print(f"  no_bars:                     {no_bars}")
    print(f"  diff exit_reason:            {len(diff_reason)}")
    print(f"  same reason, diff price:     {len(matched_diff_price)}")
    print(f"  same reason, SAME price:     {len(matched_same_price)}")

    if not matched_same_price:
        print("\n  [ERR] Нет совпадающих сделок — что-то очень не так")
        return

    # На совпадающих по exit_price — сравниваем PnL
    # Если PnL тоже совпадает → формула правильная, и проблема только в расхождении exits
    # Если PnL расходится → проблема в формуле
    print("\n" + "=" * 70)
    print("  PnL на сделках с одинаковым exit_price")
    print("=" * 70)

    sim_pnl_sum = sum(s.realized_pnl for _, s in matched_same_price)
    p2_pnl_sum = sum(t.net_pnl_rub_phase2 for t, _ in matched_same_price)
    diff = sim_pnl_sum - p2_pnl_sum
    print(f"  Sim PnL:     {sim_pnl_sum:>+15,.2f}")
    print(f"  Phase 2 PnL: {p2_pnl_sum:>+15,.2f}")
    print(f"  Diff:        {diff:>+15,.2f}  ({100*diff/abs(p2_pnl_sum):+.2f}%)")

    # Покажем 5 примеров с самым большим расхождением PnL
    matched_same_price.sort(key=lambda x: abs(x[1].realized_pnl - x[0].net_pnl_rub_phase2), reverse=True)

    print("\n  Top-5 trades with largest PnL diff (same exit_price!):")
    print(f"  {'ticker':<8s} {'side':<5s} {'size':>6s} {'entry':>10s} {'exit':>10s} "
          f"{'cost':>8s} {'sim_pnl':>12s} {'p2_pnl':>12s} {'diff':>12s} ratio")
    for trade, sim in matched_same_price[:5]:
        ratio = trade.net_pnl_rub_phase2 / sim.realized_pnl if sim.realized_pnl != 0 else 0
        print(f"  {trade.ticker:<8s} {trade.side:>+5d} {trade.size_lots:>6d} "
              f"{trade.entry:>10.4f} {sim.exit_price:>10.4f} {trade.cost_rub:>8.2f} "
              f"{sim.realized_pnl:>+12.2f} {trade.net_pnl_rub_phase2:>+12.2f} "
              f"{trade.net_pnl_rub_phase2 - sim.realized_pnl:>+12.2f} {ratio:>6.4f}")

    # Расчёт ratio для каждой сделки — есть ли постоянное соотношение?
    ratios = []
    for trade, sim in matched_same_price:
        if abs(sim.realized_pnl) > 1:  # избегаем деления на маленькие числа
            ratios.append((trade.ticker, trade.net_pnl_rub_phase2 / sim.realized_pnl))

    if ratios:
        from collections import defaultdict
        per_ticker_ratios = defaultdict(list)
        for ticker, r in ratios:
            per_ticker_ratios[ticker].append(r)

        print("\n  Median ratio (Phase2 / Sim) per ticker:")
        for tk in sorted(per_ticker_ratios.keys()):
            rs = sorted(per_ticker_ratios[tk])
            median = rs[len(rs) // 2]
            print(f"    {tk:<8s} n={len(rs):>4d}  median ratio = {median:>8.4f}")

    # Один конкретный пример с детальной разборкой
    print("\n" + "=" * 70)
    print("  Детальный пример (первая сделка с одинаковым exit)")
    print("=" * 70)
    trade, sim = matched_same_price[0]
    print(f"  Ticker:        {trade.ticker}")
    print(f"  Side:          {trade.side} ({'long' if trade.side==1 else 'short'})")
    print(f"  Size lots:     {trade.size_lots}")
    print(f"  Entry:         {trade.entry}")
    print(f"  Exit price:    {sim.exit_price}  (== Phase 2 {trade.exit_price_phase2})")
    print(f"  Exit reason:   {sim.exit_reason}  (== Phase 2 {trade.exit_reason_phase2})")
    print(f"  cost_rub:      {trade.cost_rub}")
    print()
    print(f"  Наша формула:")
    print(f"    gross = side × (exit - entry) × size_lots")
    print(f"          = {trade.side} × ({sim.exit_price} - {trade.entry}) × {trade.size_lots}")
    gross_ours = trade.side * (sim.exit_price - trade.entry) * trade.size_lots
    print(f"          = {gross_ours:.4f}")
    print(f"    net   = gross - cost_rub = {gross_ours - trade.cost_rub:.4f}")
    print(f"    sim_realized_pnl =                              {sim.realized_pnl:.4f}")
    print(f"    phase2_net_pnl_rub =                            {trade.net_pnl_rub_phase2:.4f}")
    print(f"    ratio (Phase 2 / Ours) = {trade.net_pnl_rub_phase2 / sim.realized_pnl:.6f}")


if __name__ == "__main__":
    main()
