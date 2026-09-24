"""
Sprint 4 / Commit 4.0 — Диагностика sanity-расхождения
========================================================

Что делает:
  1. Запускает baseline на 5 проблемных тикерах (MTSS, SBER, VTBR, ROSN, GAZP)
  2. Сравнивает каждую сделку: наш exit vs Phase 2 exit
  3. Группирует расхождения по типу:
     - "sim_sl_p2_tp": мы дали SL, Phase 2 дал TP (главная гипотеза)
     - "sim_sl_p2_time": мы дали SL, Phase 2 дал time
     - и так далее
  4. Для топ-3 расхождений каждой группы — печатает бар-by-бар путь цены
  5. Проверяет: в момент Phase 2 TP-exit, был ли low ниже SL в том же баре?

Цель: подтвердить или опровергнуть гипотезу про worst-case assumption в check_tp_sl_hit.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import timedelta

from base import Trade, check_tp_sl_hit
from baseline import BaselineFixedTpSl
from prices_cache import PricesCache
from trades_loader import load_trades_best_combo


PROBLEM_TICKERS = {"MTSS", "SBER", "VTBR", "ROSN", "GAZP"}


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    print("=" * 70)
    print("  Диагностика: почему 5 тикеров расходятся с Phase 2")
    print("=" * 70)

    trades = load_trades_best_combo()
    problem_trades = [t for t in trades if t.ticker in PROBLEM_TICKERS]
    print(f"  Problem trades: {len(problem_trades)}")
    print(f"  Tickers: {sorted({t.ticker for t in problem_trades})}")

    cache = PricesCache()
    cache.warmup(list(PROBLEM_TICKERS))
    strategy = BaselineFixedTpSl()

    # ── Группируем по типу расхождения ──────────────────────────────────────
    print("\nСимулируем и группируем расхождения...")
    disagreement_types: dict[str, list] = {}
    same_reason_diff_price: list = []  # тот же reason, но разная цена

    for trade in problem_trades:
        bars = cache.get_bars(
            trade.ticker,
            trade.ts_open + timedelta(minutes=1),
            trade.max_simulation_ts,
        )
        if bars.empty:
            continue
        sim = strategy.simulate(trade, bars)

        if sim.exit_reason != trade.exit_reason_phase2:
            key = f"sim_{sim.exit_reason}_p2_{trade.exit_reason_phase2}"
            disagreement_types.setdefault(key, []).append((trade, sim, bars))
        elif abs(sim.exit_price - trade.exit_price_phase2) > 0.01:
            # Тот же reason, но разная цена (для time-stop)
            same_reason_diff_price.append((trade, sim, bars))

    # ── Распределение типов расхождений ────────────────────────────────────
    print("\n" + "=" * 70)
    print("  Типы расхождений на проблемных тикерах")
    print("=" * 70)
    total = sum(len(v) for v in disagreement_types.values())
    print(f"\n  Всего расхождений по exit_reason: {total}")
    for key, items in sorted(disagreement_types.items(), key=lambda x: -len(x[1])):
        print(f"  {key:<30s} {len(items):>6,}  ({100*len(items)/total:.1f}%)")

    print(f"\n  same reason, разная цена: {len(same_reason_diff_price):,}")

    # ── Если есть sim_sl_p2_tp — это главная гипотеза ───────────────────────
    if "sim_sl_p2_tp" in disagreement_types:
        items = disagreement_types["sim_sl_p2_tp"]
        print("\n" + "=" * 70)
        print(f"  Топ-5 примеров 'sim=SL, Phase2=TP' (n={len(items)})")
        print("  Гипотеза: в одном баре low<=SL И high>=TP, мы выбираем SL, Phase 2 — TP")
        print("=" * 70)

        # Берём 5 случайных (сортируем по разнице PnL)
        items_sorted = sorted(items, key=lambda x: x[0].net_pnl_rub_phase2 - x[1].realized_pnl, reverse=True)
        for i, (trade, sim, bars) in enumerate(items_sorted[:5]):
            print(f"\n  [{i+1}] {trade.ticker} {('long' if trade.side==1 else 'short')} "
                  f"@ {trade.entry:.4f}  size={trade.size_lots}  ts_open={trade.ts_open}")
            print(f"      sl_price={trade.sl_price:.4f}  tp_price={trade.tp_price:.4f}")
            print(f"      Phase 2: {trade.exit_reason_phase2} @ {trade.exit_price_phase2:.4f} "
                  f"({trade.ts_close_phase2})  pnl={trade.net_pnl_rub_phase2:+.2f}")
            print(f"      Sim:     {sim.exit_reason} @ {sim.exit_price:.4f} ({sim.ts_close})  pnl={sim.realized_pnl:+.2f}")

            # Покажем бары до момента закрытия (sim) — на каком баре произошёл "double-hit"
            close_bar_idx = bars.index.get_indexer([sim.ts_close], method="nearest")[0]
            start_idx = max(0, close_bar_idx - 1)
            end_idx = min(len(bars), close_bar_idx + 2)
            print(f"      bars around sim close ({close_bar_idx}):")
            print(f"        {'ts':<22s} {'open':>10s} {'high':>10s} {'low':>10s} {'close':>10s}")
            for ts, bar in bars.iloc[start_idx:end_idx].iterrows():
                # Подсветка: задеты ли TP/SL в этом баре?
                hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, trade.sl_price)
                marker = ""
                # Двойной hit?
                if trade.side == 1:
                    low_hits_sl = bar["low"] <= trade.sl_price
                    high_hits_tp = bar["high"] >= trade.tp_price
                else:
                    low_hits_sl = bar["high"] >= trade.sl_price
                    high_hits_tp = bar["low"] <= trade.tp_price
                if low_hits_sl and high_hits_tp:
                    marker = "  *** DOUBLE HIT (SL+TP в одном баре) ***"
                elif low_hits_sl:
                    marker = "  [SL hit]"
                elif high_hits_tp:
                    marker = "  [TP hit]"
                print(f"        {str(ts):<22s} {bar['open']:>10.4f} {bar['high']:>10.4f} "
                      f"{bar['low']:>10.4f} {bar['close']:>10.4f}{marker}")

    # ── Сколько в total на 5 проблемных тикерах "double-hit" баров? ─────────
    print("\n" + "=" * 70)
    print("  Подсчёт 'double-hit' баров на 5 проблемных тикерах")
    print("=" * 70)
    n_double_hit_trades = 0
    n_trades_total = 0
    for trade in problem_trades:
        bars = cache.get_bars(
            trade.ticker,
            trade.ts_open + timedelta(minutes=1),
            trade.max_simulation_ts,
        )
        if bars.empty:
            continue
        n_trades_total += 1
        # Проверка: есть ли в баре хоть один double-hit?
        for ts, bar in bars.iterrows():
            if ts >= trade.time_stop_ts:
                break
            if trade.side == 1:
                if bar["low"] <= trade.sl_price and bar["high"] >= trade.tp_price:
                    n_double_hit_trades += 1
                    break
            else:
                if bar["high"] >= trade.sl_price and bar["low"] <= trade.tp_price:
                    n_double_hit_trades += 1
                    break
    print(f"  Trades с хотя бы одним double-hit баром: "
          f"{n_double_hit_trades}/{n_trades_total} ({100*n_double_hit_trades/n_trades_total:.1f}%)")


if __name__ == "__main__":
    main()
