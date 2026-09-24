"""
Sprint 4 / Commit 4.0 - Window boundaries diagnostic

Гипотеза: остаток 3.33% diff объясняется boundary handling:
  1. Phase 2 включает entry-минуту (мы исключаем)
  2. Phase 2 включает бар сразу после time-stop (мы исключаем)

Проверка: на нескольких конкретных сделках LKOH/MTSS/SBER (где большой diff)
печатаем bar-by-bar путь и видим, на какой минуте Phase 2 закрыл и на какой мы.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from base import Trade
from baseline import BaselineFixedTpSl
from prices_cache import PricesCache
from trades_loader import load_trades_best_combo


# Тикеры с самым большим относительным расхождением
PROBLEM_TICKERS = {"LKOH", "MTSS", "GMKN", "PLZL", "NVTK", "SBER", "TATN"}


def main() -> None:
    logging.basicConfig(level=logging.WARNING)

    trades = load_trades_best_combo()
    cache = PricesCache()
    cache.warmup(sorted({t.ticker for t in trades}))
    strategy = BaselineFixedTpSl()

    # Прогон, собираем сделки с расхождением
    diffs = []
    for trade in trades:
        if trade.ticker not in PROBLEM_TICKERS:
            continue
        bars = cache.get_bars(
            trade.ticker,
            trade.ts_open + timedelta(minutes=1),
            trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
        )
        if bars.empty:
            continue
        sim = strategy.simulate(trade, bars)
        pnl_diff = sim.realized_pnl - trade.net_pnl_rub_phase2
        if abs(pnl_diff) > 1.0:
            diffs.append((trade, sim, pnl_diff))

    # Группируем по типу расхождения
    print("=" * 70)
    print("  Сделки с расхождением PnL > 1 руб (проблемные тикеры)")
    print("=" * 70)
    print(f"  Total: {len(diffs)}")

    # Группа: одинаковый exit_reason, но разное ts/price
    same_reason = [(t, s, d) for t, s, d in diffs if s.exit_reason == t.exit_reason_phase2]
    diff_reason = [(t, s, d) for t, s, d in diffs if s.exit_reason != t.exit_reason_phase2]

    print(f"\n  same exit_reason, diff details:  {len(same_reason)}")
    print(f"  different exit_reason:           {len(diff_reason)}")

    # ── Топ-5 разных reason — где Phase 2 закрыл раньше или позже ──────────
    print("\n" + "=" * 70)
    print("  Топ-5 trades с разным exit_reason (по |pnl_diff|)")
    print("=" * 70)
    diff_reason.sort(key=lambda x: abs(x[2]), reverse=True)
    for trade, sim, pnl_diff in diff_reason[:5]:
        print(f"\n  {trade.ticker} {('long' if trade.side==1 else 'short')} "
              f"@ {trade.entry:.4f}  ts_open={trade.ts_open}  horizon={trade.horizon_min}")
        print(f"    sl_price={trade.sl_price:.4f}  tp_price={trade.tp_price:.4f}")
        print(f"    Phase 2: {trade.exit_reason_phase2:>4s} @ {trade.exit_price_phase2:.4f}  "
              f"ts_close={trade.ts_close_phase2}  pnl={trade.net_pnl_rub_phase2:+.2f}")
        print(f"    Sim:     {sim.exit_reason:>4s} @ {sim.exit_price:.4f}  "
              f"ts_close={sim.ts_close}  pnl={sim.realized_pnl:+.2f}")
        print(f"    diff:    {pnl_diff:+.2f}")

        # Печатаем entry-минуту (которую мы пропускаем) для понимания
        entry_bar = cache.get_bar_at(trade.ticker, trade.ts_open)
        if entry_bar is not None:
            high, low, close = entry_bar["high"], entry_bar["low"], entry_bar["close"]
            sl_in_entry = (trade.side == 1 and low <= trade.sl_price) or \
                          (trade.side == -1 and high >= trade.sl_price)
            tp_in_entry = (trade.side == 1 and high >= trade.tp_price) or \
                          (trade.side == -1 and low <= trade.tp_price)
            marker = ""
            if sl_in_entry and tp_in_entry:
                marker = " <- DOUBLE in entry bar"
            elif sl_in_entry:
                marker = " <- SL touched in entry bar"
            elif tp_in_entry:
                marker = " <- TP touched in entry bar"
            print(f"    ENTRY bar ({trade.ts_open}): high={high:.4f} low={low:.4f} close={close:.4f}{marker}")

        # Печатаем бар после time-stop (который мы не проверяем как exit, а Phase 2 включает)
        time_stop_ts = trade.ts_open + timedelta(minutes=trade.horizon_min)
        after_time_bar = cache.get_bar_at(trade.ticker, time_stop_ts + timedelta(minutes=1))
        if after_time_bar is not None:
            high, low = after_time_bar["high"], after_time_bar["low"]
            sl_after = (trade.side == 1 and low <= trade.sl_price) or \
                       (trade.side == -1 and high >= trade.sl_price)
            tp_after = (trade.side == 1 and high >= trade.tp_price) or \
                       (trade.side == -1 and low <= trade.tp_price)
            marker = ""
            if sl_after or tp_after:
                marker = f" <- {'SL' if sl_after else 'TP'} touched in bar AFTER time-stop"
            print(f"    bar after time-stop ({time_stop_ts + timedelta(minutes=1)}): "
                  f"high={high:.4f} low={low:.4f}{marker}")

    # ── Сводка: сколько сделок имели TP/SL touch в entry-минуте ───────────
    print("\n" + "=" * 70)
    print("  Сколько сделок имели TP/SL touch в entry-минуте?")
    print("=" * 70)
    n_entry_touch_sl = 0
    n_entry_touch_tp = 0
    n_total = 0
    for trade in trades:
        if trade.ticker not in PROBLEM_TICKERS:
            continue
        n_total += 1
        entry_bar = cache.get_bar_at(trade.ticker, trade.ts_open)
        if entry_bar is None:
            continue
        high, low = entry_bar["high"], entry_bar["low"]
        if trade.side == 1:
            if low <= trade.sl_price:
                n_entry_touch_sl += 1
            if high >= trade.tp_price:
                n_entry_touch_tp += 1
        else:
            if high >= trade.sl_price:
                n_entry_touch_sl += 1
            if low <= trade.tp_price:
                n_entry_touch_tp += 1
    print(f"  Total in problem tickers: {n_total}")
    print(f"  SL touched in entry bar:  {n_entry_touch_sl}  "
          f"({100*n_entry_touch_sl/n_total:.1f}%)")
    print(f"  TP touched in entry bar:  {n_entry_touch_tp}  "
          f"({100*n_entry_touch_tp/n_total:.1f}%)")
    print()
    print("  Если эти проценты > 1-2%, гипотеза подтверждена:")
    print("  Phase 2 включает entry-минуту, мы пропускаем -> расхождение PnL.")


if __name__ == "__main__":
    main()
