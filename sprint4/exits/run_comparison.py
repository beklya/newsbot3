"""
Sprint 4 / Commit 4.0 - Главный runner comparison 5 exit-стратегий
======================================================================

Что делает:
  1. Загружает best combo (h=60, rr=2.0, mx_specific) — 3300 сделок
  2. Прогоняет все 5 стратегий:
       - baseline_fixed_tp_sl
       - breakeven_after_1r
       - trailing_after_1r
       - partial_50_50_at_levels
       - time_based_partial (partial_minute=10)
  3. Считает метрики (Sharpe, MaxDD, win_rate, profit_factor, R per trade)
  4. Сохраняет Excel со sheets:
       - summary: метрики каждой стратегии
       - per_ticker: pivot стратегия × тикер
       - per_year: pivot стратегия × год (для оценки дрифта)
       - exit_dist: распределение exit_reason для каждой стратегии
       - top_diff: топ-20 сделок с наибольшим отличием PnL от baseline

Запуск:
  python run_comparison.py
  -> data/exits_comparison_<timestamp>.xlsx
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from base import ExitResult, Trade
from baseline import BaselineFixedTpSl
from breakeven import BreakevenAfter1R
from partial_at_levels import Partial50_50AtLevels
from prices_cache import PricesCache
from time_based_partial import TimeBasedPartial
from trades_loader import load_trades_best_combo
from trailing import TrailingAfter1R
from metrics import (
    compute_metrics,
    compute_per_ticker,
    compute_per_year,
)


# =============================================================================
# Config
# =============================================================================
ATR_LOOKBACK_MINUTES = 30   # минут до ts_open для расчёта ATR в trailing
OUT_DIR = Path("data")


def build_strategies() -> list:
    """Все 5 стратегий с дефолтными Phase 2 window settings."""
    return [
        BaselineFixedTpSl(),                              # 1
        BreakevenAfter1R(),                               # 2
        TrailingAfter1R(),                                # 3
        Partial50_50AtLevels(),                           # 4
        TimeBasedPartial(partial_minute=10),              # 5
    ]


def run_strategy_on_trades(
    strategy,
    trades: list[Trade],
    cache: PricesCache,
) -> list[tuple[Trade, ExitResult]]:
    """Прогоняет одну стратегию на всех сделках. Trailing получает prior_bars для ATR."""
    results: list[tuple[Trade, ExitResult]] = []

    is_trailing = isinstance(strategy, TrailingAfter1R)

    for trade in trades:
        try:
            bars = cache.get_bars(
                trade.ticker,
                trade.ts_open,
                trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
                inclusive="both",
            )
        except FileNotFoundError:
            continue

        if bars.empty:
            continue

        if is_trailing:
            # Подсасываем prior_bars для ATR
            try:
                prior_bars = cache.get_bars(
                    trade.ticker,
                    trade.ts_open - timedelta(minutes=ATR_LOOKBACK_MINUTES),
                    trade.ts_open - timedelta(minutes=1),
                    inclusive="both",
                )
            except FileNotFoundError:
                prior_bars = None
            result = strategy.simulate(trade, bars, prior_bars=prior_bars)
        else:
            result = strategy.simulate(trade, bars)

        results.append((trade, result))

    return results


def metrics_to_summary_row(name: str, metrics: dict) -> dict:
    """Превращает dict метрик в плоскую строку для DataFrame."""
    return {
        "strategy": name,
        "n_trades": metrics["n_trades"],
        "total_pnl": round(metrics["total_pnl"], 0),
        "avg_r": round(metrics["avg_r_per_trade"], 4),
        "win_rate": round(metrics["win_rate"], 4),
        "profit_factor": round(metrics["profit_factor"], 3) if metrics["profit_factor"] != float("inf") else None,
        "sharpe_daily": round(metrics["sharpe_daily"], 3),
        "max_dd_abs": round(metrics["max_dd_abs"], 0),
        "max_dd_pct": round(metrics["max_dd_pct"], 4),
        "avg_duration_min": round(metrics["avg_duration_min"], 1),
        "p50_duration_min": round(metrics["p50_duration_min"], 1),
        "p95_duration_min": round(metrics["p95_duration_min"], 1),
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    log = logging.getLogger("comparison")

    OUT_DIR.mkdir(exist_ok=True)

    print("=" * 70)
    print("  Sprint 4 / Commit 4.0 - Exits Comparison Runner")
    print("=" * 70)

    # 1. Load data
    trades = load_trades_best_combo()
    print(f"\n  Loaded {len(trades)} trades from best combo (h=60, rr=2.0, mx_specific)")

    cache = PricesCache()
    unique_tickers = sorted({t.ticker for t in trades})
    print(f"  Warming up cache for {len(unique_tickers)} tickers...")
    cache.warmup(unique_tickers)

    # 2. Run all strategies
    strategies = build_strategies()
    all_results: dict[str, list[tuple[Trade, ExitResult]]] = {}

    print(f"\n  Running {len(strategies)} strategies on {len(trades)} trades...\n")
    for strat in strategies:
        started = time.time()
        results = run_strategy_on_trades(strat, trades, cache)
        elapsed = time.time() - started
        all_results[strat.name] = results
        print(f"  {strat.name:<30s}  {len(results):>5d} trades  {elapsed:>5.1f}s")

    # 3. Compute metrics for each strategy
    print(f"\n  Computing metrics...")
    strategy_metrics: dict[str, dict] = {}
    for name, results in all_results.items():
        strategy_metrics[name] = compute_metrics(results)

    # 4. Build summary DataFrame
    summary_rows = [
        metrics_to_summary_row(name, m) for name, m in strategy_metrics.items()
    ]
    summary_df = pd.DataFrame(summary_rows).set_index("strategy")
    print()
    print("  === SUMMARY ===")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(summary_df.to_string())

    # 5. Per-ticker breakdown — pivot strategy × ticker × total_pnl
    per_ticker_rows = []
    for name, results in all_results.items():
        pt = compute_per_ticker(results)
        for ticker, m in pt.items():
            per_ticker_rows.append({
                "strategy": name,
                "ticker": ticker,
                "n": m["n_trades"],
                "pnl": round(m["total_pnl"], 0),
                "win_rate": round(m["win_rate"], 4),
                "avg_r": round(m["avg_r_per_trade"], 4),
            })
    per_ticker_df = pd.DataFrame(per_ticker_rows)
    pivot_ticker_pnl = per_ticker_df.pivot(
        index="ticker", columns="strategy", values="pnl",
    )
    pivot_ticker_winrate = per_ticker_df.pivot(
        index="ticker", columns="strategy", values="win_rate",
    )

    # 6. Per-year breakdown
    per_year_rows = []
    for name, results in all_results.items():
        py = compute_per_year(results)
        for year, m in py.items():
            per_year_rows.append({
                "strategy": name,
                "year": year,
                "n": m["n_trades"],
                "total_pnl": round(m["total_pnl"], 0),
                "win_rate": round(m["win_rate"], 4),
                "sharpe_daily": round(m["sharpe_daily"], 3),
            })
    per_year_df = pd.DataFrame(per_year_rows)
    pivot_year_pnl = per_year_df.pivot(index="year", columns="strategy", values="total_pnl")
    pivot_year_sharpe = per_year_df.pivot(index="year", columns="strategy", values="sharpe_daily")

    # 7. Exit reason distribution per strategy
    exit_rows = []
    for name, results in all_results.items():
        dist = strategy_metrics[name]["exit_distribution"]
        n = strategy_metrics[name]["n_trades"]
        for reason, count in sorted(dist.items()):
            exit_rows.append({
                "strategy": name,
                "exit_reason": reason,
                "count": count,
                "pct": round(100 * count / n, 2) if n > 0 else 0,
            })
    exit_df = pd.DataFrame(exit_rows)

    # 8. Top-20 diff vs baseline (для интуиции — где partial/trailing бьют baseline)
    baseline_results = {
        (t.ticker, t.ts_open): r.realized_pnl
        for t, r in all_results["baseline_fixed_tp_sl"]
    }

    diff_rows = []
    for name, results in all_results.items():
        if name == "baseline_fixed_tp_sl":
            continue
        for t, r in results:
            base_pnl = baseline_results.get((t.ticker, t.ts_open), 0)
            diff_rows.append({
                "strategy": name,
                "ticker": t.ticker,
                "ts_open": t.ts_open,
                "side": "long" if t.side == 1 else "short",
                "entry": t.entry,
                "baseline_pnl": round(base_pnl, 2),
                "strategy_pnl": round(r.realized_pnl, 2),
                "diff": round(r.realized_pnl - base_pnl, 2),
                "baseline_reason": "—",  # для краткости
                "strategy_reason": r.exit_reason,
            })
    diff_df = pd.DataFrame(diff_rows)
    diff_df["abs_diff"] = diff_df["diff"].abs()
    top_diff_df = diff_df.sort_values("abs_diff", ascending=False).head(40).drop(columns=["abs_diff"])

    # 9. Save Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"exits_comparison_{timestamp}.xlsx"
    print(f"\n  Writing Excel: {out_path}")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary")
        pivot_ticker_pnl.to_excel(writer, sheet_name="ticker_pnl")
        pivot_ticker_winrate.to_excel(writer, sheet_name="ticker_winrate")
        pivot_year_pnl.to_excel(writer, sheet_name="year_pnl")
        pivot_year_sharpe.to_excel(writer, sheet_name="year_sharpe")
        exit_df.to_excel(writer, sheet_name="exit_dist", index=False)
        top_diff_df.to_excel(writer, sheet_name="top_diff_vs_baseline", index=False)

    print(f"\n  Done! Excel report saved.")

    # 10. Verdict — best strategy by composite criteria
    print()
    print("=" * 70)
    print("  Strategy ranking (by Sharpe, then total_pnl)")
    print("=" * 70)
    ranking = sorted(
        strategy_metrics.items(),
        key=lambda x: (x[1]["sharpe_daily"], x[1]["total_pnl"]),
        reverse=True,
    )
    for i, (name, m) in enumerate(ranking, 1):
        print(f"  {i}. {name:<30s}  Sharpe={m['sharpe_daily']:>6.2f}  "
              f"PnL={m['total_pnl']:>+12,.0f}  win={m['win_rate']*100:>5.1f}%  "
              f"MaxDD={m['max_dd_abs']:>+12,.0f}")

    best_name, best_m = ranking[0]
    base_m = strategy_metrics["baseline_fixed_tp_sl"]
    print()
    print(f"  BEST: {best_name}")
    print(f"    PnL vs baseline:    {best_m['total_pnl'] - base_m['total_pnl']:>+12,.0f}  "
          f"({100*(best_m['total_pnl'] - base_m['total_pnl'])/abs(base_m['total_pnl']):+.1f}%)")
    print(f"    Sharpe vs baseline: {best_m['sharpe_daily'] - base_m['sharpe_daily']:>+6.2f}")
    print(f"    MaxDD vs baseline:  {best_m['max_dd_abs'] - base_m['max_dd_abs']:>+12,.0f}")


if __name__ == "__main__":
    main()
