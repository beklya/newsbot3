"""
Sprint 4 / Commit 4.0 - Метрики сравнения exit-стратегий
==========================================================

Функции:
  compute_metrics(results: list[tuple[Trade, ExitResult]]) -> dict
    Возвращает словарь со всеми метриками для одной стратегии.

  compute_per_ticker(results) -> dict[ticker, metrics_dict]
  compute_per_year(results) -> dict[year, metrics_dict]
  compute_exit_distribution(results) -> dict[reason, count_and_pct]

Metric definitions:
  - total_pnl: сумма realized_pnl
  - n_trades: общее число сделок
  - win_rate: доля сделок с pnl > 0
  - avg_r_per_trade: средний realized R
  - sharpe_daily: mean(daily_pnl) / std(daily_pnl) × sqrt(252)
  - max_dd: максимальная просадка equity curve (в денежных единицах и в %)
  - profit_factor: gross_wins / abs(gross_losses)
  - avg_duration_min: средняя длительность сделок
  - p50/p95 duration: медиана и 95-й перцентиль

Note:
  PnL в "псевдо-рублях" (см. instruments.py). Для смешанных тикеров метрики
  будут смещены USD-инструментами. Per-ticker breakdown избегает этого.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from base import ExitResult, Trade


# =============================================================================
# Core compute_metrics
# =============================================================================
def compute_metrics(results: list[tuple[Trade, ExitResult]]) -> dict[str, Any]:
    """Считает агрегированные метрики для одного списка сделок."""
    if not results:
        return _empty_metrics()

    trades = [t for t, _ in results]
    exits = [r for _, r in results]

    pnls = np.array([r.realized_pnl for r in exits])
    rs = np.array([r.realized_r for r in exits])
    durations = np.array([r.duration_min for r in exits])

    # Basic
    n_trades = len(results)
    total_pnl = float(pnls.sum())
    win_rate = float((pnls > 0).mean())
    avg_pnl = float(pnls.mean())
    avg_r = float(rs.mean())

    # Profit factor
    gross_wins = float(pnls[pnls > 0].sum())
    gross_losses = float(abs(pnls[pnls < 0].sum()))
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    # Sharpe — daily
    sharpe_daily = _compute_daily_sharpe(trades, exits)

    # MaxDD на trade-by-trade equity
    equity = np.cumsum(pnls)
    max_dd_abs, max_dd_pct = _compute_max_dd(equity)

    # Durations
    avg_dur = float(durations.mean()) if len(durations) > 0 else 0.0
    p50_dur = float(np.percentile(durations, 50)) if len(durations) > 0 else 0.0
    p95_dur = float(np.percentile(durations, 95)) if len(durations) > 0 else 0.0

    # Exit reason distribution
    exit_dist = dict(Counter(r.exit_reason for r in exits))

    return {
        "n_trades": n_trades,
        "total_pnl": total_pnl,
        "avg_pnl_per_trade": avg_pnl,
        "avg_r_per_trade": avg_r,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "sharpe_daily": sharpe_daily,
        "max_dd_abs": max_dd_abs,
        "max_dd_pct": max_dd_pct,
        "gross_wins": gross_wins,
        "gross_losses": gross_losses,
        "avg_duration_min": avg_dur,
        "p50_duration_min": p50_dur,
        "p95_duration_min": p95_dur,
        "exit_distribution": exit_dist,
    }


def _empty_metrics() -> dict[str, Any]:
    return {
        "n_trades": 0,
        "total_pnl": 0.0, "avg_pnl_per_trade": 0.0, "avg_r_per_trade": 0.0,
        "win_rate": 0.0, "profit_factor": 0.0, "sharpe_daily": 0.0,
        "max_dd_abs": 0.0, "max_dd_pct": 0.0,
        "gross_wins": 0.0, "gross_losses": 0.0,
        "avg_duration_min": 0.0, "p50_duration_min": 0.0, "p95_duration_min": 0.0,
        "exit_distribution": {},
    }


def _compute_daily_sharpe(trades: list[Trade], exits: list[ExitResult]) -> float:
    """
    Sharpe рассчитывается на агрегированном daily PnL.
    На день агрегируем все trades, у которых ts_close в этот день.
    """
    daily = {}
    for t, r in zip(trades, exits):
        day = r.ts_close.date() if hasattr(r.ts_close, "date") else pd.Timestamp(r.ts_close).date()
        daily[day] = daily.get(day, 0.0) + r.realized_pnl

    if len(daily) < 2:
        return 0.0

    pnls = np.array(list(daily.values()))
    if pnls.std() == 0:
        return 0.0
    return float(pnls.mean() / pnls.std() * math.sqrt(252))


def _compute_max_dd(equity: np.ndarray) -> tuple[float, float]:
    """
    Returns:
        (max_dd_abs, max_dd_pct)
        max_dd_abs — абсолютная просадка
        max_dd_pct — относительная (как % от peak)
    """
    if len(equity) == 0:
        return 0.0, 0.0
    peak = np.maximum.accumulate(equity)
    dd = equity - peak  # отрицательные значения
    max_dd_abs = float(dd.min())
    # Относительная — от пика на момент просадки. Защита от деления на 0/отрицательное.
    with np.errstate(divide="ignore", invalid="ignore"):
        rel_dd = np.where(peak > 0, dd / peak, 0)
    max_dd_pct = float(rel_dd.min())
    return max_dd_abs, max_dd_pct


# =============================================================================
# Per-ticker / Per-year breakdowns
# =============================================================================
def compute_per_ticker(results: list[tuple[Trade, ExitResult]]) -> dict[str, dict[str, Any]]:
    """Группирует по trade.ticker и считает метрики для каждой группы."""
    by_ticker: dict[str, list[tuple[Trade, ExitResult]]] = {}
    for t, r in results:
        by_ticker.setdefault(t.ticker, []).append((t, r))
    return {tk: compute_metrics(group) for tk, group in by_ticker.items()}


def compute_per_year(results: list[tuple[Trade, ExitResult]]) -> dict[int, dict[str, Any]]:
    """Группирует по году ts_open."""
    by_year: dict[int, list[tuple[Trade, ExitResult]]] = {}
    for t, r in results:
        year = t.ts_open.year
        by_year.setdefault(year, []).append((t, r))
    return {y: compute_metrics(group) for y, group in by_year.items()}


def compute_per_year_ticker(results: list[tuple[Trade, ExitResult]]) -> dict[tuple[int, str], dict[str, Any]]:
    """Группирует по (год, ticker) — для детального дрифт-анализа."""
    by_pair: dict[tuple[int, str], list[tuple[Trade, ExitResult]]] = {}
    for t, r in results:
        key = (t.ts_open.year, t.ticker)
        by_pair.setdefault(key, []).append((t, r))
    return {k: compute_metrics(group) for k, group in by_pair.items()}


# =============================================================================
# Self-test
# =============================================================================
if __name__ == "__main__":
    from datetime import datetime as dt

    # Fake results: 3 winning, 2 losing
    results = []
    for i, pnl in enumerate([100, 200, -50, 150, -80]):
        t = Trade(
            ticker="SBER" if i < 3 else "GAZP",
            fold=0, horizon_min=60, rr_threshold=2.0, model_type="mx_specific",
            ts_open=dt(2025, 1, 1 + i), side=1, entry=100.0, size_lots=1,
            sl_price=99.0, tp_price=101.0,
            pred_mfe_pct=0.5, pred_mae_pct=0.5,
            ts_close_phase2=dt(2025, 1, 1 + i, 10), exit_price_phase2=100.5,
            exit_reason_phase2="tp", net_pnl_rub_phase2=pnl, cost_rub=1.0,
        )
        r = ExitResult(
            strategy_name="test",
            ts_open=t.ts_open, ts_close=t.ts_open.replace(hour=10),
            side=1, entry=100.0, exit_price=100.5,
            exit_reason="tp" if pnl > 0 else "sl",
            realized_r=pnl / 100, realized_pnl=pnl, duration_min=30.0,
        )
        results.append((t, r))

    m = compute_metrics(results)
    print("Metrics:")
    for k, v in m.items():
        print(f"  {k}: {v}")

    print(f"\nTotal PnL check: {m['total_pnl']} == 320")
    assert m["total_pnl"] == 320
    print(f"Win rate: {m['win_rate']} == 0.6")
    assert abs(m["win_rate"] - 0.6) < 1e-6

    pt = compute_per_ticker(results)
    print(f"\nPer-ticker SBER trades: {pt['SBER']['n_trades']} == 3")
    assert pt["SBER"]["n_trades"] == 3
