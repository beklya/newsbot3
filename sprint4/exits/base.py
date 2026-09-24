"""
Sprint 4 / Commit 4.0 - Базовый интерфейс exit-стратегий
==========================================================

REVISION 3 (после полного чтения backtest_mfe.py):

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
  1. SL-first логика (НЕ TP-first как было в REV2):
     Phase 2 simulate_trade:
        if side == 1:
            if bar_low <= sl_price: ... break    # SL first!
            if bar_high >= tp_price: ... break
     Это пессимистичная assumption, но это то что в Phase 2.

  2. PnL умножается на lot_size:
     Phase 2: gross_pnl = (exit - entry) * lot_size * n_lots
     SBER/GAZP/MTSS/ROSN lot=10, VTBR lot=10000, USDRUB lot=1000
     остальные lot=1.

REVISION 1 (sl_dist_pct из готовых sl_price):
  Сохраняется — обратный расчёт учитывает MIN_SL_DIST floor.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd

from instruments import get_lot_size


# =============================================================================
# Phase 2 константы (информационные)
# =============================================================================
TP_FRACTION = 0.7
SL_BUFFER = 1.2
MIN_SL_DIST_PCT = 0.0005
MIN_TP_DIST_PCT = 0.001


# =============================================================================
# Trade
# =============================================================================
@dataclass(frozen=True)
class Trade:
    ticker: str
    fold: int
    horizon_min: int
    rr_threshold: float
    model_type: str

    ts_open: datetime
    side: int
    entry: float
    size_lots: int

    sl_price: float
    tp_price: float
    pred_mfe_pct: float
    pred_mae_pct: float

    ts_close_phase2: datetime
    exit_price_phase2: float
    exit_reason_phase2: str
    net_pnl_rub_phase2: float
    cost_rub: float

    @property
    def sl_dist_pct(self) -> float:
        return abs(self.entry - self.sl_price) / self.entry

    @property
    def tp_dist_pct(self) -> float:
        return abs(self.tp_price - self.entry) / self.entry

    @property
    def one_r_level(self) -> float:
        return 2 * self.entry - self.sl_price

    @property
    def rr_actual(self) -> float:
        if self.sl_dist_pct == 0:
            return 0.0
        return self.tp_dist_pct / self.sl_dist_pct

    @property
    def time_stop_ts(self) -> datetime:
        return self.ts_open + pd.Timedelta(minutes=self.horizon_min)

    @property
    def max_simulation_ts(self) -> datetime:
        return self.ts_open + pd.Timedelta(minutes=int(self.horizon_min * 1.5))

    @property
    def lot_size(self) -> int:
        """Контрактный множитель из реестра instruments.py."""
        return get_lot_size(self.ticker)


# =============================================================================
# ExitResult
# =============================================================================
@dataclass
class ExitResult:
    strategy_name: str
    ts_open: datetime
    ts_close: datetime
    side: int
    entry: float
    exit_price: float
    exit_reason: str
    realized_r: float
    realized_pnl: float
    duration_min: float
    partial_exits: list[dict] = field(default_factory=list)
    notes: str = ""


# =============================================================================
# Утилиты
# =============================================================================
def compute_realized_r(trade: Trade, exit_price: float) -> float:
    """Realized R в единицах SL distance."""
    sl_dist_abs = abs(trade.entry - trade.sl_price)
    if sl_dist_abs <= 0:
        return 0.0
    return trade.side * (exit_price - trade.entry) / sl_dist_abs


def compute_realized_r_partial(
    trade: Trade,
    parts: list[tuple[float, float]],
) -> float:
    total_weight = sum(w for w, _ in parts)
    if abs(total_weight - 1.0) > 1e-6:
        raise ValueError(f"Weights must sum to 1.0, got {total_weight}")
    return sum(w * compute_realized_r(trade, p) for w, p in parts)


def compute_pnl_pseudo_rub(trade: Trade, exit_price: float) -> float:
    """
    Gross PnL по формуле Phase 2:
        gross = side * (exit - entry) * lot_size * n_lots

    Для BR/NG/GLDRUB — псевдо-рубли (= доллары).
    """
    return trade.side * (exit_price - trade.entry) * trade.lot_size * trade.size_lots


def compute_pnl_with_costs(trade: Trade, exit_price: float, extra_legs: int = 0) -> float:
    """Net PnL = gross - cost - extra_cost_for_partial_legs."""
    gross = compute_pnl_pseudo_rub(trade, exit_price)
    extra_cost = trade.cost_rub * 0.5 * extra_legs
    return gross - trade.cost_rub - extra_cost


# =============================================================================
# Bar-by-bar helper - SL-first как в Phase 2 simulate_trade
# =============================================================================
def check_tp_sl_hit(
    bar: pd.Series,
    side: int,
    tp_price: float,
    sl_price: float,
) -> Optional[tuple[str, float]]:
    """
    *** Phase 2 assumption: SL проверяется ПЕРВЫМ ***

    Из backtest_mfe.py:376-397:
        if side == 1:
            if bar_low <= sl_price: ... break    # SL first
            if bar_high >= tp_price: ... break
        else:
            if bar_high >= sl_price: ... break   # SL first
            if bar_low <= tp_price: ... break

    Returns:
        None - ни TP ни SL не задеты
        ("sl", sl_price) - SL hit (приоритет)
        ("tp", tp_price) - TP hit
    """
    high = bar["high"]
    low = bar["low"]

    if side == 1:  # long
        if low <= sl_price:
            return ("sl", sl_price)
        if high >= tp_price:
            return ("tp", tp_price)
    else:  # short
        if high >= sl_price:
            return ("sl", sl_price)
        if low <= tp_price:
            return ("tp", tp_price)

    return None


# =============================================================================
# ExitStrategy base
# =============================================================================
class ExitStrategy(ABC):
    name: str = "abstract"

    @abstractmethod
    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        raise NotImplementedError


# =============================================================================
# Self-test
# =============================================================================
if __name__ == "__main__":
    t = Trade(
        ticker="SBER",
        fold=0, horizon_min=60, rr_threshold=2.0, model_type="mx_specific",
        ts_open=datetime(2025, 6, 15, 10, 0),
        side=1, entry=300.0, size_lots=10,
        sl_price=298.65, tp_price=301.575,
        pred_mfe_pct=0.75, pred_mae_pct=0.375,
        ts_close_phase2=datetime(2025, 6, 15, 10, 30),
        exit_price_phase2=301.5, exit_reason_phase2="tp",
        net_pnl_rub_phase2=10.0, cost_rub=2.0,
    )
    print(f"Trade: {t.ticker} {'long' if t.side==1 else 'short'} @ {t.entry}, size_lots={t.size_lots}")
    print(f"  lot_size (from registry): {t.lot_size}  (SBER expected 10)")
    print(f"  sl_dist_pct = {t.sl_dist_pct*100:.4f}%, tp_dist_pct = {t.tp_dist_pct*100:.4f}%")
    print(f"  1R level    = {t.one_r_level:.4f}")
    print()
    print("--- SL-first double-hit test ---")
    bar_double = pd.Series({
        "open": 300.0, "high": 301.8, "low": 298.4, "close": 300.5, "vol": 1000,
    })
    hit = check_tp_sl_hit(bar_double, side=1, tp_price=t.tp_price, sl_price=t.sl_price)
    print(f"  long double-hit (high=301.8, low=298.4): {hit}  (expected ('sl', 298.65))")
    assert hit == ("sl", 298.65), f"Expected SL-first, got {hit}"

    print("\n--- PnL formula test ---")
    exit_price = t.tp_price
    gross = compute_pnl_pseudo_rub(t, exit_price)
    expected_gross = 1 * (301.575 - 300.0) * 10 * 10  # side*diff*lot_size*n_lots
    print(f"  gross_pnl @ TP: {gross}  (expected {expected_gross})")
    assert abs(gross - expected_gross) < 1e-6

    print("\n--- realized_r tests ---")
    print(f"  realized_r @ 1R: {compute_realized_r(t, t.one_r_level):+.4f}  (expected +1.0)")
    print(f"  realized_r @ TP: {compute_realized_r(t, t.tp_price):+.4f}  (expected {t.rr_actual:.4f})")
    print(f"  realized_r @ SL: {compute_realized_r(t, t.sl_price):+.4f}  (expected -1.0)")

    print("\n--- VTBR test (lot_size=10000) ---")
    t_vtbr = Trade(
        ticker="VTBR",
        fold=0, horizon_min=60, rr_threshold=2.0, model_type="mx_specific",
        ts_open=datetime(2025, 6, 15, 10, 0),
        side=1, entry=0.025, size_lots=4,
        sl_price=0.02490, tp_price=0.02515,
        pred_mfe_pct=0.60, pred_mae_pct=0.50,
        ts_close_phase2=datetime(2025, 6, 15, 10, 30),
        exit_price_phase2=0.02515, exit_reason_phase2="tp",
        net_pnl_rub_phase2=6.0, cost_rub=0.1,
    )
    print(f"  VTBR lot_size: {t_vtbr.lot_size}  (expected 10000)")
    pnl_vtbr = compute_pnl_pseudo_rub(t_vtbr, 0.02515)
    expected = 1 * (0.02515 - 0.025) * 10000 * 4
    print(f"  gross_pnl: {pnl_vtbr}  (expected {expected})")
    assert abs(pnl_vtbr - expected) < 1e-6

    print("\nAll tests passed (SL-first + PnL x lot_size)")
