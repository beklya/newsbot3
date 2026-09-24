"""
Sprint 4 / Commit 4.0 - Стратегия #4: Partial50_50AtLevels
============================================================

Логика:
  Стандартный fixed TP/SL + time-stop. НО:
  - Когда цена touched 1R level — закрываем 50% позиции по 1R
  - Оставшиеся 50% продолжают торговаться до TP/SL/time

Нюансы:
  - После partial @ 1R: SL для оставшихся 50% остаётся прежним
    (НЕ breakeven, иначе это смесь двух стратегий — partial+breakeven)
  - На том же баре где touched 1R: проверяем SL первым (Phase 2 SL-first).
    Если SL hit -> exit ВСЕЙ позиции по SL. Partial НЕ активируется.
  - exit_reason: "partial_tp" если 2-я половина дошла до TP,
                 "partial_sl" если SL, "partial_time" если time-stop
  - cost_rub удваивается на partial leg (доп. комиссия за второй exit)

Realized R:
  realized_r_partial = 0.5 × R_at_1R + 0.5 × R_at_final_exit
  R_at_1R = +1.0 (по определению)
  R_at_final_exit = в зависимости от того, что произошло со второй половиной

Ожидание (из exits_analysis_phase2.txt гипотеза 2):
  Win rate растёт (~70-75% vs 60%), Avg per trade падает (~1.0R vs 1.17R),
  Sharpe чуть выше (variance падает быстрее mean).
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from base import (
    ExitResult,
    ExitStrategy,
    Trade,
    check_tp_sl_hit,
    compute_pnl_pseudo_rub,
    compute_realized_r,
    compute_realized_r_partial,
)


log = logging.getLogger(__name__)


class Partial50_50AtLevels(ExitStrategy):
    """50% @ 1R, 50% продолжает до TP/SL/time."""

    name = "partial_50_50_at_levels"

    def __init__(
        self,
        entry_bar_inclusive: bool = True,
        after_time_stop_bar_inclusive: bool = True,
        partial_weight: float = 0.5,   # 50% — половина
    ):
        self.entry_bar_inclusive = entry_bar_inclusive
        self.after_time_stop_bar_inclusive = after_time_stop_bar_inclusive
        self.partial_weight = partial_weight

    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        if bars.empty:
            return self._no_bars(trade)

        # Window-фильтр (как baseline)
        end_ts = trade.ts_open + pd.Timedelta(minutes=trade.horizon_min)
        if self.entry_bar_inclusive:
            window = bars[bars.index >= trade.ts_open]
        else:
            window = bars[bars.index > trade.ts_open]
        if self.after_time_stop_bar_inclusive:
            n_within = int((window.index <= end_ts).sum())
            if len(window) > n_within:
                window = window.iloc[: n_within + 1]
        else:
            window = window[window.index <= end_ts]

        if window.empty:
            return self._no_bars(trade, "empty after filtering")

        # State
        partial_taken = False
        partial_exit_price = None
        partial_exit_ts = None
        one_r_level = trade.one_r_level

        for ts, bar in window.iterrows():
            # 1. SL/TP-first (Phase 2 SL-first)
            hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, trade.sl_price)
            if hit is not None:
                reason, exit_price = hit
                duration = (ts - trade.ts_open).total_seconds() / 60.0

                if partial_taken:
                    # Только 2-я половина выходит здесь, 1-я уже зафиксирована на 1R
                    parts = [
                        (self.partial_weight, partial_exit_price),
                        (1.0 - self.partial_weight, exit_price),
                    ]
                    realized_r = compute_realized_r_partial(trade, parts)
                    # PnL: gross на partial весе + gross на остатке - cost*1.5 (2 exit legs)
                    gross1 = compute_pnl_pseudo_rub(trade, partial_exit_price) * self.partial_weight
                    gross2 = compute_pnl_pseudo_rub(trade, exit_price) * (1.0 - self.partial_weight)
                    extra_cost = trade.cost_rub * 0.5  # доп. комиссия за 2-й exit
                    realized_pnl = gross1 + gross2 - trade.cost_rub - extra_cost

                    full_reason = f"partial_{reason}"
                    avg_price = (
                        self.partial_weight * partial_exit_price
                        + (1.0 - self.partial_weight) * exit_price
                    )
                    return ExitResult(
                        strategy_name=self.name,
                        ts_open=trade.ts_open,
                        ts_close=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        side=trade.side,
                        entry=trade.entry,
                        exit_price=avg_price,
                        exit_reason=full_reason,
                        realized_r=realized_r,
                        realized_pnl=realized_pnl,
                        duration_min=duration,
                        partial_exits=[
                            {"ts": str(partial_exit_ts), "price": partial_exit_price,
                             "weight": self.partial_weight, "reason": "partial_1r"},
                            {"ts": str(ts), "price": exit_price,
                             "weight": 1.0 - self.partial_weight, "reason": reason},
                        ],
                    )
                else:
                    # Partial ещё не взят -> вся позиция exit по SL/TP
                    realized_pnl = compute_pnl_pseudo_rub(trade, exit_price) - trade.cost_rub
                    return ExitResult(
                        strategy_name=self.name,
                        ts_open=trade.ts_open,
                        ts_close=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        side=trade.side,
                        entry=trade.entry,
                        exit_price=float(exit_price),
                        exit_reason=reason,
                        realized_r=compute_realized_r(trade, exit_price),
                        realized_pnl=realized_pnl,
                        duration_min=duration,
                    )

            # 2. SL/TP не сработали — проверяем активацию partial (1R touch)
            if not partial_taken:
                touched_1r = (
                    (trade.side == 1 and bar["high"] >= one_r_level)
                    or (trade.side == -1 and bar["low"] <= one_r_level)
                )
                if touched_1r:
                    partial_taken = True
                    partial_exit_price = one_r_level
                    partial_exit_ts = ts

        # Конец window — time-exit для оставшейся половины (или всей позиции)
        last_row = window.iloc[-1]
        last_ts = window.index[-1]
        last_close = float(last_row["close"])
        duration = (last_ts - trade.ts_open).total_seconds() / 60.0

        if partial_taken:
            # 1-я половина уже взята на 1R, 2-я выходит по time-stop close
            parts = [
                (self.partial_weight, partial_exit_price),
                (1.0 - self.partial_weight, last_close),
            ]
            realized_r = compute_realized_r_partial(trade, parts)
            gross1 = compute_pnl_pseudo_rub(trade, partial_exit_price) * self.partial_weight
            gross2 = compute_pnl_pseudo_rub(trade, last_close) * (1.0 - self.partial_weight)
            extra_cost = trade.cost_rub * 0.5
            realized_pnl = gross1 + gross2 - trade.cost_rub - extra_cost
            avg_price = (
                self.partial_weight * partial_exit_price
                + (1.0 - self.partial_weight) * last_close
            )
            return ExitResult(
                strategy_name=self.name,
                ts_open=trade.ts_open,
                ts_close=last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts,
                side=trade.side, entry=trade.entry,
                exit_price=avg_price, exit_reason="partial_time",
                realized_r=realized_r, realized_pnl=realized_pnl,
                duration_min=duration,
                partial_exits=[
                    {"ts": str(partial_exit_ts), "price": partial_exit_price,
                     "weight": self.partial_weight, "reason": "partial_1r"},
                    {"ts": str(last_ts), "price": last_close,
                     "weight": 1.0 - self.partial_weight, "reason": "time"},
                ],
            )
        else:
            # Partial не активирован — вся позиция time-exit
            realized_pnl = compute_pnl_pseudo_rub(trade, last_close) - trade.cost_rub
            return ExitResult(
                strategy_name=self.name,
                ts_open=trade.ts_open,
                ts_close=last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts,
                side=trade.side, entry=trade.entry,
                exit_price=last_close, exit_reason="time",
                realized_r=compute_realized_r(trade, last_close),
                realized_pnl=realized_pnl,
                duration_min=duration,
            )

    def _no_bars(self, trade: Trade, note: str = "no bars") -> ExitResult:
        return ExitResult(
            strategy_name=self.name,
            ts_open=trade.ts_open, ts_close=trade.ts_open,
            side=trade.side, entry=trade.entry, exit_price=trade.entry,
            exit_reason="no_bars",
            realized_r=0.0, realized_pnl=-trade.cost_rub,
            duration_min=0.0, notes=note,
        )


# =============================================================================
# Self-tests
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)-7s %(message)s")

    t = Trade(
        ticker="SBER",
        fold=0, horizon_min=60, rr_threshold=2.0, model_type="mx_specific",
        ts_open=datetime(2025, 6, 15, 10, 0),
        side=1, entry=300.0, size_lots=10,
        sl_price=298.65, tp_price=301.575,   # 1R=301.35, rr_actual=1.1667
        pred_mfe_pct=0.75, pred_mae_pct=0.375,
        ts_close_phase2=datetime(2025, 6, 15, 10, 30),
        exit_price_phase2=301.5, exit_reason_phase2="tp",
        net_pnl_rub_phase2=10.0, cost_rub=2.0,
    )
    s = Partial50_50AtLevels()

    # Test 1: 1R touched, потом дошёл до TP
    print("\n=== Test 1: 1R touched, then TP ===")
    bars1 = pd.DataFrame({
        "open":  [300.5, 301.0, 301.5],
        "high":  [300.9, 301.4, 301.8],   # bar 2: 1R touched. bar 3: TP hit.
        "low":   [300.3, 300.7, 301.0],
        "close": [300.7, 301.2, 301.6],
        "vol":   [1000]*3,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3)], name="ts"
    ))
    r = s.simulate(t, bars1)
    print(f"  reason: {r.exit_reason}  (expected: partial_tp)")
    print(f"  realized_r: {r.realized_r:.4f}  (expected: 0.5*1.0 + 0.5*1.1667 = 1.0833)")
    print(f"  partial_exits: {r.partial_exits}")
    expected_r = 0.5 * 1.0 + 0.5 * 1.1667
    assert r.exit_reason == "partial_tp"
    assert abs(r.realized_r - expected_r) < 1e-3

    # Test 2: 1R touched, откатилось до SL
    print("\n=== Test 2: 1R touched, then SL ===")
    bars2 = pd.DataFrame({
        "open":  [300.5, 301.0, 300.5, 299.5],
        "high":  [300.9, 301.4, 300.8, 299.8],
        "low":   [300.3, 300.7, 300.2, 298.5],   # bar 4: SL hit
        "close": [300.7, 301.2, 300.4, 298.7],
        "vol":   [1000]*4,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3, 4)], name="ts"
    ))
    r = s.simulate(t, bars2)
    print(f"  reason: {r.exit_reason}  (expected: partial_sl)")
    print(f"  realized_r: {r.realized_r:.4f}  (expected: 0.5*1.0 + 0.5*(-1.0) = 0.0)")
    assert r.exit_reason == "partial_sl"
    assert abs(r.realized_r) < 1e-3

    # Test 3: Не дошло до 1R — обычный SL
    print("\n=== Test 3: SL without 1R touch ===")
    bars3 = pd.DataFrame({
        "open":  [300.0, 299.0],
        "high":  [300.1, 299.2],
        "low":   [299.0, 298.5],
        "close": [299.2, 298.7],
        "vol":   [1000]*2,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2)], name="ts"
    ))
    r = s.simulate(t, bars3)
    print(f"  reason: {r.exit_reason}  (expected: sl)")
    assert r.exit_reason == "sl"
    assert abs(r.realized_r + 1.0) < 1e-6

    print("\nAll partial tests passed")
