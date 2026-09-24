"""
Sprint 4 / Commit 4.0 - Стратегия #5: TimeBasedPartial
========================================================

Логика (гипотеза из exits_analysis_phase2.txt - самая ожидаемая улучшать):
  Стандартный fixed TP/SL + time-stop. НО:
  - На 10-й минуте от entry: закрываем 50% позиции по close 10-го бара
  - Оставшиеся 50% продолжают до TP/SL/time-stop (стандартный horizon)

Идея:
  Новостные импульсы обычно "выстреливают" в первые 5-15 минут на institutional фиксации.
  Затем 15-60 минут — retail continuation или fade.
  Time-based partial фиксирует institutional move (early), не отказываясь от retail extension.

Это РОВНО идея, с которой проект начинался ("выйти до отскока").

Параметры:
  partial_minute: на какой минуте делать partial exit (default = 10)
  partial_weight: какая доля позиции (default = 0.5)

Особенности:
  - На минуте partial_minute exit по close бара (не open!)
  - Если TP/SL hit ДО partial_minute -> вся позиция exit, partial не активируется
  - cost_rub удваивается на partial leg

exit_reason:
  - "sl" / "tp" — если случился до partial_minute (вся позиция)
  - "partial_tp" / "partial_sl" / "partial_time" — если был partial и потом 2-я часть вышла
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


class TimeBasedPartial(ExitStrategy):
    """50% позиции фиксируется на partial_minute, 50% продолжает."""

    name = "time_based_partial"

    def __init__(
        self,
        partial_minute: int = 10,
        partial_weight: float = 0.5,
        entry_bar_inclusive: bool = True,
        after_time_stop_bar_inclusive: bool = True,
    ):
        self.partial_minute = partial_minute
        self.partial_weight = partial_weight
        self.entry_bar_inclusive = entry_bar_inclusive
        self.after_time_stop_bar_inclusive = after_time_stop_bar_inclusive

    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        if bars.empty:
            return self._no_bars(trade)

        # Window-фильтр
        end_ts = trade.ts_open + pd.Timedelta(minutes=trade.horizon_min)
        partial_ts_target = trade.ts_open + pd.Timedelta(minutes=self.partial_minute)

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

        for ts, bar in window.iterrows():
            # 1. SL/TP проверка (Phase 2 SL-first)
            hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, trade.sl_price)
            if hit is not None:
                reason, exit_price = hit
                duration = (ts - trade.ts_open).total_seconds() / 60.0

                if partial_taken:
                    parts = [
                        (self.partial_weight, partial_exit_price),
                        (1.0 - self.partial_weight, exit_price),
                    ]
                    realized_r = compute_realized_r_partial(trade, parts)
                    gross1 = compute_pnl_pseudo_rub(trade, partial_exit_price) * self.partial_weight
                    gross2 = compute_pnl_pseudo_rub(trade, exit_price) * (1.0 - self.partial_weight)
                    realized_pnl = gross1 + gross2 - trade.cost_rub - trade.cost_rub * 0.5
                    avg_price = (
                        self.partial_weight * partial_exit_price
                        + (1.0 - self.partial_weight) * exit_price
                    )
                    return ExitResult(
                        strategy_name=self.name,
                        ts_open=trade.ts_open,
                        ts_close=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        side=trade.side, entry=trade.entry,
                        exit_price=avg_price,
                        exit_reason=f"partial_{reason}",
                        realized_r=realized_r,
                        realized_pnl=realized_pnl,
                        duration_min=duration,
                        partial_exits=[
                            {"ts": str(partial_exit_ts), "price": partial_exit_price,
                             "weight": self.partial_weight, "reason": f"partial_t{self.partial_minute}"},
                            {"ts": str(ts), "price": exit_price,
                             "weight": 1.0 - self.partial_weight, "reason": reason},
                        ],
                    )
                else:
                    # Partial не успел случиться -> вся позиция exit
                    realized_pnl = compute_pnl_pseudo_rub(trade, exit_price) - trade.cost_rub
                    return ExitResult(
                        strategy_name=self.name,
                        ts_open=trade.ts_open,
                        ts_close=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                        side=trade.side, entry=trade.entry,
                        exit_price=float(exit_price),
                        exit_reason=reason,
                        realized_r=compute_realized_r(trade, exit_price),
                        realized_pnl=realized_pnl,
                        duration_min=duration,
                    )

            # 2. SL/TP не сработали — проверяем активацию partial по времени
            #    Берём partial когда ts >= partial_ts_target (10 минут от entry)
            if not partial_taken and ts >= partial_ts_target:
                partial_taken = True
                partial_exit_price = float(bar["close"])
                partial_exit_ts = ts

        # Конец window — time-exit для 2-й половины
        last_row = window.iloc[-1]
        last_ts = window.index[-1]
        last_close = float(last_row["close"])
        duration = (last_ts - trade.ts_open).total_seconds() / 60.0

        if partial_taken:
            parts = [
                (self.partial_weight, partial_exit_price),
                (1.0 - self.partial_weight, last_close),
            ]
            realized_r = compute_realized_r_partial(trade, parts)
            gross1 = compute_pnl_pseudo_rub(trade, partial_exit_price) * self.partial_weight
            gross2 = compute_pnl_pseudo_rub(trade, last_close) * (1.0 - self.partial_weight)
            realized_pnl = gross1 + gross2 - trade.cost_rub - trade.cost_rub * 0.5
            avg_price = (
                self.partial_weight * partial_exit_price
                + (1.0 - self.partial_weight) * last_close
            )
            return ExitResult(
                strategy_name=self.name,
                ts_open=trade.ts_open,
                ts_close=last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts,
                side=trade.side, entry=trade.entry,
                exit_price=avg_price,
                exit_reason="partial_time",
                realized_r=realized_r,
                realized_pnl=realized_pnl,
                duration_min=duration,
                partial_exits=[
                    {"ts": str(partial_exit_ts), "price": partial_exit_price,
                     "weight": self.partial_weight, "reason": f"partial_t{self.partial_minute}"},
                    {"ts": str(last_ts), "price": last_close,
                     "weight": 1.0 - self.partial_weight, "reason": "time"},
                ],
            )
        else:
            # Partial не успел (horizon < partial_minute — для коротких horizons это возможно)
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
        sl_price=298.65, tp_price=301.575,
        pred_mfe_pct=0.75, pred_mae_pct=0.375,
        ts_close_phase2=datetime(2025, 6, 15, 10, 30),
        exit_price_phase2=301.5, exit_reason_phase2="tp",
        net_pnl_rub_phase2=10.0, cost_rub=2.0,
    )
    s = TimeBasedPartial(partial_minute=10)

    # Test 1: цена дрейфует, на 10-й минуте partial, потом продолжает до time
    print("\n=== Test 1: partial @ 10m, then time ===")
    rows = []
    times = []
    for i in range(15):
        # Дрейф вверх до 301.0 к 10-й минуте, потом плато
        if i < 10:
            close = 300.0 + 0.1 * i
        else:
            close = 301.0
        rows.append({
            "open": close - 0.05, "high": close + 0.05, "low": close - 0.05,
            "close": close, "vol": 1000,
        })
        times.append(datetime(2025, 6, 15, 10, 1) + pd.Timedelta(minutes=i))
    bars1 = pd.DataFrame(rows, index=pd.DatetimeIndex(times, name="ts"))
    r = s.simulate(t, bars1)
    # ts_open=10:00, partial_minute=10 → partial_ts_target=10:10
    # На баре 10:10 (i=9) — должен взяться partial @ close=300.9
    # Затем продолжает... но эти бары не дошли до time-stop (только 15 минут), это горизонт=60
    # Простой тест времени не подходит для time-exit. Лучше: убедимся, что partial взялся.
    print(f"  reason: {r.exit_reason}  (expected: partial_*)")
    print(f"  partial_exits: {r.partial_exits}")
    assert "partial" in r.exit_reason

    # Test 2: TP до 10-й минуты — partial не активируется
    print("\n=== Test 2: TP before 10m -> no partial ===")
    bars2 = pd.DataFrame({
        "open":  [300.0, 301.0, 301.5],
        "high":  [300.5, 301.4, 301.8],   # bar 3: TP hit
        "low":   [299.8, 300.8, 301.3],
        "close": [300.4, 301.2, 301.7],
        "vol":   [1000]*3,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3)], name="ts"
    ))
    r = s.simulate(t, bars2)
    print(f"  reason: {r.exit_reason}  (expected: tp — partial не успел)")
    assert r.exit_reason == "tp"
    assert len(r.partial_exits) == 0

    # Test 3: partial @ 10m, потом дошёл до TP
    print("\n=== Test 3: partial @ 10m, then TP ===")
    rows = []
    times = []
    for i in range(20):
        if i < 10:
            close = 300.0 + 0.04 * i
        elif i < 15:
            close = 300.4 + 0.1 * (i - 9)
        else:
            close = 301.5
        h = close + 0.1
        if i == 15:
            h = 301.8  # TP touched на баре 16
        rows.append({
            "open": close, "high": h, "low": close - 0.1, "close": close, "vol": 1000,
        })
        times.append(datetime(2025, 6, 15, 10, 1) + pd.Timedelta(minutes=i))
    bars3 = pd.DataFrame(rows, index=pd.DatetimeIndex(times, name="ts"))
    r = s.simulate(t, bars3)
    print(f"  reason: {r.exit_reason}  (expected: partial_tp)")
    print(f"  realized_r: {r.realized_r:.4f}")
    assert r.exit_reason == "partial_tp"

    print("\nAll time_based_partial tests passed")
