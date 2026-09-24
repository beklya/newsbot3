"""
Sprint 4 / Commit 4.0 - Стратегия #2: BreakevenAfter1R
=======================================================

Логика:
  Стандартный fixed TP/SL + time-stop. НО:
  Если в каком-то баре цена touched 1R level (high>=1R для long, low<=1R для short),
  то для всех последующих баров SL = entry.

  Это защита прибыли — если позиция прошла 1R, как минимум не дадим ей уйти в минус.

Тонкости:
  - 1R touch определяется по high/low бара (intraday touch, не close)
  - На том же баре, где touched 1R: проверяем SL old первым (Phase 2 SL-first).
    Если SL hit -> exit по old SL. Breakeven НЕ активируется задним числом.
  - После активации breakeven: SL = entry, TP остаётся прежним
  - Time-exit логика та же: close последнего бара в window

Ожидание (из exits_analysis_phase2.txt гипотеза 2):
  Win rate UP (с ~60% до ~70%), Avg per trade DOWN (~0.8R вместо 1.17R), Sharpe MIX-эффект
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
    compute_pnl_with_costs,
    compute_realized_r,
)
from baseline import BaselineFixedTpSl


log = logging.getLogger(__name__)


class BreakevenAfter1R(ExitStrategy):
    """SL подтягивается к entry после прохождения 1R."""

    name = "breakeven_after_1r"

    def __init__(
        self,
        entry_bar_inclusive: bool = True,
        after_time_stop_bar_inclusive: bool = True,
    ):
        self.entry_bar_inclusive = entry_bar_inclusive
        self.after_time_stop_bar_inclusive = after_time_stop_bar_inclusive

    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        if bars.empty:
            return self._no_bars(trade)

        # Применяем те же window-rules что и baseline
        end_ts = trade.ts_open + pd.Timedelta(minutes=trade.horizon_min)
        if self.entry_bar_inclusive:
            window = bars[bars.index >= trade.ts_open]
        else:
            window = bars[bars.index > trade.ts_open]
        if self.after_time_stop_bar_inclusive:
            mask_within = window.index <= end_ts
            n_within = int(mask_within.sum())
            if len(window) > n_within:
                window = window.iloc[: n_within + 1]
        else:
            window = window[window.index <= end_ts]

        if window.empty:
            return self._no_bars(trade, "empty after filtering")

        # State: текущий SL уровень (может измениться на breakeven)
        current_sl = trade.sl_price
        breakeven_activated = False
        one_r_level = trade.one_r_level

        for ts, bar in window.iterrows():
            # 1. Сначала Phase 2 SL-first проверка на текущем SL уровне
            hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, current_sl)
            if hit is not None:
                reason, exit_price = hit
                # Расширяем exit_reason: если breakeven активирован и SL hit -> "breakeven"
                if breakeven_activated and reason == "sl":
                    reason = "breakeven"
                duration = (ts - trade.ts_open).total_seconds() / 60.0
                return ExitResult(
                    strategy_name=self.name,
                    ts_open=trade.ts_open,
                    ts_close=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                    side=trade.side,
                    entry=trade.entry,
                    exit_price=float(exit_price),
                    exit_reason=reason,
                    realized_r=compute_realized_r(trade, exit_price),
                    realized_pnl=compute_pnl_with_costs(trade, exit_price),
                    duration_min=duration,
                )

            # 2. Если SL/TP не сработал — проверяем 1R touch для активации breakeven
            #    Только если breakeven ещё не активирован.
            if not breakeven_activated:
                touched_1r = (
                    (trade.side == 1 and bar["high"] >= one_r_level)
                    or (trade.side == -1 and bar["low"] <= one_r_level)
                )
                if touched_1r:
                    breakeven_activated = True
                    current_sl = trade.entry  # SL -> entry

        # Time-exit
        last_row = window.iloc[-1]
        last_ts = window.index[-1]
        last_close = float(last_row["close"])
        duration = (last_ts - trade.ts_open).total_seconds() / 60.0
        return ExitResult(
            strategy_name=self.name,
            ts_open=trade.ts_open,
            ts_close=last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts,
            side=trade.side,
            entry=trade.entry,
            exit_price=last_close,
            exit_reason="time",
            realized_r=compute_realized_r(trade, last_close),
            realized_pnl=compute_pnl_with_costs(trade, last_close),
            duration_min=duration,
            notes=f"breakeven_activated={breakeven_activated}",
        )

    def _no_bars(self, trade: Trade, note: str = "no bars") -> ExitResult:
        return ExitResult(
            strategy_name=self.name,
            ts_open=trade.ts_open,
            ts_close=trade.ts_open,
            side=trade.side,
            entry=trade.entry,
            exit_price=trade.entry,
            exit_reason="no_bars",
            realized_r=0.0,
            realized_pnl=-trade.cost_rub,
            duration_min=0.0,
            notes=note,
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
        sl_price=298.65,    # 1R distance = 1.35
        tp_price=301.575,
        pred_mfe_pct=0.75, pred_mae_pct=0.375,
        ts_close_phase2=datetime(2025, 6, 15, 10, 30),
        exit_price_phase2=301.5, exit_reason_phase2="tp",
        net_pnl_rub_phase2=10.0, cost_rub=2.0,
    )
    print(f"Trade: SBER long @ 300, sl=298.65, tp=301.575, 1R_level={t.one_r_level}")

    s = BreakevenAfter1R()

    # Test 1: Цена прошла 1R, потом откатилась до entry -> breakeven exit
    print("\n=== Test 1: 1R touched, then pullback to entry -> breakeven ===")
    bars1 = pd.DataFrame(
        {
            "open":  [300.5, 301.0, 301.2, 300.5, 299.9],
            "high":  [300.9, 301.4, 301.3, 300.8, 300.1],   # bar 2: high=301.4>=1R=301.35 → trigger
            "low":   [300.3, 300.7, 300.9, 300.0, 299.7],   # bar 5: low=299.7 < entry=300 → breakeven SL
            "close": [300.7, 301.2, 301.0, 300.2, 299.8],
            "vol":   [1000]*5,
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3, 4, 5)],
            name="ts",
        ),
    )
    r = s.simulate(t, bars1)
    print(f"  reason: {r.exit_reason}  (expected: breakeven)")
    print(f"  exit:   {r.exit_price}  (expected: 300.0 — entry)")
    print(f"  realized_r: {r.realized_r:.4f}  (expected: 0.0)")
    assert r.exit_reason == "breakeven", f"Got {r.exit_reason}"
    assert abs(r.exit_price - 300.0) < 1e-6
    assert abs(r.realized_r) < 1e-6

    # Test 2: Цена прошла 1R и дошла до TP — обычный TP exit
    print("\n=== Test 2: 1R touched, continued to TP ===")
    bars2 = pd.DataFrame(
        {
            "open":  [300.5, 301.0, 301.2, 301.5],
            "high":  [300.9, 301.4, 301.6, 301.8],   # bar 2: 1R trigger; bar 3: TP hit
            "low":   [300.3, 300.7, 301.0, 301.3],
            "close": [300.7, 301.2, 301.5, 301.6],
            "vol":   [1000]*4,
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3, 4)],
            name="ts",
        ),
    )
    r = s.simulate(t, bars2)
    print(f"  reason: {r.exit_reason}  (expected: tp)")
    print(f"  exit:   {r.exit_price}  (expected: 301.575)")
    assert r.exit_reason == "tp"

    # Test 3: Цена сразу пошла к SL (без 1R) — обычный SL
    print("\n=== Test 3: Direct SL hit, no 1R touch ===")
    bars3 = pd.DataFrame(
        {
            "open":  [300.0, 299.0],
            "high":  [300.1, 299.2],
            "low":   [299.0, 298.5],   # bar 2: low=298.5 < SL=298.65
            "close": [299.2, 298.7],
            "vol":   [1000]*2,
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, m) for m in (1, 2)],
            name="ts",
        ),
    )
    r = s.simulate(t, bars3)
    print(f"  reason: {r.exit_reason}  (expected: sl)")
    print(f"  realized_r: {r.realized_r:.4f}  (expected: -1.0)")
    assert r.exit_reason == "sl"
    assert abs(r.realized_r + 1.0) < 1e-6

    # Test 4: 1R touched, время вышло — time exit
    print("\n=== Test 4: 1R touched, time-stop ===")
    # 60 баров, 1R touched на баре 5, потом просто дрейф
    rows = []
    times = []
    for i in range(60):
        if i == 4:
            rows.append({"open": 301.0, "high": 301.5, "low": 300.8, "close": 301.0, "vol": 1000})
        else:
            rows.append({"open": 300.5, "high": 300.7, "low": 300.2, "close": 300.5, "vol": 1000})
        times.append(datetime(2025, 6, 15, 10, 1) + pd.Timedelta(minutes=i))
    bars4 = pd.DataFrame(rows, index=pd.DatetimeIndex(times, name="ts"))
    r = s.simulate(t, bars4)
    print(f"  reason: {r.exit_reason}  (expected: time)")
    print(f"  exit:   {r.exit_price}  (expected: 300.5)")
    assert r.exit_reason == "time"

    print("\nAll breakeven tests passed")
