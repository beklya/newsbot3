"""
Sprint 4 / Commit 4.0 - Стратегия #3: TrailingAfter1R
=======================================================

Логика:
  Стандартный fixed TP + initial SL + time-stop.
  Когда цена touched 1R level — активируется trailing:
    - Считается ATR(14) на минутных барах до момента активации
    - На каждом баре после активации: новый SL = close +/- 0.5*ATR (от текущего close)
    - SL только подтягивается в сторону прибыли, никогда не откатывается
    - TP остаётся прежним

Anti look-ahead bias:
  - ATR считается на барах ДО ts_open (используем lookback)
  - Trailing решение принимаем по close бара (не по high/low)
  - SL новый сравнивается с close, который УЖЕ зафиксирован

Тонкости:
  - На том же баре где SL hit (старый) -> exit по old SL, trailing не обновляется
  - Если ATR=0 (плоский рынок) -> trailing деградирует в breakeven

Ожидание (из exits_analysis_phase2.txt гипотеза 1):
  Trailing на новостных импульсах скорее ВРЕДИТ (стопает на нормальном откате внутри импульса).
  Ожидаем Sharpe вниз на ~10-20% vs baseline. Но MaxDD может улучшиться.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd

from base import (
    ExitResult,
    ExitStrategy,
    Trade,
    check_tp_sl_hit,
    compute_pnl_with_costs,
    compute_realized_r,
)


log = logging.getLogger(__name__)


# Параметры trailing
ATR_WINDOW = 14
TRAILING_MULTIPLIER = 0.5    # SL = close - 0.5*ATR (для long)
ATR_FALLBACK_PCT = 0.001     # если ATR=0 (плоский рынок) — используем 0.1% от entry


def compute_atr(bars: pd.DataFrame, window: int = ATR_WINDOW) -> float:
    """
    Вычисляет средний true range за последние `window` баров.

    True range (для минутного бара):
        max(high-low, |high-prev_close|, |low-prev_close|)

    Возвращает скаляр (среднее за window).
    Если баров меньше чем window — берёт что есть.
    """
    if bars.empty:
        return 0.0

    high = bars["high"]
    low = bars["low"]
    prev_close = bars["close"].shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # Первый бар: prev_close is NaN → tr = high - low
    tr.iloc[0] = high.iloc[0] - low.iloc[0]
    # Берём среднее за последние `window`
    return float(tr.tail(window).mean())


class TrailingAfter1R(ExitStrategy):
    """Trailing SL по ATR(14) после прохождения 1R."""

    name = "trailing_after_1r"

    def __init__(
        self,
        entry_bar_inclusive: bool = True,
        after_time_stop_bar_inclusive: bool = True,
        atr_lookback_bars: int = 14,
        trailing_multiplier: float = 0.5,
    ):
        self.entry_bar_inclusive = entry_bar_inclusive
        self.after_time_stop_bar_inclusive = after_time_stop_bar_inclusive
        self.atr_lookback_bars = atr_lookback_bars
        self.trailing_multiplier = trailing_multiplier

    def simulate(
        self,
        trade: Trade,
        bars: pd.DataFrame,
        prior_bars: pd.DataFrame | None = None,
    ) -> ExitResult:
        """
        Args:
            bars: бары для exit-симуляции (как в baseline)
            prior_bars: бары ДО ts_open для расчёта ATR.
                Если None — ATR считается на первых atr_lookback_bars из bars.
                Передающий код (runner) должен предоставить достаточный lookback.
        """
        if bars.empty:
            return self._no_bars(trade)

        # ATR из prior_bars, fallback на первые бары если prior нет
        if prior_bars is not None and not prior_bars.empty:
            atr = compute_atr(prior_bars, self.atr_lookback_bars)
        else:
            # Fallback: берём ATR на первых баров (с look-ahead bias на лучшую сторону,
            # но альтернатива — отсутствие ATR. Runner должен передавать prior_bars.)
            atr = compute_atr(bars.head(self.atr_lookback_bars), self.atr_lookback_bars)

        # Если ATR деградирует — используем fallback
        if atr <= 0:
            atr = trade.entry * ATR_FALLBACK_PCT

        trailing_step = self.trailing_multiplier * atr

        # Window-фильтр (как в baseline)
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
        current_sl = trade.sl_price
        trailing_active = False
        one_r_level = trade.one_r_level

        for ts, bar in window.iterrows():
            # 1. Проверка SL/TP с текущим SL уровнем (Phase 2 SL-first)
            hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, current_sl)
            if hit is not None:
                reason, exit_price = hit
                # Расширенный exit_reason
                if trailing_active and reason == "sl":
                    reason = "trailing"
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
                    notes=f"atr={atr:.5f}, trailing_active={trailing_active}",
                )

            # 2. Если SL/TP не сработали — проверяем активацию/обновление trailing
            bar_close = bar["close"]

            if not trailing_active:
                # Проверка активации: touched 1R?
                touched_1r = (
                    (trade.side == 1 and bar["high"] >= one_r_level)
                    or (trade.side == -1 and bar["low"] <= one_r_level)
                )
                if touched_1r:
                    trailing_active = True
                    # Первое подтягивание SL — на основе close этого бара
                    if trade.side == 1:
                        new_sl = bar_close - trailing_step
                        # Защита: не подтягиваем ниже entry (минимум — breakeven)
                        new_sl = max(new_sl, trade.entry)
                        current_sl = max(current_sl, new_sl)
                    else:
                        new_sl = bar_close + trailing_step
                        new_sl = min(new_sl, trade.entry)
                        current_sl = min(current_sl, new_sl)
            else:
                # Уже активирован — подтягиваем SL по close
                if trade.side == 1:
                    new_sl = bar_close - trailing_step
                    current_sl = max(current_sl, new_sl)  # SL только вверх
                else:
                    new_sl = bar_close + trailing_step
                    current_sl = min(current_sl, new_sl)  # SL только вниз

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
            notes=f"atr={atr:.5f}, trailing_active={trailing_active}, final_sl={current_sl:.4f}",
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
    print(f"Trade: SBER long @ 300, sl=298.65, tp=301.575, 1R={t.one_r_level}")

    # ATR test
    print("\n=== Test 0: compute_atr ===")
    sample_bars = pd.DataFrame({
        "open": [300, 300, 300, 300],
        "high": [301, 301, 301, 301],
        "low": [299, 299, 299, 299],
        "close": [300.5, 300.5, 300.5, 300.5],
    })
    atr = compute_atr(sample_bars, window=14)
    print(f"  ATR (constant 2.0 range): {atr:.4f}  (expected ~2.0)")
    assert abs(atr - 2.0) < 0.1

    # Prior bars для ATR
    prior = pd.DataFrame({
        "open":  [299.5] * 14,
        "high":  [300.0] * 14,
        "low":   [299.0] * 14,
        "close": [299.8] * 14,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 9, 46) + pd.Timedelta(minutes=m) for m in range(14)]
    ))
    # ATR ~= 1.0 (high-low = 1.0 каждую минуту)
    # trailing_step = 0.5 * 1.0 = 0.5

    s = TrailingAfter1R()

    # Test 1: цена пошла к 1R, потом откатилась — trailing exit
    print("\n=== Test 1: 1R touched, pullback -> trailing exit ===")
    bars1 = pd.DataFrame({
        "open":  [300.0, 301.0, 301.4, 301.0, 300.4],
        "high":  [300.3, 301.4, 301.5, 301.1, 300.5],   # bar 2: 1R touched (1R=301.35)
        "low":   [299.8, 300.8, 301.1, 300.5, 300.0],   # bar 5: low=300.0
        "close": [300.2, 301.2, 301.3, 300.7, 300.2],   # bar 2 close=301.2 → trailing SL = 301.2-0.5 = 300.7, max(298.65, 300.7) = 300.7
        "vol":   [1000]*5,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3, 4, 5)], name="ts"
    ))
    r = s.simulate(t, bars1, prior_bars=prior)
    print(f"  reason: {r.exit_reason}  exit: {r.exit_price}  realized_r: {r.realized_r:.4f}")
    print(f"  notes: {r.notes}")
    # bar 2: 1R touched, close=301.2, new_sl=301.2-0.5=300.7. max(298.65, 300.7)=300.7
    # bar 3: close=301.3, new_sl=301.3-0.5=300.8. max(300.7, 300.8)=300.8
    # bar 4: close=300.7, new_sl=300.7-0.5=300.2. max(300.8, 300.2)=300.8 (только вверх)
    # bar 4: low=300.5, current_sl=300.8 → low=300.5 < 300.8 → trailing SL hit!
    # Реально smell-test: low бара 4 = 300.5, SL после bar 3 = 300.8 → hit
    assert r.exit_reason == "trailing", f"Got {r.exit_reason}"

    # Test 2: Trailing не активирован — цена пошла прямо к SL
    print("\n=== Test 2: Direct SL, no 1R touch ===")
    bars2 = pd.DataFrame({
        "open":  [300.0, 299.0],
        "high":  [300.1, 299.3],
        "low":   [299.0, 298.5],
        "close": [299.2, 298.7],
        "vol":   [1000]*2,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2)], name="ts"
    ))
    r = s.simulate(t, bars2, prior_bars=prior)
    print(f"  reason: {r.exit_reason}  (expected: sl, NOT trailing)")
    assert r.exit_reason == "sl"

    # Test 3: цена пошла прямо к TP
    print("\n=== Test 3: Straight to TP ===")
    bars3 = pd.DataFrame({
        "open":  [300.0, 301.0],
        "high":  [301.0, 301.8],   # bar 2: TP hit. Но bar 1 уже touched 1R=301.35? нет, high=301
        "low":   [299.9, 300.8],
        "close": [300.8, 301.6],
        "vol":   [1000]*2,
    }, index=pd.DatetimeIndex(
        [datetime(2025, 6, 15, 10, m) for m in (1, 2)], name="ts"
    ))
    r = s.simulate(t, bars3, prior_bars=prior)
    print(f"  reason: {r.exit_reason}  exit: {r.exit_price}")
    assert r.exit_reason == "tp"

    print("\nAll trailing tests passed")
