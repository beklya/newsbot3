"""
Sprint 4 / Commit 4.0 - Стратегия #1: BaselineFixedTpSl (REV4)
================================================================

REVISION 4 (после window diagnostic):

Точная имитация Phase 2 backtest_mfe.py:359-403:
  idx_start = candles.index.searchsorted(entry_ts)         # включает entry-bar
  idx_end = candles.index.searchsorted(end_ts) + 1         # +1 бар после time-stop
  window = candles.iloc[idx_start:idx_end]

Diagnostic подтвердил: 1.6% сделок имеют SL touch в entry-минуте,
0.2% — TP touch. Это и есть остаточные 3.33% diff в sanity check.

Дизайн:
  - Параметры simulate(): entry_bar_inclusive, after_time_stop_bar_inclusive
  - Default = Phase 2 reproducibility (оба True)
  - Для production-ready симуляции рекомендуется False/False
    (это исключает look-ahead bias на entry-минуте)
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

log = logging.getLogger(__name__)


# =============================================================================
# Конвенции window
# =============================================================================
# Phase 2 reproducibility: оба True
WINDOW_PHASE2 = {"entry_bar_inclusive": True, "after_time_stop_bar_inclusive": True}
# Production-ready (без look-ahead): оба False
WINDOW_PROD = {"entry_bar_inclusive": False, "after_time_stop_bar_inclusive": False}


class BaselineFixedTpSl(ExitStrategy):
    """Phase 2 simulate_trade. Default reproduces Phase 2 точь-в-точь."""

    name = "baseline_fixed_tp_sl"

    def __init__(
        self,
        entry_bar_inclusive: bool = True,
        after_time_stop_bar_inclusive: bool = True,
    ):
        """
        Args:
            entry_bar_inclusive: проверяется ли TP/SL в баре entry-минуты.
                True (Phase 2): да, low/high бара проверяются.
                    -> в 1.6% сделок может срабатывать SL "ретроспективно"
                False (production): нет, exit-проверка начинается с ts_open+1.
                    -> более реалистично для live торговли
            after_time_stop_bar_inclusive: учитывается ли бар сразу после time-stop.
                True (Phase 2): да, idx_end + 1 в Phase 2 — последний проверяемый бар.
                False (production): нет, time-exit = close ровно на ts_open + horizon.
        """
        self.entry_bar_inclusive = entry_bar_inclusive
        self.after_time_stop_bar_inclusive = after_time_stop_bar_inclusive

    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        """
        Args:
            bars: должны включать ts_open (если entry_bar_inclusive=True) и
                  до ts_open + horizon + 1 минута (если after_time_stop_bar=True).
                  Передаваемое окно должно быть с запасом — мы сами режем.
        """
        if bars.empty:
            return self._no_bars_result(trade)

        # Phase 2 window logic:
        #   start: searchsorted(entry_ts) -> первый бар с index >= entry_ts
        #   end:   searchsorted(end_ts) + 1 -> один бар после time-stop
        end_ts = trade.ts_open + pd.Timedelta(minutes=trade.horizon_min)

        # Левая граница
        if self.entry_bar_inclusive:
            window = bars[bars.index >= trade.ts_open]
        else:
            window = bars[bars.index > trade.ts_open]

        # Правая граница
        if self.after_time_stop_bar_inclusive:
            # Бар после time-stop тоже включаем: первый бар с index > end_ts тоже OK
            # Phase 2: searchsorted(end_ts) + 1 = first_index_after(end_ts) включён
            # Это значит: ВКЛЮЧИТЬ все бары с index <= end_ts ПЛЮС один следующий бар.
            mask_within = window.index <= end_ts
            n_within = int(mask_within.sum())
            n_total = len(window)
            if n_total > n_within:
                # Есть хотя бы один бар после end_ts -> берём первые (n_within + 1) баров
                window = window.iloc[: n_within + 1]
            else:
                # Баров после end_ts нет -> берём то что есть
                window = window  # уже отфильтровано выше как нужно
        else:
            window = window[window.index <= end_ts]

        if window.empty:
            return self._no_bars_result(trade, "window empty after filtering")

        # Bar-by-bar walk -- SL first внутри check_tp_sl_hit (Phase 2 logic)
        for ts, bar in window.iterrows():
            hit = check_tp_sl_hit(bar, trade.side, trade.tp_price, trade.sl_price)
            if hit is not None:
                reason, exit_price = hit
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

        # Time-exit: close ПОСЛЕДНЕГО бара в window
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
        )

    def _no_bars_result(self, trade: Trade, note: str = "no bars") -> ExitResult:
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

    # Phase 2 reproducibility test — entry bar inclusive
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

    # Test 1: SL touched в entry-баре
    # Phase 2 mode (default) — должен поймать SL
    # Production mode — должен идти дальше и взять TP/time
    print("\n=== Test 1: SL touch in entry bar — Phase 2 vs Production ===")
    bars = pd.DataFrame(
        {
            "open":  [300.0, 300.5, 301.0, 301.5],
            "high":  [300.3, 301.0, 301.5, 301.8],
            "low":   [298.5, 300.4, 300.9, 301.3],   # ENTRY бар low=298.5 < sl=298.65
            "close": [300.2, 300.8, 301.3, 301.6],
            "vol":   [1000, 1000, 1000, 1000],
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, m) for m in (0, 1, 2, 3)],
            name="ts",
        ),
    )

    strategy_p2 = BaselineFixedTpSl()  # default: Phase 2
    r_p2 = strategy_p2.simulate(t, bars)
    print(f"  Phase 2 mode:   reason={r_p2.exit_reason}  price={r_p2.exit_price}  ts_close={r_p2.ts_close}")
    assert r_p2.exit_reason == "sl", f"Phase 2 mode should catch SL, got {r_p2.exit_reason}"

    strategy_prod = BaselineFixedTpSl(entry_bar_inclusive=False, after_time_stop_bar_inclusive=False)
    r_prod = strategy_prod.simulate(t, bars)
    print(f"  Production mode: reason={r_prod.exit_reason}  price={r_prod.exit_price}  ts_close={r_prod.ts_close}")
    assert r_prod.exit_reason == "tp", f"Production mode should catch TP, got {r_prod.exit_reason}"

    # Test 2: TP in bar AFTER time-stop
    print("\n=== Test 2: TP touch in bar AFTER time-stop ===")
    # ts_open=10:00, horizon=60, end_ts=11:00
    # Создаём 62 бара: ts 10:01..11:02
    # До 11:00 цена в коридоре, на 11:01 (после end_ts) — TP touch
    bars_after = pd.DataFrame(
        {
            "open":  [300.0] * 62,
            "high":  [300.3] * 60 + [302.0, 302.0],   # бар 11:01 high=302>tp=301.575
            "low":   [299.7] * 62,
            "close": [300.0] * 60 + [301.8, 301.9],
            "vol":   [1000] * 62,
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, 1) + pd.Timedelta(minutes=m) for m in range(62)],
            name="ts",
        ),
    )
    r_p2 = strategy_p2.simulate(t, bars_after)
    print(f"  Phase 2 mode:   reason={r_p2.exit_reason}  price={r_p2.exit_price}  ts_close={r_p2.ts_close}")
    # В Phase 2 mode после time-stop проверяется ещё 1 бар (11:01)
    # На баре 11:01 high=302 >= tp=301.575 -> TP
    assert r_p2.exit_reason == "tp", f"Phase 2 mode should catch TP in after-bar, got {r_p2.exit_reason}"

    r_prod = strategy_prod.simulate(t, bars_after)
    print(f"  Production mode: reason={r_prod.exit_reason}  price={r_prod.exit_price}  ts_close={r_prod.ts_close}")
    # В production mode time-exit = close на 11:00
    assert r_prod.exit_reason == "time"

    # Test 3: Обычный TP в середине window (должен работать одинаково)
    print("\n=== Test 3: Normal TP hit (no boundary issues) ===")
    bars_normal = pd.DataFrame(
        {
            "open":  [300.5, 300.8, 301.2, 301.5],
            "high":  [300.9, 301.1, 301.6, 301.8],
            "low":   [300.3, 300.7, 301.0, 301.3],
            "close": [300.8, 301.0, 301.5, 301.6],
            "vol":   [1000, 1200, 1500, 1100],
        },
        index=pd.DatetimeIndex(
            [datetime(2025, 6, 15, 10, m) for m in (1, 2, 3, 4)],
            name="ts",
        ),
    )
    r_p2 = strategy_p2.simulate(t, bars_normal)
    r_prod = strategy_prod.simulate(t, bars_normal)
    print(f"  Phase 2 mode:   reason={r_p2.exit_reason} @ {r_p2.exit_price}")
    print(f"  Production mode: reason={r_prod.exit_reason} @ {r_prod.exit_price}")
    assert r_p2.exit_reason == "tp"
    assert r_prod.exit_reason == "tp"
    assert abs(r_p2.exit_price - r_prod.exit_price) < 1e-6

    print("\nAll baseline tests passed (Phase 2 + Production modes)")
