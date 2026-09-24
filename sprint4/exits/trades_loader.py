"""
Sprint 4 / Commit 4.0 — Загрузчик сделок Phase 2
==================================================

Загружает phase2_mfe_trades.parquet, фильтрует по grid-cell, нормализует
имена тикеров и превращает строки DataFrame в объекты `Trade`.

Default фильтр: best Phase 2 combo (h=60, rr=2.0, mx_specific) — 3,300 сделок,
PnL ≈ +2.09M ₽. Этот срез используем как baseline для exits comparison.

Использование:
  from trades_loader import load_trades_best_combo
  trades = load_trades_best_combo()  # list[Trade]
  print(f"Loaded {len(trades)} trades")
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from base import Trade
from instruments import normalize_ticker

log = logging.getLogger(__name__)


TRADES_PATH = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)

# Best Phase 2 combo (см. SPRINT2_MFE results + check_formulas.py output)
BEST_COMBO = {
    "horizon_min": 60,
    "rr_threshold": 2.0,
    "model_type": "mx_specific",
}


# =============================================================================
# Загрузка raw DataFrame
# =============================================================================
def load_raw_trades(path: Path = TRADES_PATH) -> pd.DataFrame:
    """
    Загружает весь parquet файл с phase2_mfe_trades. Без фильтрации.

    Возвращает DataFrame со всеми 359K строк и 22 колонками.
    """
    if not path.exists():
        raise FileNotFoundError(f"Trades parquet not found: {path}")
    log.info("Loading %s...", path.name)
    df = pd.read_parquet(path)
    log.info("  %d rows loaded", len(df))
    return df


def filter_combo(
    df: pd.DataFrame,
    horizon_min: int,
    rr_threshold: float,
    model_type: str,
) -> pd.DataFrame:
    """
    Фильтрует DataFrame по конкретной grid-cell.

    Phase 2 grid: 10 horizons × 4 rr × 2 models = 80 combos.
    Нас обычно интересует одна best combo.
    """
    mask = (
        (df["horizon_min"] == horizon_min)
        & (df["rr_threshold"] == rr_threshold)
        & (df["model_type"] == model_type)
    )
    filtered = df[mask].copy()
    log.info(
        "Filter (h=%d, rr=%.1f, %s): %d trades (%.1f%% of total)",
        horizon_min, rr_threshold, model_type,
        len(filtered), 100 * len(filtered) / len(df),
    )
    return filtered


# =============================================================================
# Конверсия строк → Trade objects
# =============================================================================
def row_to_trade(row: pd.Series) -> Trade:
    """
    Превращает одну строку DataFrame в объект Trade.

    Делает:
      - normalize_ticker (Si → SI, MX → MIX, YNDX → YDEX, GOLD → GLDRUB)
      - Конвертит datetime64 → python datetime (для совместимости с timedelta)
    """
    return Trade(
        ticker=normalize_ticker(row["ticker"]),
        fold=int(row["fold"]),
        horizon_min=int(row["horizon_min"]),
        rr_threshold=float(row["rr_threshold"]),
        model_type=str(row["model_type"]),
        ts_open=row["ts_open"].to_pydatetime() if hasattr(row["ts_open"], "to_pydatetime") else row["ts_open"],
        side=int(row["side"]),
        entry=float(row["entry"]),
        size_lots=int(row["size_lots"]),
        sl_price=float(row["sl_price"]),
        tp_price=float(row["tp_price"]),
        pred_mfe_pct=float(row["pred_mfe_pct"]),
        pred_mae_pct=float(row["pred_mae_pct"]),
        ts_close_phase2=row["ts_close"].to_pydatetime() if hasattr(row["ts_close"], "to_pydatetime") else row["ts_close"],
        exit_price_phase2=float(row["exit"]),
        exit_reason_phase2=str(row["exit_reason"]),
        net_pnl_rub_phase2=float(row["net_pnl_rub"]),
        cost_rub=float(row["cost_rub"]),
    )


def df_to_trades(df: pd.DataFrame) -> list[Trade]:
    """Конвертирует весь DataFrame в список Trade."""
    log.info("Converting %d rows to Trade objects...", len(df))
    trades = [row_to_trade(row) for _, row in df.iterrows()]
    log.info("  %d trades created", len(trades))
    return trades


# =============================================================================
# Public API — основные функции для использования из других модулей
# =============================================================================
def load_trades_best_combo(
    path: Path = TRADES_PATH,
    combo: Optional[dict] = None,
) -> list[Trade]:
    """
    Главная функция загрузки. Возвращает список Trade для best Phase 2 combo.

    Default combo: h=60, rr=2.0, mx_specific (3,300 сделок, +2.09M ₽).
    """
    if combo is None:
        combo = BEST_COMBO
    raw = load_raw_trades(path)
    filtered = filter_combo(raw, **combo)
    return df_to_trades(filtered)


def load_trades_filtered_df(
    path: Path = TRADES_PATH,
    combo: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Альтернатива: вернуть отфильтрованный DataFrame (для агрегаций/анализа).
    Тикеры нормализованы.
    """
    if combo is None:
        combo = BEST_COMBO
    raw = load_raw_trades(path)
    df = filter_combo(raw, **combo)
    df["ticker"] = df["ticker"].apply(normalize_ticker)
    return df


# =============================================================================
# CLI: быстрая инспекция load_trades_best_combo
# =============================================================================
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    print("=" * 70)
    print(f"  Loading best combo: {BEST_COMBO}")
    print("=" * 70)

    trades = load_trades_best_combo()
    print(f"\nLoaded {len(trades)} Trade objects")

    if not trades:
        print("[ERR] No trades loaded — check filter / parquet path")
        return

    # Sanity: первые 3 сделки
    print("\nFirst 3 trades:")
    for i, t in enumerate(trades[:3]):
        print(f"\n  [{i}] {t.ticker} {('long' if t.side == 1 else 'short')} "
              f"@ {t.entry:.4f}  size={t.size_lots}  ts={t.ts_open}")
        print(f"      sl={t.sl_price:.4f} (dist={t.sl_dist_pct*100:.4f}%)"
              f"  tp={t.tp_price:.4f} (dist={t.tp_dist_pct*100:.4f}%)"
              f"  1R={t.one_r_level:.4f}  rr_actual={t.rr_actual:.2f}")
        print(f"      Phase 2 exit: {t.exit_reason_phase2} @ {t.exit_price_phase2:.4f}"
              f"  pnl={t.net_pnl_rub_phase2:+.2f}")

    # Проверка нормализации тикеров
    print(f"\nTickers after normalization (top-5):")
    from collections import Counter
    c = Counter(t.ticker for t in trades)
    for t, n in c.most_common(5):
        print(f"  {t:<8s} {n:>5d}")

    # Aggregate sanity: должно совпасть с check_formulas.py output
    total_pnl = sum(t.net_pnl_rub_phase2 for t in trades)
    wins = sum(1 for t in trades if t.net_pnl_rub_phase2 > 0)
    print(f"\nAggregate sanity (matches check_formulas.py?):")
    print(f"  total_pnl:  {total_pnl:>15,.0f}   (expected ≈ 2,087,879)")
    print(f"  win_rate:   {100*wins/len(trades):>14.1f}%  (expected = 59.9%)")

    exit_dist = Counter(t.exit_reason_phase2 for t in trades)
    print(f"  exit_reason:")
    for r, n in sorted(exit_dist.items()):
        print(f"    {r:<6s} {n:>5d}  ({100*n/len(trades):>5.1f}%)")


if __name__ == "__main__":
    main()
