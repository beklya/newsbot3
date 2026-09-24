"""
Sanity check: согласованность sl_price/tp_price с pred_*_pct в trades.parquet.

Цель: убедиться, что наша интерпретация Phase 2 формулы корректна:
  sl_price = entry × (1 - side × pred_mae_pct × 1.2 / 100)
  tp_price = entry × (1 + side × pred_mfe_pct × 0.7 / 100)

Если совпадает — пишем trades_loader спокойно.
Если расходится — выясняем, какая реальная формула в Phase 2.
"""

from pathlib import Path

import numpy as np
import pandas as pd

TRADES_PATH = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)

TP_FRACTION = 0.7
SL_BUFFER = 1.2


def main() -> None:
    print(f"Loading {TRADES_PATH.name}...")
    df = pd.read_parquet(TRADES_PATH)
    print(f"  {len(df):,} rows")

    # Берём sample из best combo для чистоты эксперимента
    best = df[
        (df["horizon_min"] == 60)
        & (df["rr_threshold"] == 2.0)
        & (df["model_type"] == "mx_specific")
    ].copy()
    print(f"  Best combo (h=60, rr=2.0, mx_specific): {len(best):,} trades")

    if len(best) == 0:
        print("  [ERR] Empty — проверь фильтр (возможно другие значения rr_threshold).")
        print(f"  Unique horizon_min:  {sorted(df['horizon_min'].unique())}")
        print(f"  Unique rr_threshold: {sorted(df['rr_threshold'].unique())}")
        print(f"  Unique model_type:   {sorted(df['model_type'].unique())}")
        return

    # Вычисляем ожидаемые уровни по нашей формуле
    side = best["side"]
    entry = best["entry"]
    pred_mfe = best["pred_mfe_pct"]
    pred_mae = best["pred_mae_pct"]

    expected_sl = entry * (1 - side * pred_mae * SL_BUFFER / 100)
    expected_tp = entry * (1 + side * pred_mfe * TP_FRACTION / 100)

    # Сравнение
    sl_diff = (best["sl_price"] - expected_sl).abs()
    tp_diff = (best["tp_price"] - expected_tp).abs()
    # Относительная погрешность (в долях цены)
    sl_diff_rel = sl_diff / entry
    tp_diff_rel = tp_diff / entry

    print()
    print("=" * 60)
    print("  SL_PRICE vs формула (1 - side × pred_mae × 1.2 / 100)")
    print("=" * 60)
    print(f"  max abs diff:  {sl_diff.max():.6f}")
    print(f"  max rel diff:  {sl_diff_rel.max():.8f}  ({sl_diff_rel.max()*100:.6f}%)")
    print(f"  p99 rel diff:  {sl_diff_rel.quantile(0.99):.8f}")
    print(f"  median rel:    {sl_diff_rel.median():.8f}")

    print()
    print("=" * 60)
    print("  TP_PRICE vs формула (1 + side × pred_mfe × 0.7 / 100)")
    print("=" * 60)
    print(f"  max abs diff:  {tp_diff.max():.6f}")
    print(f"  max rel diff:  {tp_diff_rel.max():.8f}  ({tp_diff_rel.max()*100:.6f}%)")
    print(f"  p99 rel diff:  {tp_diff_rel.quantile(0.99):.8f}")
    print(f"  median rel:    {tp_diff_rel.median():.8f}")

    # Вердикт
    print()
    print("=" * 60)
    print("  ВЕРДИКТ")
    print("=" * 60)
    threshold = 1e-6  # 0.0001% — sub-цент погрешность округления
    if sl_diff_rel.max() < threshold and tp_diff_rel.max() < threshold:
        print("  ✓ Формула SL/TP полностью согласована (< 0.0001% погрешности)")
        print("  → Можно писать trades_loader с этой формулой")
    elif sl_diff_rel.max() < 1e-4 and tp_diff_rel.max() < 1e-4:
        print("  ⚠ Небольшое расхождение (~ floating point precision)")
        print("  → Формула в целом верна, есть рассинхрон в округлении")
    else:
        print("  🔴 Существенное расхождение — формула в Phase 2 другая!")
        print()
        print("  Показываем 5 примеров с самым большим расхождением SL:")
        worst = best.iloc[sl_diff_rel.argsort()[::-1][:5]]
        for _, row in worst.iterrows():
            calc_sl = row["entry"] * (1 - row["side"] * row["pred_mae_pct"] * SL_BUFFER / 100)
            print(
                f"    {row['ticker']:<6} side={row['side']:+d} "
                f"entry={row['entry']:.4f}  pred_mae={row['pred_mae_pct']:.4f}%  "
                f"sl_actual={row['sl_price']:.4f}  sl_calc={calc_sl:.4f}  "
                f"diff={row['sl_price']-calc_sl:+.4f}"
            )

    # Дополнительная диагностика: сколько в этой выборке тикеров и каких
    print()
    print("Распределение тикеров в best combo (h=60, rr=2.0, mx_specific):")
    vc = best["ticker"].value_counts()
    for t, n in vc.items():
        pct = 100 * n / len(best)
        print(f"  {t:<8s} {n:>6,}  ({pct:>5.1f}%)")

    # Также: какой реализованный Sharpe на этом срезе?
    # Грубый Sharpe (daily) для понимания baseline
    print()
    print("Baseline Phase 2 metrics на этом срезе (для будущей sanity):")
    pnl = best["net_pnl_rub"]
    print(f"  trades:        {len(best):,}")
    print(f"  total_pnl:     {pnl.sum():>15,.0f}")
    print(f"  mean_pnl:      {pnl.mean():>15.2f}")
    print(f"  win_rate:      {(pnl > 0).mean()*100:>14.1f}%")
    print(f"  by exit_reason:")
    for r, n in best["exit_reason"].value_counts().items():
        print(f"    {r:<6s} {n:>6,}  ({100*n/len(best):>5.1f}%)")


if __name__ == "__main__":
    main()
