r"""Counterfactual «бесконечной скорости»: вход ДО поступления новости.

Для каждого hard-event (2022-2026, first-report) три входа:
  E-1: open бара ЗА ОДИН до новостного (60–120 сек до новости) — НЕторгуемо,
       диагностика: какой PnL достался бы тому, кто знал новость заранее.
  E0 : open бара новостной минуты (0–60 сек до публикации).
  E+1: open следующего бара (наш стандарт, 0–60 сек после).

Выход у всех: EOD close (последний бар ≤18:45). ret_dir — в сторону правила
типа. news_jump = (E+1/E0 − 1) в сторону правила — ход «сквозь» минуту новости.

Usage: python scripts/analyze_prenews_entry.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from prices_cache import PricesCache  # noqa: E402
from scripts.hard_events_pilot import extract_events_jsonl, dedup_first_report  # noqa: E402


def main() -> int:
    ev = extract_events_jsonl(Path(r"D:\quik_sber\newsbot\telegram_news.jsonl"),
                              "2022-01-01", "2026-06-06")
    ev = dedup_first_report(ev)
    print(f"events: {len(ev)}")

    cache = PricesCache()
    cache.warmup()
    rows = []
    for r in ev.itertuples(index=False):
        try:
            bars = cache.get_bars(r.ticker, r.dt - pd.Timedelta(minutes=10),
                                  r.dt + pd.Timedelta(hours=11))
        except Exception:
            continue
        if bars is None or len(bars) < 3:
            continue
        nm0 = r.dt.floor("min")
        i_news = bars.index.searchsorted(nm0, side="right") - 1
        if i_news < 1:
            continue
        # пропускаем ночные/гэповые события: новостной бар должен быть «живым»
        if (nm0 - bars.index[i_news]).total_seconds() > 300:
            continue
        i_next = bars.index.searchsorted(
            (r.dt + pd.Timedelta(seconds=60)).floor("min"))
        if i_next >= len(bars):
            continue
        e_m1 = float(bars["open"].iloc[i_news - 1])
        e_0 = float(bars["open"].iloc[i_news])
        e_p1 = float(bars["open"].iloc[i_next])
        if min(e_m1, e_0, e_p1) <= 0:
            continue
        t0 = bars.index[i_next]
        eod = t0.normalize() + pd.Timedelta(hours=18, minutes=45)
        ie = bars.index.searchsorted(eod, side="right") - 1
        if ie <= i_next:
            continue
        exit_px = float(bars["close"].iloc[ie])
        d = 1 if r.side == "BUY" else -1
        rows.append({
            "etype": r.etype, "ticker": r.ticker, "dt": r.dt,
            "ret_m1": d * (exit_px / e_m1 - 1) * 100,
            "ret_0": d * (exit_px / e_0 - 1) * 100,
            "ret_p1": d * (exit_px / e_p1 - 1) * 100,
            "news_jump": d * (e_p1 / e_0 - 1) * 100,
        })
    df = pd.DataFrame(rows)
    print(f"with prices: {len(df)}\n")

    hdr = (f"{'type':>10} {'n':>5} | {'E-1 mean':>9} {'hit':>4} | "
           f"{'E0 mean':>8} {'hit':>4} | {'E+1 mean':>9} {'hit':>4} | "
           f"{'jump новостной минуты':>22}")
    print(hdr)
    print("-" * len(hdr))

    def line(label, g):
        print(f"{label:>10} {len(g):>5} | {g['ret_m1'].mean():>+9.3f} "
              f"{(g['ret_m1'] > 0).mean() * 100:>3.0f}% | "
              f"{g['ret_0'].mean():>+8.3f} {(g['ret_0'] > 0).mean() * 100:>3.0f}% | "
              f"{g['ret_p1'].mean():>+9.3f} {(g['ret_p1'] > 0).mean() * 100:>3.0f}% | "
              f"{g['news_jump'].mean():>+12.4f} (med {g['news_jump'].median():+.4f})")

    for et, g in df.groupby("etype"):
        if len(g) >= 10:
            line(et, g)
    line("ALL", df)
    out = PROJECT_ROOT / "data/walk_forward/prenews_entry_report.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
