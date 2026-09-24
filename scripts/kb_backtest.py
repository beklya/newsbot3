r"""Sprint 9 Фаза 4 — бэктест оценок на досье v2 + GATE.

Переиспустит механику dossier_backtest (outcomes_for/load_evals/bucket_stats,
кости), но читает kb-чекпоинты (kb_evals_*) и пишет в data/kb/report.md.
Сравнивает honest vs shuffle, выводит автоматический gate-вердикт.

GATE (Sprint 9, зафиксирован в плане):
  genuine_surprise @лучший-горизонт: hit ≥55% И > shuffle на ≥5пп И net>0
  И плюс в ≥2/3 лет И n≥40. Промежуточный: 52–55% И >shuffle+3пп И >Sprint8 42.5%.

Usage: python scripts/kb_backtest.py --model-slug DeepSeek_R1
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from prices_cache import PricesCache  # noqa: E402
from scripts.dossier_backtest import (  # noqa: E402
    outcomes_for, load_evals, bucket_stats, RT_STOCK, FUND_DAY, HORIZONS,
)

KB = PROJECT_ROOT / "data" / "kb"
KB_CK = KB / "checkpoints"
SPRINT8_GENUINE_EOD_HIT = 42.5  # baseline для сравнения


def process(df: pd.DataFrame, cache: PricesCache) -> pd.DataFrame:
    oc_cache: dict = {}
    cols = {h: [] for h in HORIZONS}
    nights = []
    for r in df.itertuples(index=False):
        key = (r.ticker, r.dt)
        if key not in oc_cache:
            oc_cache[key] = outcomes_for(cache, r.ticker, r.dt)
        oc = oc_cache[key]
        for h in HORIZONS:
            cols[h].append(oc[h] if oc else None)
        nights.append(oc.get("t1_nights", 0) if oc else 0)
    for h in HORIZONS:
        df[f"ret_{h}"] = cols[h]
    df["t1_nights"] = nights
    d = df["direction"].map({"long": 1.0, "short": -1.0, "neutral": 0.0})
    t = df[d != 0].copy()
    dd = d[d != 0]
    for h in HORIZONS:
        gross = t[f"ret_{h}"] * dd
        cost = RT_STOCK + np.where((h == "t1") & (dd < 0), FUND_DAY * t["t1_nights"], 0.0)
        t[f"net_{h}"] = gross - cost
    return t


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-slug", default="DeepSeek_R1")
    args = ap.parse_args()
    cache = PricesCache(); cache.warmup()

    lines = [f"# Sprint 9 — KB-досье v2 бэктест ({args.model_slug})", ""]
    frames = {}
    for suffix in ("", "_shuffle"):
        p = KB_CK / f"kb_evals_{args.model_slug}{suffix}.jsonl"
        if not p.exists():
            continue
        df = load_evals(p)
        if df.empty:
            continue
        t = process(df, cache)
        frames[suffix or "honest"] = t
        label = "HONEST" if suffix == "" else "SHUFFLE-КОНТРОЛЬ"
        lines.append(f"## {label}: n_evals={len(df)}, traded={len(t)}")
        lines.append(f"levels: {df['surprise_level'].value_counts().to_dict()}")
        lines.append("")
        lines.append("| bucket | horizon | n | hit % | mean net % | median | years+ | top5 % |")
        lines.append("|" + "---|" * 8)
        for lvl in ("genuine_surprise", "priced", "confirmed"):
            sub = t[t["surprise_level"] == lvl]
            for h in HORIZONS:
                s = bucket_stats(sub, h)
                if s["n"] == 0:
                    continue
                lines.append(f"| {lvl} | {h} | {s['n']} | {s['hit']:.1f} | "
                             f"**{s['mean']:+.3f}** | {s['median']:+.3f} | "
                             f"{s['years_pos']}/{s['years_n']} | {s['top5']:.0f} |")
        lines.append("")

    # --- GATE: лучший горизонт genuine_surprise ---
    lines.append("## GATE (genuine_surprise)")
    lines.append("")
    if "honest" in frames:
        g = frames["honest"]
        g = g[g["surprise_level"] == "genuine_surprise"]
        sh = frames.get("_shuffle")
        sh = sh[sh["surprise_level"] == "genuine_surprise"] if sh is not None else None
        best = None
        for h in HORIZONS:
            s = bucket_stats(g, h)
            if s["n"] >= 40:
                sh_hit = bucket_stats(sh, h)["hit"] if sh is not None else None
                cand = {"h": h, **s, "sh_hit": sh_hit}
                if best is None or s["hit"] > best["hit"]:
                    best = cand
        if best is None:
            lines.append("❌ нет горизонта с n≥40")
        else:
            sh_hit = best["sh_hit"]
            checks = {
                "hit ≥55%": best["hit"] >= 55,
                "net >0": best["mean"] > 0,
                "n ≥40": best["n"] >= 40,
                "плюс в ≥2 лет": best["years_pos"] >= min(2, best["years_n"]),
                "vs shuffle ≥+5пп": sh_hit is not None and best["hit"] - sh_hit >= 5,
                f"> Sprint8 {SPRINT8_GENUINE_EOD_HIT}%": best["hit"] > SPRINT8_GENUINE_EOD_HIT,
            }
            for k, v in checks.items():
                lines.append(f"- {'✅' if v else '❌'} {k}")
            lines.append("")
            lines.append(f"Лучший горизонт **{best['h']}**: n={best['n']}, hit={best['hit']:.1f}%, "
                         f"net={best['mean']:+.3f}%, by_year={best['by_year']}, "
                         f"shuffle_hit={f'{sh_hit:.1f}%' if sh_hit is not None else 'n/a'}")
            lines.append("")
            full = all(checks.values())
            promising = (52 <= best["hit"] < 55 and best["hit"] > SPRINT8_GENUINE_EOD_HIT
                         and sh_hit is not None and best["hit"] - sh_hit >= 3)
            lines.append("## " + ("✅ GATE PASSED" if full else
                                  "🟡 PROMISING (доинвест оправдан)" if promising else
                                  "❌ GATE NOT PASSED"))
        frames["honest"].to_parquet(KB / "kb_eval_trades.parquet")

    out = KB / "report.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nSaved: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
