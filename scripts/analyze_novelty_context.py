r"""Sprint 6.5 — тест идеи «контекст/накопленная статистика» (автор,
2026-06-11): отличаются ли НОВЫЕ события (surprise vs накопленный контекст)
от ЭХА по направленности и размеру хода?

novelty per (event,ticker):
  gap_days — дней с предыдущей новости этого тикера
  max_sim  — максимальный Jaccard-overlap токенов headline с предыдущими
             новостями тикера за 14 дней (0 = ничего похожего не было)

Buckets: NOVEL (gap≥3d или max_sim<0.15) / FRESH (sim 0.15–0.4) / ECHO (≥0.4).
Метрики: range_eod, P(≥1%), LLM-direction hit-rate, mean ret_dir.

Gate: NOVEL-подмножество с hit ≥58% на n≥100 → контекст-слой стоит строить.

Usage: python scripts/analyze_novelty_context.py
"""
from __future__ import annotations

import re
import sys
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from scripts.analyze_event_impact import explode_corpus, try_normalize_ticker  # noqa: E402

WORD_RE = re.compile(r"[а-яa-z]{4,}")


def tokens(text: str) -> frozenset:
    return frozenset(WORD_RE.findall(text.lower()))


def main() -> int:
    corpus = PROJECT_ROOT / "data/reenrich_phase2/y6_corpus_70b.parquet"
    # headlines нужны отдельно (explode_corpus их не несёт)
    heads = pd.read_parquet(corpus, columns=["id", "headline"])
    heads["id"] = heads["id"].astype(str)
    id2head = dict(zip(heads["id"], heads["headline"].fillna("")))

    llm = explode_corpus(corpus)
    llm["head_tokens"] = llm["id"].map(lambda i: tokens(id2head.get(str(i), "")))
    llm = llm.sort_values("dt").reset_index(drop=True)

    # --- novelty pass: per ticker rolling 14d window of (dt, tokens) ---
    hist: dict[str, deque] = {}
    gap_days = np.full(len(llm), np.nan)
    max_sim = np.zeros(len(llm))
    for i, r in enumerate(llm.itertuples(index=False)):
        dq = hist.setdefault(r.ticker, deque())
        cutoff = r.dt - pd.Timedelta(days=14)
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        if dq:
            gap_days[i] = (r.dt - dq[-1][0]).total_seconds() / 86400
            tk = r.head_tokens
            if tk:
                best = 0.0
                for _, prev in dq:
                    if not prev:
                        continue
                    inter = len(tk & prev)
                    if inter:
                        s = inter / len(tk | prev)
                        if s > best:
                            best = s
                max_sim[i] = best
        dq.append((r.dt, r.head_tokens))
    llm["gap_days"] = gap_days
    llm["max_sim"] = max_sim

    # --- outcomes join ---
    tgt = pd.read_parquet(PROJECT_ROOT / "data/reenrich_phase2/targets_multi_horizon.parquet")
    tgt["id"] = tgt["id"].astype(str)
    tgt["ticker"] = tgt["ticker"].map(lambda x: try_normalize_ticker(str(x)) or x)
    llm["id"] = llm["id"].astype(str)
    df = llm.merge(tgt, on=["id", "ticker"], how="inner")
    df["range"] = df[["mfe_long_eod", "mfe_short_eod"]].max(axis=1)
    df = df[df["range"].notna() & df["ret_eod"].notna()].copy()
    dir_sign = df["direction"].map({"long": 1.0, "short": -1.0}).fillna(0.0)
    df["ret_dir"] = df["ret_eod"] * dir_sign
    df["hitable"] = dir_sign != 0
    df["dir_ok"] = (np.sign(df["ret_eod"]) == dir_sign) & df["hitable"]

    def bucket(r):
        if np.isnan(r["gap_days"]) or r["gap_days"] >= 3 or r["max_sim"] < 0.15:
            return "NOVEL"
        return "ECHO" if r["max_sim"] >= 0.40 else "FRESH"
    df["bucket"] = df.apply(bucket, axis=1)

    print(f"(event,ticker) pairs: {len(df)}")
    print(df["bucket"].value_counts().to_string())
    print()
    hdr = f"{'bucket':>7} {'n':>6} {'range p50':>9} {'P(>=1%)':>8} {'P(>=2%)':>8} {'hit |ret|>=1%':>13} {'mean ret_dir':>12}"
    for sub_label, sub in (("ALL", df),
                           ("dir!=neutral", df[df["hitable"]]),
                           ("hard-magnitude (range>=1%)", df[df["range"] >= 1.0])):
        print(f"--- {sub_label} ---")
        print(hdr)
        for b in ("NOVEL", "FRESH", "ECHO"):
            g = sub[sub["bucket"] == b]
            if g.empty:
                continue
            big = g[(g["ret_eod"].abs() >= 1.0) & g["hitable"]]
            hit = big["dir_ok"].mean() * 100 if len(big) >= 20 else np.nan
            print(f"{b:>7} {len(g):>6} {g['range'].median():>8.2f}% "
                  f"{(g['range'] >= 1).mean() * 100:>7.1f}% "
                  f"{(g['range'] >= 2).mean() * 100:>7.1f}% "
                  f"{(f'{hit:>12.1f}%' if not np.isnan(hit) else '        n<20')} "
                  f"{g[g['hitable']]['ret_dir'].mean():>+11.3f}%")
        print()

    # extreme novelty: первая новость тикера за 7+ дней
    g = df[df["gap_days"].isna() | (df["gap_days"] >= 7)]
    big = g[(g["ret_eod"].abs() >= 1.0) & g["hitable"]]
    print(f"EXTREME novel (gap>=7d): n={len(g)}, range p50 {g['range'].median():.2f}%, "
          f"hit={big['dir_ok'].mean() * 100 if len(big) >= 10 else float('nan'):.1f}% "
          f"(n_big={len(big)}), mean ret_dir {g[g['hitable']]['ret_dir'].mean():+.3f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
