r"""Sprint 6.4 №1 — сколько РЕАЛЬНО даёт новость движения цены, и читается ли
это ex-ante из LLM-оценок.

Join: targets_multi_horizon (realized mfe/ret @ 120m/240m/eod/t1)
  × y6_corpus_70b per-(event,ticker) LLM fields (direction/confidence/
    impact_strength) + event fields (urgency/category/is_actionable/timeframe).

Метрики:
  range_h  = max(mfe_long_h, mfe_short_h)  — максимальный односторонний ход
  ret_h    — знаковый close-return (long convention)
  ret_dir  = ret в сторону LLM (long→+ret, short→−ret) — gross-прокси сделки
  hit-rate — sign(ret)==LLM direction на |ret|≥X% подвыборках

Usage: python scripts/analyze_event_impact.py
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

from instruments import normalize_ticker  # noqa: E402


def try_normalize_ticker(t: str) -> str | None:
    try:
        return normalize_ticker(t)
    except KeyError:
        return None

H_MAIN = "eod"
RANGE_THRESHOLDS = (0.3, 0.5, 1.0, 2.0)
MONTHS_SPAN = 17.2  # 2025-01-01 → 2026-06-06


def explode_corpus(path: Path) -> pd.DataFrame:
    c = pd.read_parquet(path, columns=[
        "id", "datetime_msk", "channel", "is_financial", "is_actionable",
        "urgency", "category", "expected_timeframe", "tickers"])
    c = c[c["is_financial"] == True]  # noqa: E712
    rows = []
    for r in c.itertuples(index=False):
        tk_list = r.tickers if r.tickers is not None else []
        for t in tk_list:
            if not isinstance(t, dict):
                continue
            tk = try_normalize_ticker((t.get("ticker") or "").strip())
            if tk is None:
                continue
            rows.append({
                "id": str(r.id), "ticker": tk, "dt": r.datetime_msk,
                "channel": r.channel, "urgency": r.urgency,
                "category": r.category, "is_actionable": bool(r.is_actionable),
                "timeframe": r.expected_timeframe,
                "direction": (t.get("direction") or "neutral").lower(),
                "confidence": float(t.get("confidence") or 0.0),
                "impact": float(t.get("impact_strength") or 0.0),
            })
    return pd.DataFrame(rows)


def bucket_table(df: pd.DataFrame, by: str, lines: list[str],
                 title: str | None = None) -> None:
    lines.append(f"### by {title or by}")
    lines.append("")
    lines.append("| bucket | n | range p50 % | range p90 % | P(≥0.5%) | P(≥1%) | "
                 "P(≥2%) | mean ret_dir % | hit(|ret|≥1%) |")
    lines.append("|" + "---|" * 9)
    for b, g in df.groupby(by, observed=True):
        big = g[g["ret_abs"] >= 1.0]
        hit = (big["dir_correct"].mean() * 100) if len(big) >= 10 else np.nan
        lines.append(
            f"| {b} | {len(g)} | {g['range'].median():.3f} | "
            f"{g['range'].quantile(0.9):.3f} | "
            f"{(g['range'] >= 0.5).mean() * 100:.1f}% | "
            f"{(g['range'] >= 1.0).mean() * 100:.1f}% | "
            f"{(g['range'] >= 2.0).mean() * 100:.1f}% | "
            f"{g['ret_dir'].mean():+.3f} | "
            f"{(f'{hit:.0f}%' if not np.isnan(hit) else 'n<10')} |"
        )
    lines.append("")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path,
                    default=PROJECT_ROOT / "data/reenrich_phase2/y6_corpus_70b.parquet")
    ap.add_argument("--targets", type=Path,
                    default=PROJECT_ROOT / "data/reenrich_phase2/targets_multi_horizon.parquet")
    ap.add_argument("--out", type=Path,
                    default=PROJECT_ROOT / "data/walk_forward/event_impact_report.md")
    args = ap.parse_args()

    llm = explode_corpus(args.corpus)
    tgt = pd.read_parquet(args.targets)
    tgt["id"] = tgt["id"].astype(str)
    tgt["ticker"] = tgt["ticker"].map(lambda x: try_normalize_ticker(str(x)) or x)

    df = llm.merge(tgt, on=["id", "ticker"], how="inner")
    h = H_MAIN
    df["range"] = df[[f"mfe_long_{h}", f"mfe_short_{h}"]].max(axis=1)
    df["ret"] = df[f"ret_{h}"]
    df = df[df["range"].notna() & df["ret"].notna()].copy()
    df["ret_abs"] = df["ret"].abs()
    dir_sign = df["direction"].map({"long": 1.0, "short": -1.0}).fillna(0.0)
    df["ret_dir"] = df["ret"] * dir_sign
    df["dir_correct"] = (np.sign(df["ret"]) == dir_sign) & (dir_sign != 0)

    n = len(df)
    lines = [f"# Event impact study — realized {h.upper()} move vs LLM markers", ""]
    lines.append(f"(event,ticker) pairs joined: {n} (Y6 corpus 2025-01→2026-06, "
                 f"{MONTHS_SPAN:.0f} мес). range = max(mfe_long, mfe_short) @ {h}.")
    lines.append("")
    q = df["range"].quantile([0.5, 0.75, 0.9, 0.95, 0.99])
    lines.append("## Unconditional: сколько вообще даёт новость")
    lines.append("")
    lines.append(f"- range {h} квантили: p50 **{q[0.5]:.2f}%**, p75 {q[0.75]:.2f}%, "
                 f"p90 **{q[0.9]:.2f}%**, p95 {q[0.95]:.2f}%, p99 **{q[0.99]:.2f}%**")
    for thr in RANGE_THRESHOLDS:
        cnt = int((df["range"] >= thr).sum())
        lines.append(f"- ход ≥{thr}%: {cnt} пар ({cnt / n * 100:.1f}%), "
                     f"≈{cnt / MONTHS_SPAN:.0f}/мес")
    big = df[df["ret_abs"] >= 1.0]
    lines.append(f"- |ret_{h}| ≥1%: {len(big)} пар; LLM direction hit-rate на них: "
                 f"**{big['dir_correct'].mean() * 100:.1f}%** "
                 f"(neutral={100 * (big['direction'] == 'neutral').mean():.0f}% исключён из hit)")
    lines.append("")

    lines.append("## Conditional: читается ли крупный ход ex-ante")
    lines.append("")
    df["impact_b"] = pd.cut(df["impact"], [-0.01, 0.3, 0.5, 0.7, 1.01],
                            labels=["0–0.3", "0.3–0.5", "0.5–0.7", "0.7–1.0"])
    df["conf_b"] = pd.cut(df["confidence"], [-0.01, 0.5, 0.7, 0.85, 1.01],
                          labels=["0–0.5", "0.5–0.7", "0.7–0.85", "0.85–1.0"])
    bucket_table(df, "impact_b", lines, "impact_strength")
    bucket_table(df, "conf_b", lines, "confidence")
    bucket_table(df, "urgency", lines)
    bucket_table(df, "is_actionable", lines)
    bucket_table(df, "timeframe", lines, "expected_timeframe")
    bucket_table(df, "category", lines)

    # --- ex-ante tiers (event-tier candidates) ---
    lines.append("## Ex-ante tiers (кандидаты event-tier стратегии)")
    lines.append("")
    tiers = {
        "T1: urg=high & impact≥0.7 & conf≥0.7 & dir≠neutral":
            (df["urgency"] == "high") & (df["impact"] >= 0.7)
            & (df["confidence"] >= 0.7) & (df["direction"] != "neutral"),
        "T2: impact≥0.7 & conf≥0.7 & dir≠neutral":
            (df["impact"] >= 0.7) & (df["confidence"] >= 0.7)
            & (df["direction"] != "neutral"),
        "T3: urg=high & impact≥0.5 & dir≠neutral & actionable":
            (df["urgency"] == "high") & (df["impact"] >= 0.5)
            & (df["direction"] != "neutral") & df["is_actionable"],
        "T4: impact≥0.9":
            df["impact"] >= 0.9,
    }
    lines.append("| tier | n | n/мес | range p50 | P(≥1%) | mean ret_dir % | "
                 "median ret_dir % | hit(|ret|≥1%) | hit(all) |")
    lines.append("|" + "---|" * 9)
    for name, mask in tiers.items():
        g = df[mask]
        if g.empty:
            lines.append(f"| {name} | 0 | | | | | | | |")
            continue
        gb = g[g["ret_abs"] >= 1.0]
        hit_big = gb["dir_correct"].mean() * 100 if len(gb) >= 5 else np.nan
        dirless = g[g["direction"] != "neutral"]
        lines.append(
            f"| {name} | {len(g)} | {len(g) / MONTHS_SPAN:.1f} | "
            f"{g['range'].median():.2f}% | {(g['range'] >= 1.0).mean() * 100:.1f}% | "
            f"**{g['ret_dir'].mean():+.3f}** | {g['ret_dir'].median():+.3f} | "
            f"{(f'{hit_big:.0f}%' if not np.isnan(hit_big) else 'n<5')} | "
            f"{dirless['dir_correct'].mean() * 100:.0f}% |"
        )
    lines.append("")
    lines.append("Cost floor для справки (v2.1, Сбер): стоки TP-exit 0.115% / "
                 "time-exit 0.140%; фьючерсы 0.057–0.082%. mean ret_dir тира — "
                 "это gross-проксі time-exit стратегии «вход по направлению LLM, "
                 "выход EOD».")

    out = "\n".join(lines)
    args.out.write_text(out, encoding="utf-8")
    print(out)
    print(f"\nSaved: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
