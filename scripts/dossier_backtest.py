r"""Sprint 8 Этап 3 — бэктест оценок досье-пилота + gate-вердикт.

Читает eval-чекпоинты (модель × honest/shuffle), считает исходы нашим
стандартом (вход open минуты+1; горизонты 60m/240m/EOD/t1), кости v2.1,
бакеты surprise_level, по-годовую робастность, top5-share, сравнение
с shuffle-контролем.

GATE (зафиксирован в плане ДО прогона):
  genuine_surprise @EOD: hit ≥58% И mean net ret_dir ≥ +0.4% И n ≥40
  И плюс в ≥2 из 3 лет И top5 <60% И hit − hit(shuffle) ≥ +5 п.п.

Usage: python scripts/dossier_backtest.py [--model-slug Llama_3_3_70B_Instruct]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from prices_cache import PricesCache  # noqa: E402
from scripts.dossier_lib import DATA_DIR  # noqa: E402
from scripts.dossier_build import CHECKPOINT_DIR  # noqa: E402

RT_STOCK = 0.19
FUND_DAY = 0.13 / 365 * 100 + 0.0045
HORIZONS = ("60m", "240m", "eod", "t1")


def outcomes_for(cache: PricesCache, ticker: str, dt: pd.Timestamp) -> dict | None:
    """Сырые long-доходности на 4 горизонтах + ночи для t1."""
    try:
        bars = cache.get_bars(ticker, dt, dt + pd.Timedelta(days=6))
    except Exception:
        return None
    if bars is None or len(bars) == 0:
        return None
    nm = (dt + pd.Timedelta(seconds=60)).floor("min")
    i0 = bars.index.searchsorted(nm)
    if i0 >= len(bars) or (bars.index[i0] - nm).total_seconds() > 1800:
        return None
    entry = float(bars["open"].iloc[i0])
    if entry <= 0:
        return None
    t0 = bars.index[i0]
    out = {}
    for label, td in (("60m", pd.Timedelta(minutes=60)),
                      ("240m", pd.Timedelta(minutes=240))):
        ix = bars.index.searchsorted(t0 + td, side="right") - 1
        out[label] = (float(bars["close"].iloc[ix]) / entry - 1) * 100 if ix > i0 else None
    eod = t0.normalize() + pd.Timedelta(hours=18, minutes=45)
    ie = bars.index.searchsorted(eod, side="right") - 1
    out["eod"] = (float(bars["close"].iloc[ie]) / entry - 1) * 100 if ie > i0 else None
    dates = pd.DatetimeIndex(bars.index.normalize().unique())
    nxt = dates[dates > t0.normalize()]
    if len(nxt):
        t1c = nxt[0] + pd.Timedelta(hours=18, minutes=45)
        i1 = bars.index.searchsorted(t1c, side="right") - 1
        out["t1"] = (float(bars["close"].iloc[i1]) / entry - 1) * 100 if i1 > i0 else None
        out["t1_nights"] = (nxt[0] - t0.normalize()).days
    else:
        out["t1"], out["t1_nights"] = None, 0
    return out


def load_evals(path: Path) -> pd.DataFrame:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if not r.get("result"):
                continue
            rows.append({"eid": r["eid"], "dt": pd.Timestamp(r["dt"]),
                         "ticker": r["ticker"], "etype": r["etype"], "src": r["src"],
                         **r["result"]})
    df = pd.DataFrame(rows)
    return df.drop_duplicates(subset="eid", keep="last") if not df.empty else df


def bucket_stats(df: pd.DataFrame, h: str) -> dict:
    v = df[df[f"net_{h}"].notna()]
    if v.empty:
        return {"n": 0}
    net = v[f"net_{h}"]
    byy = v.groupby(v["dt"].dt.year)[f"net_{h}"].mean()
    tot = net.sum()
    top5 = net.nlargest(5).sum() / tot * 100 if tot > 0 else np.nan
    return {"n": len(v), "hit": (net > 0).mean() * 100, "mean": net.mean(),
            "median": net.median(), "years_pos": int((byy > 0).sum()),
            "years_n": len(byy), "top5": top5,
            "by_year": {int(y): round(x, 3) for y, x in byy.items()}}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-slug", default="Llama_3_3_70B_Instruct")
    args = ap.parse_args()

    cache = PricesCache()
    cache.warmup()
    lines = [f"# Sprint 8 — досье-пилот: бэктест ({args.model_slug})", ""]
    frames = {}
    for suffix in ("", "_shuffle"):
        p = CHECKPOINT_DIR / f"evals_{args.model_slug}{suffix}.jsonl"
        if not p.exists():
            continue
        df = load_evals(p)
        if df.empty:
            continue
        # исходы
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
        traded = df[d != 0].copy()
        dd = d[d != 0]
        for h in HORIZONS:
            gross = traded[f"ret_{h}"] * dd
            cost = RT_STOCK + np.where(
                (h == "t1") & (dd < 0), FUND_DAY * traded["t1_nights"], 0.0)
            traded[f"net_{h}"] = gross - cost
        frames[suffix or "honest"] = traded

        label = "HONEST" if suffix == "" else "SHUFFLE-КОНТРОЛЬ"
        lines.append(f"## {label}: n_evals={len(df)}, traded={len(traded)} "
                     f"(neutral={int((d == 0).sum())})")
        lines.append("")
        lines.append(f"levels: {df['surprise_level'].value_counts().to_dict()}")
        lines.append("")
        lines.append("| bucket | horizon | n | hit % | mean net % | median | "
                     "years+ | top5 % |")
        lines.append("|" + "---|" * 8)
        for lvl in ("genuine_surprise", "priced", "confirmed"):
            sub = traded[traded["surprise_level"] == lvl]
            for h in HORIZONS:
                s = bucket_stats(sub, h)
                if s["n"] == 0:
                    continue
                lines.append(
                    f"| {lvl} | {h} | {s['n']} | {s['hit']:.1f} | "
                    f"**{s['mean']:+.3f}** | {s['median']:+.3f} | "
                    f"{s['years_pos']}/{s['years_n']} | {s['top5']:.0f} |")
        lines.append("")

    # --- GATE ---
    lines.append("## GATE (genuine_surprise @EOD)")
    lines.append("")
    if "honest" in frames:
        g = frames["honest"]
        g = g[g["surprise_level"] == "genuine_surprise"]
        s = bucket_stats(g, "eod")
        sh_hit = None
        if "_shuffle" in frames:
            gs = frames["_shuffle"]
            gs = gs[gs["surprise_level"] == "genuine_surprise"]
            ss = bucket_stats(gs, "eod")
            sh_hit = ss.get("hit")
        if s["n"] == 0:
            lines.append("❌ нет genuine_surprise сделок")
        else:
            checks = {
                "hit ≥58%": s["hit"] >= 58,
                "mean net ≥ +0.4%": s["mean"] >= 0.4,
                "n ≥40": s["n"] >= 40,
                "плюс в ≥2 из лет": s["years_pos"] >= min(2, s["years_n"]),
                "top5 <60%": (not np.isnan(s["top5"])) and s["top5"] < 60,
                "vs shuffle ≥ +5пп": (sh_hit is not None and s["hit"] - sh_hit >= 5),
            }
            for k, v in checks.items():
                lines.append(f"- {'✅' if v else '❌'} {k}")
            lines.append("")
            lines.append(f"Факт: n={s['n']}, hit={s['hit']:.1f}%, "
                         f"mean={s['mean']:+.3f}%, by_year={s['by_year']}, "
                         f"top5={s['top5']:.0f}%, shuffle_hit="
                         f"{f'{sh_hit:.1f}%' if sh_hit is not None else 'n/a'}")
            verdict = all(checks.values())
            lines.append("")
            lines.append("## " + ("✅ GATE PASSED" if verdict else "❌ GATE NOT PASSED"))
        # сохранить trades
        frames["honest"].to_parquet(DATA_DIR / "eval_trades.parquet")

    out = DATA_DIR / "report.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nSaved: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
