r"""Sprint 6.5 — Пилот A+E: hard-event таксономия + детерминированные правила
направления + first-report дедуп + event study + OCO-breakout.

Гипотеза автора (исходный замысел проекта): жёсткие события (дивиденды,
ставка ЦБ, отчётности, санкции на эмитента, война/мир) дают ход >1%, направление
очевидно из СОДЕРЖАНИЯ (правило типа, не LLM-сентимент), кость незначительна.

Gate: существует тип события с hit-rate ≥60% (EOD), |медианный ход| ≥0.7%
и n ≥30 за 17 месяцев Y6.

Usage: python scripts/hard_events_pilot.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from prices_cache import PricesCache  # noqa: E402

# ---------------------------------------------------------------------------
# Company name → ticker (наши 12 стоков)
# ---------------------------------------------------------------------------
COMPANY_TICKER = [
    (r"сбербанк|сбера?\b|\bсбер\b", "SBER"),
    (r"газпром(?!\s*нефт)", "GAZP"),
    (r"лукойл", "LKOH"),
    (r"яндекс", "YDEX"),
    (r"роснефт", "ROSN"),
    (r"норникел|норильск\w* никел|гмк", "GMKN"),
    (r"новатэк", "NVTK"),
    (r"татнефт", "TATN"),
    (r"магнит\b", "MGNT"),
    (r"\bмтс\b", "MTSS"),
    (r"полюс", "PLZL"),
    (r"\bвтб\b", "VTBR"),
    # Sprint 9 — MOEXBC blue-chip additions
    (r"сургутнефтегаз|сургут\w*\s+нефтегаз", "SNGS"),
    (r"\bтинькофф\b|т-банк|тинькофф банк|\bткс\b|т-технологи", "T"),
    (r"\bozon\b|\bозон\b", "OZON"),
    (r"\bx5\b|икс\s?5|пятёроч|пятероч|перекрёст|перекрест", "X5"),
    # MOEX (Мосбиржа) — компания; индексные упоминания шумят, но event-классы
    # гейтятся keyword'ами событий, так что это лишь шум в контекстных материалах
    (r"московск\w+ бирж|мосбирж", "MOEX"),
]


def match_ticker(text: str) -> str | None:
    for pat, tk in COMPANY_TICKER:
        if re.search(pat, text):
            return tk
    return None


# ---------------------------------------------------------------------------
# Таксономия: (type, pattern, direction_fn) — direction_fn(text, ticker) →
# list[(instrument, side)] либо [].
# ---------------------------------------------------------------------------
RE_DIV = re.compile(r"(рекомендова|совет директоров|набсовет|утвердил)[^.]{0,80}дивиденд"
                    r"|дивиденд[^.]{0,60}(руб|на акци)")
RE_DIV_NONE = re.compile(r"(не выплачивать|отказ\w*\s+от\s+выплат)[^.]{0,40}дивиденд"
                         r"|дивиденд[^.]{0,40}не выплачив")
RE_CBR = re.compile(r"(банк россии|цб)[^.]{0,60}(ключев|учетн)\w*\s*ставк")
RE_CBR_CUT = re.compile(r"сниз|пониз|понижен")
RE_CBR_HIKE = re.compile(r"повы|поднял")
RE_EARN = re.compile(r"(чистая прибыль|чистый убыток|чист\w+ прибыл)[^.]{0,80}"
                     r"(выросл|увелич|снизил|сократил|упал|млрд|млн)"
                     r"|(увеличил|нарастил)[^.]{0,30}чист\w+ прибыл"
                     r"|завершил[^.]{0,40}убытком")
RE_EARN_NEG = re.compile(r"убыт|прибыл\w*[^.]{0,30}(снизил|сократил|упал)"
                         r"|(снизил|сократил)[^.]{0,30}прибыл")
RE_SANC = re.compile(r"санкци\w*[^.]{0,60}(против|в отношении|включ)"
                     r"|(ввел\w?|ввод\w+)[^.]{0,30}санкци|sdn[- ]лист")
RE_PEACE = re.compile(r"перемири|прекращени\w+ огня|мирн\w+ (переговор|план|соглашени)"
                      r"|деэскалаци")
RE_MIL = re.compile(r"массированн\w+ (удар|атак)|ядерн\w+ (удар|оружи|эскалаци)"
                    r"|мобилизаци|военн\w+ положени|объявил\w? войн")
RE_BUYBACK = re.compile(r"обратн\w+ выкуп|buyback|байбэк")
RE_SPO = re.compile(r"допэмисси|дополнительн\w+ эмисси")


def classify(text: str) -> list[tuple[str, str, str]]:
    """→ list of (event_type, instrument, side)."""
    out = []
    tk = match_ticker(text)
    if RE_DIV_NONE.search(text):
        if tk:
            out.append(("DIV_NONE", tk, "SELL"))
    elif RE_DIV.search(text):
        if tk:
            out.append(("DIV_REC", tk, "BUY"))
    if RE_CBR.search(text):
        if RE_CBR_CUT.search(text):
            out += [("CBR_CUT", "MIX", "BUY"), ("CBR_CUT", "SBER", "BUY")]
        elif RE_CBR_HIKE.search(text):
            out += [("CBR_HIKE", "MIX", "SELL"), ("CBR_HIKE", "SBER", "SELL")]
        # сохранил — без консенсуса направление не определено → skip
    if RE_EARN.search(text) and tk:
        side = "SELL" if RE_EARN_NEG.search(text) else "BUY"
        out.append((f"EARN_{'DOWN' if side == 'SELL' else 'UP'}", tk, side))
    if RE_SANC.search(text) and tk:
        out.append(("SANC_RU", tk, "SELL"))
    if RE_PEACE.search(text):
        out.append(("PEACE", "MIX", "BUY"))
    if RE_MIL.search(text):
        out.append(("MIL_ESC", "MIX", "SELL"))
    if RE_BUYBACK.search(text) and tk:
        out.append(("BUYBACK", tk, "BUY"))
    if RE_SPO.search(text) and tk:
        out.append(("SPO_DILUT", tk, "SELL"))
    return out


# ---------------------------------------------------------------------------
def extract_events(corpus_path: Path) -> pd.DataFrame:
    c = pd.read_parquet(corpus_path,
                        columns=["id", "headline", "full_text", "datetime_msk", "channel"])
    rows = []
    for r in c.itertuples(index=False):
        text = ((r.headline or "") + " " + (r.full_text or "")[:300]).lower()
        for etype, instr, side in classify(text):
            rows.append({"id": r.id, "dt": pd.Timestamp(r.datetime_msk),
                         "channel": r.channel, "etype": etype,
                         "ticker": instr, "side": side,
                         "headline": (r.headline or "")[:140]})
    df = pd.DataFrame(rows).sort_values("dt").reset_index(drop=True)
    return df


def extract_events_jsonl(jsonl_path: Path, dt_from: str, dt_to: str) -> pd.DataFrame:
    """Same extraction over the raw telegram archive (no LLM needed)."""
    import json
    lo, hi = pd.Timestamp(dt_from), pd.Timestamp(dt_to)
    rows = []
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            dt = pd.Timestamp(rec.get("datetime"))
            if not (lo <= dt <= hi):
                continue
            text = ((rec.get("headline") or "") + " "
                    + (rec.get("full_text") or "")[:300]).lower()
            for etype, instr, side in classify(text):
                rows.append({"id": rec.get("id"), "dt": dt,
                             "channel": rec.get("channel"), "etype": etype,
                             "ticker": instr, "side": side,
                             "headline": (rec.get("headline") or "")[:140]})
    return pd.DataFrame(rows).sort_values("dt").reset_index(drop=True)


def dedup_first_report(df: pd.DataFrame, window_h: int = 48) -> pd.DataFrame:
    """Keep earliest report per (etype, ticker) cluster within window."""
    keep = []
    last_kept: dict[tuple, pd.Timestamp] = {}
    for r in df.itertuples(index=False):
        key = (r.etype, r.ticker)
        prev = last_kept.get(key)
        if prev is not None and (r.dt - prev) < pd.Timedelta(hours=window_h):
            continue
        last_kept[key] = r.dt
        keep.append(r)
    return pd.DataFrame(keep)


# ---------------------------------------------------------------------------
def study_event(cache: PricesCache, r) -> dict | None:
    """Entry at next-min open; signed returns at 60m/240m/EOD + OCO test."""
    try:
        bars = cache.get_bars(r.ticker, r.dt, r.dt + pd.Timedelta(hours=14))
    except Exception:
        return None
    if bars is None or len(bars) == 0:
        return None
    nm = (r.dt + pd.Timedelta(seconds=60)).floor("min")
    i0 = bars.index.searchsorted(nm)
    if i0 >= len(bars):
        return None
    t0 = bars.index[i0]
    gap_h = (t0 - nm).total_seconds() / 3600
    entry = float(bars["open"].iloc[i0])
    if entry <= 0:
        return None
    d = 1 if r.side == "BUY" else -1
    eod = t0.normalize() + pd.Timedelta(hours=18, minutes=45)
    out = {"etype": r.etype, "ticker": r.ticker, "side": r.side, "dt": r.dt,
           "entry_ts": t0, "entry": entry, "overnight_gap": gap_h > 2,
           "headline": r.headline}
    for label, td in (("60m", pd.Timedelta(minutes=60)),
                      ("240m", pd.Timedelta(minutes=240))):
        ix = bars.index.searchsorted(t0 + td, side="right") - 1
        out[f"ret_{label}"] = (d * (float(bars["close"].iloc[ix]) / entry - 1) * 100
                               if ix > i0 else np.nan)
    ie = bars.index.searchsorted(eod, side="right") - 1
    out["ret_eod"] = (d * (float(bars["close"].iloc[ie]) / entry - 1) * 100
                      if ie > i0 else np.nan)
    w = bars.iloc[i0:ie + 1]
    out["range_max"] = max((w["high"].max() - entry), (entry - w["low"].min())) / entry * 100

    # --- OCO breakout: ±X%, trigger window 60 мин, ride to EOD close ---
    for x in (0.2, 0.3, 0.5, 0.7):
        up, dn = entry * (1 + x / 100), entry * (1 - x / 100)
        trig_end = bars.index.searchsorted(t0 + pd.Timedelta(minutes=60), side="right")
        oco_dir = 0
        trig_px = np.nan
        for j in range(i0, min(trig_end, ie + 1)):
            hi, lo = float(bars["high"].iloc[j]), float(bars["low"].iloc[j])
            both = hi >= up and lo <= dn
            if both:
                oco_dir = 0  # ambiguous bar — skip event for this X
                break
            if hi >= up:
                oco_dir, trig_px = 1, up
                break
            if lo <= dn:
                oco_dir, trig_px = -1, dn
                break
        if oco_dir != 0 and ie > i0:
            out[f"oco{x}"] = oco_dir * (float(bars["close"].iloc[ie]) / trig_px - 1) * 100
        else:
            out[f"oco{x}"] = np.nan
    return out


def agg_table(df: pd.DataFrame, col: str, lines: list[str], title: str) -> None:
    lines.append(f"## {title}")
    lines.append("")
    lines.append("| type | n | mean % | median % | hit % | p25 | p75 |")
    lines.append("|" + "---|" * 7)
    for et, g in df.groupby("etype"):
        v = g[col].dropna()
        if len(v) == 0:
            continue
        lines.append(f"| {et} | {len(v)} | {v.mean():+.3f} | {v.median():+.3f} | "
                     f"{(v > 0).mean() * 100:.1f} | {v.quantile(.25):+.3f} | "
                     f"{v.quantile(.75):+.3f} |")
    v = df[col].dropna()
    lines.append(f"| **ALL** | {len(v)} | {v.mean():+.3f} | {v.median():+.3f} | "
                 f"{(v > 0).mean() * 100:.1f} | {v.quantile(.25):+.3f} | "
                 f"{v.quantile(.75):+.3f} |")
    lines.append("")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path,
                    default=PROJECT_ROOT / "data/reenrich_phase2/y6_corpus_70b.parquet")
    ap.add_argument("--jsonl", type=Path, default=None,
                    help="Use raw telegram archive instead of the Y6 corpus.")
    ap.add_argument("--from", dest="dt_from", default="2022-01-01")
    ap.add_argument("--to", dest="dt_to", default="2024-12-31")
    ap.add_argument("--out-dir", type=Path,
                    default=PROJECT_ROOT / "data/walk_forward/hard_events_pilot")
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.jsonl:
        ev = extract_events_jsonl(args.jsonl, args.dt_from, args.dt_to)
    else:
        ev = extract_events(args.corpus)
    print(f"extracted raw typed events: {len(ev)}")
    print(ev["etype"].value_counts().to_string())
    ev = dedup_first_report(ev)
    print(f"after first-report dedup (48h): {len(ev)}")
    print(ev["etype"].value_counts().to_string())

    cache = PricesCache()
    cache.warmup()
    rows = [d for d in (study_event(cache, r) for r in ev.itertuples(index=False))
            if d is not None]
    df = pd.DataFrame(rows)
    df.to_parquet(args.out_dir / "events_study.parquet")

    lines = ["# Hard-events pilot (A+E) — направление по правилам типа, не LLM", ""]
    lines.append(f"Событий после дедупа с ценами: {len(df)} "
                 f"(из них overnight-gap: {int(df['overnight_gap'].sum())}). "
                 "ret_* — знаковый ход в сторону правила. "
                 "Cost floor справки: стоки ~0.14–0.19% RT, фьючерсы ~0.06–0.08%.")
    lines.append("")
    agg_table(df, "ret_60m", lines, "Направленный ход через 60 минут")
    agg_table(df, "ret_240m", lines, "Направленный ход через 240 минут")
    agg_table(df, "ret_eod", lines, "Направленный ход к EOD (18:45)")
    intraday = df[~df["overnight_gap"]]
    agg_table(intraday, "ret_eod", lines, "EOD, ТОЛЬКО intraday-события (без ночных гэпов)")
    agg_table(df, "oco0.2", lines, "OCO-breakout ±0.2% (trigger 60м, exit EOD)")
    agg_table(df, "oco0.3", lines, "OCO-breakout ±0.3%")
    agg_table(df, "oco0.5", lines, "OCO-breakout ±0.5%")
    agg_table(df, "oco0.7", lines, "OCO-breakout ±0.7%")

    # Gate check
    lines.append("## Gate (hit≥60% & |median|≥0.7% & n≥30, EOD)")
    lines.append("")
    passed = []
    for et, g in df.groupby("etype"):
        v = g["ret_eod"].dropna()
        if len(v) >= 30 and (v > 0).mean() >= 0.60 and abs(v.median()) >= 0.7:
            passed.append(f"{et} (n={len(v)}, hit={(v > 0).mean() * 100:.0f}%, "
                          f"med={v.median():+.2f}%)")
    lines.append(("✅ PASSED: " + "; ".join(passed)) if passed else
                 "❌ Ни один тип не прошёл gate.")

    (args.out_dir / "report.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nSaved: {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
