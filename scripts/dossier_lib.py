r"""Sprint 8 Этап 0 — библиотека досье-пилота: per-ticker индекс новостей,
селектор материалов point-in-time, структурные блоки (дивиденды/цены),
anti-look-ahead гварды.

Build index (один раз, ~3 мин):
    python scripts/dossier_lib.py --build-index

Demo материалов (верификация):
    python scripts/dossier_lib.py --demo SBER --date 2024-01-01
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from scripts.hard_events_pilot import COMPANY_TICKER, classify  # noqa: E402

ARCHIVE = Path(r"D:\quik_sber\newsbot\telegram_news.jsonl")
DATA_DIR = PROJECT_ROOT / "data" / "dossier"
INDEX_DIR = DATA_DIR / "news_index"
DIVIDENDS_CSV = PROJECT_ROOT / "data" / "pairs" / "dividends.csv"

# Sprint 9 — универс MOEXBC (blue chips, 15). MGNT/MTSS выбыли из индекса,
# но данные по ним есть — оставлены для совместимости с Sprint 8.
TICKERS = ["SBER", "GAZP", "LKOH", "YDEX", "ROSN", "GMKN", "NVTK", "TATN",
           "PLZL", "VTBR", "SNGS", "MOEX", "T", "OZON", "X5", "MGNT", "MTSS"]

RE_FIN = re.compile(
    r"прибыл|выручк|дивиден|отчет|отчёт|мсфо|рсбу|санкци|buyback|байбэк|"
    r"обратн\w+ выкуп|допэмисси|прогноз|гайденс|ebitda|капитализаци|долг")

WORD_RE = re.compile(r"[а-яa-z]{4,}")


# ---------------------------------------------------------------------------
# Этап 0.1 — индекс
# ---------------------------------------------------------------------------
def build_news_index(archive: Path = ARCHIVE) -> None:
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    rows: dict[str, list] = {t: [] for t in TICKERS}
    n_lines = 0
    with archive.open(encoding="utf-8") as f:
        for line in f:
            n_lines += 1
            if n_lines % 100_000 == 0:
                print(f"  ... {n_lines} lines")
            try:
                rec = json.loads(line)
            except Exception:
                continue
            head = rec.get("headline") or ""
            text = (head + " " + (rec.get("full_text") or "")[:300]).lower()
            matched = [tk for pat, tk in COMPANY_TICKER if re.search(pat, text)]
            if not matched:
                continue
            etypes = ",".join(sorted({e for e, _tk, _s in classify(text)}))
            for tk in set(matched):
                if tk not in rows:
                    continue
                rows[tk].append({
                    "dt": rec.get("datetime"), "headline": head[:200],
                    "text500": (rec.get("full_text") or "")[:500],
                    "etype": etypes, "is_fin": bool(RE_FIN.search(text)),
                })
    for tk, rr in rows.items():
        df = pd.DataFrame(rr)
        if df.empty:
            print(f"{tk}: EMPTY")
            continue
        df["dt"] = pd.to_datetime(df["dt"])
        df = df.sort_values("dt").reset_index(drop=True)
        df.to_parquet(INDEX_DIR / f"{tk}.parquet")
        print(f"{tk}: {len(df)} rows  {df['dt'].min().date()} → {df['dt'].max().date()}  "
              f"fin={int(df['is_fin'].sum())} typed={int((df['etype'] != '').sum())}")


@lru_cache(maxsize=16)
def load_index(ticker: str) -> pd.DataFrame:
    return pd.read_parquet(INDEX_DIR / f"{ticker}.parquet")


# ---------------------------------------------------------------------------
# Этап 0.2 — селектор материалов point-in-time
# ---------------------------------------------------------------------------
def _norm_headline(h: str) -> str:
    return " ".join(WORD_RE.findall(h.lower()))[:120]


def select_materials(ticker: str, t: pd.Timestamp, window_days: int = 365,
                     cap: int = 250, per_month_min: int = 6) -> str:
    """Блок строк 'YYYY-MM-DD | headline' (только материалы < t)."""
    df = load_index(ticker)
    w = df[(df["dt"] < t) & (df["dt"] >= t - pd.Timedelta(days=window_days))].copy()
    if w.empty:
        return "(новостей за период нет)"
    w["norm"] = w["headline"].map(_norm_headline)
    w = w.drop_duplicates(subset="norm", keep="first")
    w["prio"] = np.where(w["etype"] != "", 2, np.where(w["is_fin"], 1, 0))
    w["month"] = w["dt"].dt.to_period("M")

    picked_idx: list = []
    # помесячный минимум (хронологическое покрытие)
    for _, g in w.groupby("month"):
        picked_idx += list(g.sort_values(["prio", "dt"], ascending=[False, True])
                           .head(per_month_min).index)
    # добор по приоритету до cap
    rest = w.drop(index=picked_idx).sort_values(["prio", "dt"], ascending=[False, False])
    room = max(0, cap - len(picked_idx))
    picked_idx += list(rest.head(room).index)
    sel = w.loc[sorted(set(picked_idx))].sort_values("dt")
    if len(sel) > cap:
        sel = sel.tail(cap)
    return "\n".join(f"{r.dt.date()} | {r.headline}" for r in sel.itertuples(index=False))


# ---------------------------------------------------------------------------
# Этап 0.3 — структурные блоки point-in-time
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _dividends() -> pd.DataFrame:
    return pd.read_csv(DIVIDENDS_CSV, parse_dates=["registryclosedate"])


def dividends_block(ticker: str, t: pd.Timestamp, last_n: int = 8) -> str:
    d = _dividends()
    d = d[(d["secid"] == ticker) & (d["registryclosedate"] < t)]
    d = d.sort_values("registryclosedate").tail(last_n)
    if d.empty:
        return "(выплат в базе нет)"
    return "\n".join(f"{r.registryclosedate.date()} (отсечка): {r.value} руб/акция"
                     for r in d.itertuples(index=False))


@lru_cache(maxsize=16)
def _daily(ticker: str) -> pd.Series:
    from prices_cache import PricesCache
    cache = _prices_cache()
    bars = cache.get_bars(ticker, pd.Timestamp("2022-01-01"), pd.Timestamp("2026-06-12"))
    b = bars[bars.index.time <= pd.Timestamp("18:45").time()]
    return b.groupby(b.index.normalize())["close"].last()


@lru_cache(maxsize=1)
def _prices_cache():
    from prices_cache import PricesCache
    c = PricesCache()
    c.warmup()
    return c


def price_block(ticker: str, t: pd.Timestamp) -> str:
    s = _daily(ticker)
    s = s[s.index < t.normalize()]
    if len(s) < 260:
        return "(недостаточно ценовой истории)"
    last = s.iloc[-1]
    ret = lambda n: (last / s.iloc[-n] - 1) * 100 if len(s) >= n else np.nan  # noqa: E731
    hi52, lo52 = s.tail(252).max(), s.tail(252).min()
    vol20 = s.pct_change().tail(20).std() * np.sqrt(252) * 100
    return (f"Цена закрытия {s.index[-1].date()}: {last:.2f}\n"
            f"Доходность: 1 мес {ret(21):+.1f}%, 3 мес {ret(63):+.1f}%, "
            f"12 мес {ret(252):+.1f}%\n"
            f"От 52-нед. максимума: {(last / hi52 - 1) * 100:+.1f}%, "
            f"от минимума: {(last / lo52 - 1) * 100:+.1f}%\n"
            f"Годовая волатильность (20д): {vol20:.0f}%")


# ---------------------------------------------------------------------------
# Anti-look-ahead гвард
# ---------------------------------------------------------------------------
RE_DATES = re.compile(r"\b(\d{4})-(\d{2})-\d{2}\b|\b\d{2}\.(\d{2})\.(\d{4})\b|\b(20\d{2})\s*год")


def find_lookahead_dates(text: str, t: pd.Timestamp) -> list[str]:
    """Возвращает упоминания дат строго ПОЗЖЕ t (год/месяц-грануляция)."""
    bad = []
    for m in RE_DATES.finditer(text):
        if m.group(1):                      # YYYY-MM-DD
            y, mo = int(m.group(1)), int(m.group(2))
        elif m.group(4):                    # DD.MM.YYYY
            y, mo = int(m.group(4)), int(m.group(3))
        else:                               # 'YYYY год'
            y, mo = int(m.group(5)), 1
        if (y, mo) > (t.year, t.month):
            bad.append(m.group(0))
    return bad


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-index", action="store_true")
    ap.add_argument("--demo", default=None, metavar="TICKER")
    ap.add_argument("--date", default="2024-01-01")
    args = ap.parse_args()
    if args.build_index:
        build_news_index()
    if args.demo:
        t = pd.Timestamp(args.date)
        print(f"=== MATERIALS {args.demo} @ {t.date()} ===")
        m = select_materials(args.demo, t)
        print(f"(строк: {len(m.splitlines())})")
        print("\n".join(m.splitlines()[:15]), "\n...")
        print("\n".join(m.splitlines()[-10:]))
        print(f"\n=== DIVIDENDS ===\n{dividends_block(args.demo, t)}")
        print(f"\n=== PRICE ===\n{price_block(args.demo, t)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
