r"""Sprint 9 Фаза 1 — point-in-time таймлайн фундамента из smart-lab.

Источник: smart-lab.ru/q/<TICKER>/f/y/ (годовые) и /f/q/ (квартальные) МСФО.
Ключевое: страница содержит строку `<tr field="date">` с ДАТАМИ ПУБЛИКАЦИИ
отчётов → point-in-time анкеровка бесплатна (known_from = дата отчёта).

requests с браузерным User-Agent работает (200); bs4 не нужен — regex по
консистентным `<tr field="...">` блокам.

Build:  python scripts/kb_fundamentals.py --build
Demo:   python scripts/kb_fundamentals.py --asof SBER --date 2024-06-01
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from functools import lru_cache
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = PROJECT_ROOT / "data" / "kb" / "fundamentals"
UNIVERSE_JSON = PROJECT_ROOT / "data" / "kb" / "universe.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html",
}

# Поля для досье (русские подписи). Остальные 50+ полей сохраняем как есть,
# но эти — приоритетные в md-рендере. Банки/нефинансы имеют разный набор —
# рендерим то, что присутствует.
PRIORITY_FIELDS = {
    "revenue": "Выручка, млрд", "net_income": "Чистая прибыль, млрд",
    "ebitda": "EBITDA, млрд", "net_operating_income": "Чистый опер.доход, млрд",
    "eps": "EPS, руб", "dividend": "Дивиденд, руб/акц",
    "div_yield": "Див.доходность, %", "div_payout_ratio": "Payout, %",
    "roe": "ROE, %", "roa": "ROA, %", "p_e": "P/E", "p_b": "P/B",
    "ev_ebitda": "EV/EBITDA", "market_cap": "Капитализация, млрд",
    "net_debt": "Чистый долг, млрд", "debt_ebitda": "Долг/EBITDA",
    "capital": "Капитал, млрд", "bank_assets": "Активы, млрд",
}

TD_RE = re.compile(r'<td(?P<attr>[^>]*)>(?P<val>.*?)</td>', re.S)
TR_RE = re.compile(r'<tr field="(?P<field>[^"]+)">(?P<body>.*?)</tr>', re.S)
TH_RE = re.compile(r'<th>(?P<th>.*?)</th>', re.S)
TAG_RE = re.compile(r'<[^>]+>')


def _clean(s: str) -> str:
    s = TAG_RE.sub("", s)
    s = s.replace("&nbsp;", " ").replace("\xa0", " ")
    return s.strip()


def _to_num(s: str):
    s = _clean(s).replace(" ", "").replace("%", "").replace(",", ".")
    if s in ("", "-", "?", "n/a", "—"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_html(ticker: str, period: str) -> str | None:
    url = f"https://smart-lab.ru/q/{ticker}/f/{period}/"
    for attempt in range(4):
        try:
            r = requests_get(url)
            if r.status_code == 200 and "Дата отчета" in r.text:
                return r.text
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return None


def requests_get(url: str):
    import requests
    return requests.get(url, headers=HEADERS, timeout=25)


def _value_cells(body: str) -> list[str]:
    """Значащие <td> строки (без chartrow-спарклайна и ltm_spc-спейсера)."""
    out = []
    for m in TD_RE.finditer(body):
        attr = m.group("attr")
        if "chartrow" in attr or "ltm_spc" in attr:
            continue
        out.append(m.group("val"))
    return out


def parse_page(html: str) -> dict:
    """→ {periods:[...], report_dates:[...], fields:{field:{label,values}}}."""
    rows = {m.group("field"): m.group("body") for m in TR_RE.finditer(html)}
    if "date" not in rows:
        return {}
    report_dates = [_clean(v) for v in _value_cells(rows["date"])]
    n = len(report_dates)
    # period-метки: годы/кварталы из хедера до "Дата отчета"
    head = html[: html.find('field="date"')]
    periods = re.findall(r">\s*(20\d\d(?:\s*[QКк]?\s*[1-4]| LTM)?)\s*<", head)
    # последний столбец у smart-lab — LTM
    periods = periods[-(n - 1):] + ["LTM"] if n >= 1 else periods
    if len(periods) != n:
        periods = [f"P{i}" for i in range(n - 1)] + ["LTM"]
    fields = {}
    for field, body in rows.items():
        th = TH_RE.search(body)
        label = _clean(th.group("th")) if th else field
        vals = _value_cells(body)
        if len(vals) != n:
            continue
        fields[field] = {"label": label,
                         "values": [(_clean(v) if field in ("date", "currency")
                                     else _to_num(v)) for v in vals]}
    return {"periods": periods, "report_dates": report_dates, "fields": fields}


def page_to_long(parsed: dict, ticker: str, gran: str) -> list[dict]:
    """Длинный формат: одна строка на (ticker, period)."""
    if not parsed:
        return []
    periods, dates = parsed["periods"], parsed["report_dates"]
    rows = []
    for i, per in enumerate(periods):
        rd = dates[i] if i < len(dates) else ""
        known_from = None
        m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", rd)
        if m:
            known_from = pd.Timestamp(f"{m.group(3)}-{m.group(2)}-{m.group(1)}")
        rec = {"ticker": ticker, "gran": gran, "period": per,
               "report_date": rd, "known_from": known_from, "is_ltm": per == "LTM"}
        for field, d in parsed["fields"].items():
            if field in ("date",):
                continue
            rec[field] = d["values"][i]
        rows.append(rec)
    return rows


def build(tickers: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for tk in tickers:
        all_rows = []
        for gran in ("y", "q"):
            html = fetch_html(tk, gran)
            if html is None:
                print(f"{tk}/{gran}: НЕТ (fetch fail)")
                continue
            parsed = parse_page(html)
            rows = page_to_long(parsed, tk, gran)
            all_rows += rows
            nf = len(parsed.get("fields", {}))
            print(f"{tk}/{gran}: {len(rows)} периодов, {nf} полей, "
                  f"даты {parsed.get('report_dates', [''])[0]}..{parsed.get('report_dates', [''])[-1]}")
            time.sleep(1.0)
        if all_rows:
            df = pd.DataFrame(all_rows)
            df.to_parquet(OUT_DIR / f"{tk}.parquet")


@lru_cache(maxsize=32)
def _load(ticker: str) -> pd.DataFrame:
    p = OUT_DIR / f"{ticker}.parquet"
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def fundamentals_asof(ticker: str, t: pd.Timestamp, gran: str = "y") -> dict | None:
    """Последний период с known_from ≤ t (point-in-time)."""
    df = _load(ticker)
    if df.empty:
        return None
    w = df[(df["gran"] == gran) & df["known_from"].notna() & (df["known_from"] <= t)]
    if w.empty:
        return None
    return w.sort_values("known_from").iloc[-1].to_dict()


def fundamentals_md(ticker: str, t: pd.Timestamp) -> str:
    """md-блок фундамента point-in-time для скармливания в LLM."""
    df = _load(ticker)
    if df.empty:
        return "(фундамент недоступен)"
    y = df[(df["gran"] == "y") & df["known_from"].notna() & (df["known_from"] <= t)]
    if y.empty:
        return "(нет отчётов до даты)"
    y = y.sort_values("known_from")
    last = y.iloc[-1]
    lines = [f"Последний известный отчёт: {last['period']} "
             f"(опубликован {last['report_date']})"]
    # тренд по последним 3 годовым
    tail = y[~y["is_ltm"]].tail(3)
    for field, lbl in PRIORITY_FIELDS.items():
        if field not in df.columns:
            continue
        vals = [(r["period"], r[field]) for _, r in tail.iterrows()
                if pd.notna(r.get(field))]
        if vals:
            seq = ", ".join(f"{p}: {v:g}" for p, v in vals)
            lines.append(f"- {lbl}: {seq}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--tickers", default="")
    ap.add_argument("--asof", default=None)
    ap.add_argument("--date", default="2024-06-01")
    args = ap.parse_args()
    if args.build:
        if args.tickers:
            tks = [x.strip().upper() for x in args.tickers.split(",")]
        else:
            tks = [u["ticker"] for u in json.load(open(UNIVERSE_JSON, encoding="utf-8"))]
        build(tks)
    if args.asof:
        t = pd.Timestamp(args.date)
        print(f"=== {args.asof} fundamentals as-of {t.date()} ===")
        print(fundamentals_md(args.asof, t))
        rec = fundamentals_asof(args.asof, t)
        if rec:
            print("\nknown_from:", rec["known_from"], "period:", rec["period"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
