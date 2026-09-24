r"""Sprint 9 Фаза 2 — генератор досье v2 с НАСТОЯЩИМ фундаментом.

Отличие от Sprint 8 dossier_build (склейка заголовков): контекст для LLM =
point-in-time фундамент со smart-lab (kb_fundamentals) + дивиденды (ISS) +
ценовой контекст + новостные материалы. LLM синтезирует досье с ЧИСЛЕННЫМИ
ожиданиями.

Переиспользует DI-batch/чекпоинт/anti-look-ahead паттерн dossier_build.

Usage:
    python scripts/kb_dossier.py                        # все, 70B
    python scripts/kb_dossier.py --tickers SBER --dates 2024-04-01
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from scripts.dossier_lib import select_materials, dividends_block, price_block, find_lookahead_dates  # noqa: E402
from scripts.kb_fundamentals import fundamentals_md  # noqa: E402
from scripts.dossier_build import get_api_key, DEEPINFRA_BASE_URL, DEFAULT_MODEL  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("kb_dossier")
logging.getLogger("httpx").setLevel(logging.WARNING)

DATA = PROJECT_ROOT / "data" / "kb"
CK_DIR = DATA / "checkpoints"
DOS_DIR = DATA / "dossiers"
UNIVERSE_JSON = DATA / "universe.json"

COMPANY_NAME = {
    "SBER": "Сбербанк", "GAZP": "Газпром", "LKOH": "ЛУКОЙЛ", "YDEX": "Яндекс",
    "ROSN": "Роснефть", "GMKN": "Норникель", "NVTK": "НОВАТЭК", "TATN": "Татнефть",
    "PLZL": "Полюс", "VTBR": "ВТБ", "SNGS": "Сургутнефтегаз", "MOEX": "Московская биржа",
    "T": "Т-Технологии", "OZON": "Озон", "X5": "X5 Group",
}
# X5 — цена с 2025-01, бэктест ограничен; но досье строим (фундамент с 2022).
QUARTER_DATES = ([f"{y}-{m:02d}-01" for y in (2022, 2023, 2024, 2025) for m in (1, 4, 7, 10)]
                 + ["2026-01-01", "2026-04-01"])

SYSTEM_PROMPT = """Ты — финансовый аналитик по российскому рынку акций.
Сегодня {date}. Составляешь досье {name} ({ticker}) ИСКЛЮЧИТЕЛЬНО по приведённым
материалам: реальные финансовые показатели (point-in-time, с датами публикации
отчётов), дивиденды, ценовая динамика, новостные заголовки до {date}.

ЖЁСТКИЕ ПРАВИЛА:
1. КАТЕГОРИЧЕСКИ запрещено использовать знания о событиях после {date}.
2. Опирайся на КОНКРЕТНЫЕ ЦИФРЫ из блока фундамента. Не выдумывай.
3. Не упоминай ни одной даты позже {date}.
4. Ожидания — численные и проверяемые (вывести из payout-политики ожидаемый
   дивиденд; из тренда прибыли — ожидаемый диапазон; из мультипликаторов — оценку)."""

USER_PROMPT = """=== ФУНДАМЕНТ (point-in-time, только отчёты до {date}) ===
{fundamentals}

=== ДИВИДЕНДНЫЕ ОТСЕЧКИ (факт) ===
{dividends}

=== ЦЕНОВАЯ ДИНАМИКА (до {date}) ===
{price}

=== НОВОСТНЫЕ ЗАГОЛОВКИ за 12 мес (дата | заголовок) ===
{materials}

Составь досье в markdown со СТРОГО следующими секциями:

# Досье {ticker} на {date}
## 1. Профиль бизнеса
## 2. Финансовая картина
(оперируй цифрами из блока фундамента: прибыль/выручка/маржа, тренд YoY)
## 3. Оценка
(мультипликаторы P/E, P/B, EV/EBITDA из фундамента + текущая цена; дорого/дёшево
относительно своей истории)
## 4. Дивиденды
(история, payout-политика, ОЖИДАЕМЫЙ следующий дивиденд С ЦИФРОЙ)
## 5. Открытые риски и катализаторы
## 6. Консенсус-нарратив
## 7. ОЖИДАНИЯ
```yaml
expectations:
  - expectation: "<конкретное численное ожидание>"
    status: pending
    if_confirmed: long|short|neutral
    if_broken: long|short|neutral
```
(4-8 ожиданий)"""


def load_processed(path: Path) -> set[str]:
    done = set()
    if path.exists():
        for line in path.open(encoding="utf-8"):
            try:
                r = json.loads(line)
                if r.get("md"):
                    done.add(r["id"])
            except Exception:
                pass
    return done


async def gen_one(client, model, ticker, date, sem, ck, lock):
    t = pd.Timestamp(date)
    usr = USER_PROMPT.format(
        date=date, ticker=ticker,
        fundamentals=fundamentals_md(ticker, t),
        dividends=dividends_block(ticker, t),
        price=price_block(ticker, t),
        materials=select_materials(ticker, t))
    sysp = SYSTEM_PROMPT.format(date=date, name=COMPANY_NAME.get(ticker, ticker), ticker=ticker)
    rec = {"id": f"{ticker}_{date}", "ticker": ticker, "date": date, "model": model,
           "md": None, "error": None, "lookahead_flags": [], "ts": time.time()}
    async with sem:
        for attempt in range(5):
            try:
                extra = ("" if not rec["lookahead_flags"] else
                         f"\n\nУбери все даты позже {date}: {rec['lookahead_flags']}")
                resp = await client.chat.completions.create(
                    model=model, temperature=0.2, max_tokens=2800,
                    messages=[{"role": "system", "content": sysp},
                              {"role": "user", "content": usr + extra}])
                md = re.sub(r"<think>.*?</think>", "", resp.choices[0].message.content or "",
                            flags=re.S).strip()
                bad = find_lookahead_dates(md, t)
                if bad and not rec["lookahead_flags"]:
                    rec["lookahead_flags"] = bad[:5]
                    continue
                rec["md"] = md
                rec["lookahead_flags"] = bad[:5]
                rec["usage"] = {"in": resp.usage.prompt_tokens, "out": resp.usage.completion_tokens}
                break
            except Exception as e:
                msg = str(e)
                if any(x in msg for x in ("429", "500", "502", "503", "timeout", "Timeout")):
                    await asyncio.sleep(2 ** attempt * 2)
                    continue
                rec["error"] = msg[:300]
                break
        else:
            rec["error"] = rec.get("error") or "retries exhausted"
    async with lock:
        with ck.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    if rec["md"]:
        DOS_DIR.mkdir(parents=True, exist_ok=True)
        (DOS_DIR / f"{ticker}_{date}.md").write_text(rec["md"], encoding="utf-8")
        flag = f" ⚠️{rec['lookahead_flags']}" if rec["lookahead_flags"] else ""
        log.info("done %s_%s (%d tok)%s", ticker, date, rec["usage"]["out"], flag)
    else:
        log.warning("FAIL %s_%s: %s", ticker, date, rec["error"])


async def amain(args):
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=get_api_key(), base_url=DEEPINFRA_BASE_URL,
                         max_retries=0, timeout=180)
    CK_DIR.mkdir(parents=True, exist_ok=True)
    slug = args.model.split("/")[-1].replace(".", "_").replace("-", "_")
    ck = CK_DIR / f"kb_dossiers_{slug}.jsonl"
    done = load_processed(ck)
    if args.tickers:
        tickers = [x.strip().upper() for x in args.tickers.split(",")]
    else:
        tickers = [u["ticker"] for u in json.load(open(UNIVERSE_JSON, encoding="utf-8"))]
    dates = [x.strip() for x in args.dates.split(",")] if args.dates else QUARTER_DATES
    todo = [(tk, d) for tk in tickers for d in dates if f"{tk}_{d}" not in done]
    log.info("kb-dossiers todo: %d (done %d) model=%s", len(todo), len(done), args.model)
    sem = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()
    await asyncio.gather(*(gen_one(client, args.model, tk, d, sem, ck, lock)
                           for tk, d in todo))
    await client.close()
    log.info("checkpoint: %s", ck)
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--tickers", default="")
    ap.add_argument("--dates", default="")
    ap.add_argument("--concurrency", type=int, default=16)
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
