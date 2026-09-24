r"""Sprint 8 Этап 1 — генерация point-in-time досье (12 тикеров × кварталы
2023Q1–2026Q2 = 168 шт) через DeepInfra.

Паттерн batch-прогона взят из scripts/deepinfra_runner.py: OpenAI SDK на DI
endpoint, Semaphore, append-only jsonl чекпоинт, resume по id, retry 429/5xx.

Anti-look-ahead: системный промпт запрещает знания после даты; после генерации
авточек дат (dossier_lib.find_lookahead_dates) → одна перегенерация с
предупреждением; если снова — флаг в чекпоинт.

Usage:
    python scripts/dossier_build.py                       # все 168, 70B
    python scripts/dossier_build.py --tickers SBER,GAZP --dates 2024-01-01
    python scripts/dossier_build.py --model deepseek-ai/DeepSeek-R1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from scripts.dossier_lib import (  # noqa: E402
    TICKERS, DATA_DIR, select_materials, dividends_block, price_block,
    find_lookahead_dates,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dossier_build")

DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DEFAULT_MODEL = "meta-llama/Llama-3.3-70B-Instruct"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
DOSSIER_DIR = DATA_DIR / "dossiers"

COMPANY_NAME = {
    "SBER": "Сбербанк", "GAZP": "Газпром", "LKOH": "ЛУКОЙЛ", "YDEX": "Яндекс",
    "ROSN": "Роснефть", "GMKN": "Норникель", "NVTK": "НОВАТЭК",
    "TATN": "Татнефть", "MGNT": "Магнит", "MTSS": "МТС",
    "PLZL": "Полюс", "VTBR": "ВТБ",
}

QUARTER_DATES = [f"{y}-{m:02d}-01" for y in (2023, 2024, 2025) for m in (1, 4, 7, 10)] \
    + ["2026-01-01", "2026-04-01"]

SYSTEM_PROMPT = """Ты — финансовый аналитик по российскому рынку акций.
Сегодняшняя дата: {date}. Ты составляешь досье компании {name} ({ticker})
ИСКЛЮЧИТЕЛЬНО на основе предоставленных материалов (новостные заголовки
до {date}, история дивидендных отсечек, ценовая динамика).

ЖЁСТКИЕ ПРАВИЛА:
1. КАТЕГОРИЧЕСКИ запрещено использовать любые знания о событиях после {date}.
2. Если информации нет в материалах — пиши «нет данных», не придумывай.
3. Не упоминай ни одной даты позже {date}.
4. Ожидания формулируй конкретно и проверяемо (с цифрами, где материалы позволяют)."""

USER_PROMPT = """МАТЕРИАЛЫ — новостные заголовки за последние 12 месяцев (дата | заголовок):
{materials}

ИСТОРИЯ ДИВИДЕНДНЫХ ОТСЕЧЕК (факт):
{dividends}

ЦЕНОВАЯ ДИНАМИКА:
{price}

Составь досье в формате markdown со СТРОГО следующими секциями:

# Досье {ticker} на {date}
## 1. Профиль бизнеса
(2-4 предложения: чем зарабатывает, ключевые сегменты)
## 2. Финансовая картина
(последние известные из материалов цифры: прибыль/выручка/дивиденды, тренд)
## 3. Дивиденды
(история выплат, дивидендная политика если известна, что рынок ждёт дальше)
## 4. Открытые риски и катализаторы
(маркированный список конкретных висящих вопросов)
## 5. Консенсус-нарратив
(2-4 предложения: как рынок сейчас смотрит на бумагу и почему)
## 6. ОЖИДАНИЯ
```yaml
expectations:
  - expectation: "<конкретное проверяемое ожидание>"
    status: pending
    if_confirmed: long|short|neutral   # реакция цены если подтвердится
    if_broken: long|short|neutral      # реакция цены если НЕ сбудется/обманет
```
(4-8 ожиданий, от самых важных. Где материалы позволяют — формулируй ЧИСЛЕННО:
например, из прибыли и payout-политики выведи ожидаемый размер дивиденда;
из заявлений менеджмента — ожидаемый диапазон прибыли/выручки.)"""


def load_processed(path: Path) -> set[str]:
    done = set()
    if path.exists():
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("md"):
                        done.add(r["id"])
                except Exception:
                    pass
    return done


def get_api_key() -> str:
    key = os.getenv("DEEPINFRA_API_KEY")
    if not key:
        for line in (PROJECT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
            if line.startswith("DEEPINFRA_API_KEY="):
                key = line.split("=", 1)[1].strip()
    if not key:
        raise RuntimeError("DEEPINFRA_API_KEY not found (env or .env)")
    return key


async def generate_one(client, model: str, ticker: str, date: str,
                       sem: asyncio.Semaphore, ckpt: Path, lock: asyncio.Lock) -> None:
    t = pd.Timestamp(date)
    materials = select_materials(ticker, t)
    sys_p = SYSTEM_PROMPT.format(date=date, name=COMPANY_NAME[ticker], ticker=ticker)
    usr_p = USER_PROMPT.format(materials=materials,
                               dividends=dividends_block(ticker, t),
                               price=price_block(ticker, t),
                               ticker=ticker, date=date)
    rec = {"id": f"{ticker}_{date}", "ticker": ticker, "date": date,
           "model": model, "md": None, "error": None,
           "lookahead_flags": [], "ts": time.time()}
    async with sem:
        for attempt in range(5):
            try:
                extra = ("" if attempt == 0 or not rec["lookahead_flags"] else
                         f"\n\nВНИМАНИЕ: в прошлой версии были упомянуты даты позже "
                         f"{date}: {rec['lookahead_flags']}. Убери всё после {date}.")
                resp = await client.chat.completions.create(
                    model=model, temperature=0.2, max_tokens=2500,
                    messages=[{"role": "system", "content": sys_p},
                              {"role": "user", "content": usr_p + extra}])
                md = resp.choices[0].message.content or ""
                # reasoning-модели (R1) могут отдавать <think> — отрезаем
                md = re.sub(r"<think>.*?</think>", "", md, flags=re.S).strip()
                bad = find_lookahead_dates(md, t)
                if bad and not rec["lookahead_flags"]:
                    rec["lookahead_flags"] = bad[:5]
                    continue  # одна перегенерация с предупреждением
                rec["md"] = md
                rec["lookahead_flags"] = bad[:5]
                rec["usage"] = {"in": resp.usage.prompt_tokens,
                                "out": resp.usage.completion_tokens}
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
        with ckpt.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    if rec["md"]:
        DOSSIER_DIR.mkdir(parents=True, exist_ok=True)
        (DOSSIER_DIR / f"{ticker}_{date}.md").write_text(rec["md"], encoding="utf-8")
        flag = f"  ⚠️lookahead:{rec['lookahead_flags']}" if rec["lookahead_flags"] else ""
        log.info("done %s_%s (%d tok out)%s", ticker, date, rec["usage"]["out"], flag)
    else:
        log.warning("FAIL %s_%s: %s", ticker, date, rec["error"])


async def amain(args) -> int:
    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=get_api_key(), base_url=DEEPINFRA_BASE_URL,
                         max_retries=0, timeout=180)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    model_slug = args.model.split("/")[-1].replace(".", "_").replace("-", "_")
    ckpt = CHECKPOINT_DIR / f"dossiers_{model_slug}.jsonl"
    done = load_processed(ckpt)
    tickers = [x.strip().upper() for x in args.tickers.split(",")] if args.tickers else TICKERS
    dates = [x.strip() for x in args.dates.split(",")] if args.dates else QUARTER_DATES

    todo = [(tk, d) for tk in tickers for d in dates
            if f"{tk}_{d}" not in done]
    log.info("dossiers todo: %d (done: %d)  model=%s", len(todo), len(done), args.model)
    sem = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()
    await asyncio.gather(*(generate_one(client, args.model, tk, d, sem, ckpt, lock)
                           for tk, d in todo))
    await client.close()
    log.info("checkpoint: %s", ckpt)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--tickers", default="")
    ap.add_argument("--dates", default="")
    ap.add_argument("--concurrency", type=int, default=16)
    args = ap.parse_args()
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
