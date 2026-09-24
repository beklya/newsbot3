r"""Sprint 8 Этап 2 — оценка событий на фоне point-in-time досье.

Контекст: досье (последний квартал ≤ t) + свежий хвост (топ-20 заголовков
тикера за [дата_досье, t)) + сама новость → LLM → строгий JSON:
{surprise_level, surprise_score, direction, magnitude, expectation_ref, reasoning}.

Режимы:
    --events hard|impact|all   (hard: типизированные 2023+; impact: Y6 ≥порога)
    --limit 100                (калибровочная сотня для A/B, стратифицированно)
    --shuffle                  (контроль: досье ЧУЖОГО тикера, фикс. ротация)

Usage:
    python scripts/dossier_evaluate.py --events all --limit 100        # A/B 70B
    python scripts/dossier_evaluate.py --events all --limit 100 --model deepseek-ai/DeepSeek-R1
    python scripts/dossier_evaluate.py --events all                    # полный прогон
    python scripts/dossier_evaluate.py --events all --shuffle          # контроль
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
from typing import Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

from scripts.dossier_lib import TICKERS, DATA_DIR, load_index  # noqa: E402
from scripts.dossier_build import (  # noqa: E402
    DEEPINFRA_BASE_URL, DEFAULT_MODEL, QUARTER_DATES, COMPANY_NAME,
    CHECKPOINT_DIR, get_api_key,
)
from scripts.hard_events_pilot import extract_events_jsonl, dedup_first_report  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dossier_eval")
logging.getLogger("httpx").setLevel(logging.WARNING)

ARCHIVE = Path(r"D:\quik_sber\newsbot\telegram_news.jsonl")
TICKER_ETYPES = {"DIV_REC", "DIV_NONE", "EARN_UP", "EARN_DOWN",
                 "SANC_RU", "BUYBACK", "SPO_DILUT"}
DOSSIER_DIR = DATA_DIR / "dossiers"
# фиксированная ротация для shuffle-контроля
SHUFFLE_MAP = {tk: TICKERS[(i + 1) % len(TICKERS)] for i, tk in enumerate(TICKERS)}


class EvalResult(BaseModel):
    surprise_level: Literal["confirmed", "priced", "genuine_surprise"]
    surprise_score: float = Field(ge=0.0, le=1.0)
    direction: Literal["long", "short", "neutral"]
    magnitude: Literal["small", "medium", "large"]
    expectation_ref: str = ""
    reasoning: str = ""


SYSTEM_PROMPT = """Ты — финансовый аналитик. Сегодня {date}. У тебя есть досье
компании {name} ({ticker}), составленное ранее, и свежие новости после досье.
Твоя задача — оценить НОВУЮ новость СТРОГО на фоне задокументированных в досье
ожиданий:

- confirmed: новость подтверждает ожидание из досье (рынок этого ждал)
- priced: новость по теме, которая уже многократно обсуждалась/в цене
- genuine_surprise: новость ПРОТИВОРЕЧИТ ожиданиям досье или сообщает
  существенно новый факт, которого в досье нет

direction — ожидаемая реакция ЦЕНЫ АКЦИИ с учётом того, что уже в цене
(не наивный сентимент!): если событие ожидалось — реакция может отсутствовать
или быть обратной.

Ответь ТОЛЬКО валидным JSON без markdown:
{{"surprise_level": "confirmed|priced|genuine_surprise",
"surprise_score": 0.0-1.0, "direction": "long|short|neutral",
"magnitude": "small|medium|large",
"expectation_ref": "<какое ожидание досье затронуто или 'нет в досье'>",
"reasoning": "<до 300 символов>"}}"""

USER_PROMPT = """ДОСЬЕ (составлено {dossier_date}):
{dossier}

СВЕЖИЕ НОВОСТИ ПОСЛЕ ДОСЬЕ (контекст):
{fresh_tail}

=== НОВАЯ НОВОСТЬ ({date}) ===
{headline}
{text}

Оцени новую новость на фоне ожиданий досье. Только JSON."""


# ---------------------------------------------------------------------------
# Сборка событий
# ---------------------------------------------------------------------------
def hard_events() -> pd.DataFrame:
    ev = extract_events_jsonl(ARCHIVE, "2023-04-01", "2026-06-06")
    ev = dedup_first_report(ev)
    ev = ev[ev["etype"].isin(TICKER_ETYPES) & ev["ticker"].isin(TICKERS)].copy()
    ev["eid"] = "H_" + ev["ticker"] + "_" + ev["dt"].dt.strftime("%Y%m%d%H%M")
    ev["text500"] = ""  # у hard есть только headline (текст добавит индекс при матче)
    ev["src"] = "hard"
    return ev[["eid", "dt", "ticker", "etype", "headline", "text500", "src"]]


def impact_events(impact_min: float) -> pd.DataFrame:
    from scripts.analyze_event_impact import explode_corpus
    corp = pd.read_parquet(PROJECT_ROOT / "data/reenrich_phase2/y6_corpus_70b.parquet",
                           columns=["id", "headline", "full_text"])
    corp["id"] = corp["id"].astype(str)
    id2 = {r.id: (r.headline or "", (r.full_text or "")[:500])
           for r in corp.itertuples(index=False)}
    llm = explode_corpus(PROJECT_ROOT / "data/reenrich_phase2/y6_corpus_70b.parquet")
    llm["id"] = llm["id"].astype(str)
    e = llm[(llm["ticker"].isin(TICKERS)) & (llm["impact"] >= impact_min)].copy()
    e["dt"] = pd.to_datetime(e["dt"])
    e = e[e["dt"] >= pd.Timestamp("2025-01-01")]
    e["etype"] = "Y6IMPACT"
    e = dedup_first_report(e.sort_values("dt"))
    e["headline"] = e["id"].map(lambda i: id2.get(i, ("", ""))[0])
    e["text500"] = e["id"].map(lambda i: id2.get(i, ("", ""))[1])
    e["eid"] = "Y_" + e["id"].astype(str) + "_" + e["ticker"]
    e["src"] = "impact"
    return e[["eid", "dt", "ticker", "etype", "headline", "text500", "src"]]


def stratified_limit(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if limit <= 0 or len(df) <= limit:
        return df
    parts = []
    for et, g in df.groupby("etype"):
        k = max(1, round(limit * len(g) / len(df)))
        parts.append(g.sample(n=min(k, len(g)), random_state=42))
    out = pd.concat(parts).drop_duplicates(subset="eid")
    return out.sort_values("dt").head(limit)


# ---------------------------------------------------------------------------
# Контекст
# ---------------------------------------------------------------------------
def latest_dossier(ticker: str, t: pd.Timestamp) -> tuple[str, str] | None:
    dates = [d for d in QUARTER_DATES if pd.Timestamp(d) <= t]
    if not dates:
        return None
    d = dates[-1]
    p = DOSSIER_DIR / f"{ticker}_{d}.md"
    if not p.exists():
        return None
    return d, p.read_text(encoding="utf-8")


def fresh_tail(ticker: str, d_from: str, t: pd.Timestamp, cap: int = 20) -> str:
    df = load_index(ticker)
    w = df[(df["dt"] >= pd.Timestamp(d_from)) & (df["dt"] < t)].copy()
    if w.empty:
        return "(нет)"
    w["prio"] = (w["etype"] != "").astype(int) * 2 + w["is_fin"].astype(int)
    w = w.sort_values(["prio", "dt"], ascending=[False, False]).head(cap).sort_values("dt")
    return "\n".join(f"{r.dt.date()} | {r.headline}" for r in w.itertuples(index=False))


def extract_json(text: str) -> dict:
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S)
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("no json found")
    return json.loads(m.group(0))


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------
async def eval_one(client, model: str, r, shuffle: bool,
                   sem: asyncio.Semaphore, ckpt: Path, lock: asyncio.Lock) -> None:
    t = r.dt
    ctx_ticker = SHUFFLE_MAP[r.ticker] if shuffle else r.ticker
    dos = latest_dossier(ctx_ticker, t)
    rec = {"eid": r.eid, "dt": str(r.dt), "ticker": r.ticker, "etype": r.etype,
           "src": r.src, "model": model, "shuffle": shuffle,
           "ctx_ticker": ctx_ticker, "result": None, "error": None, "ts": time.time()}
    if dos is None:
        rec["error"] = "no_dossier"
    else:
        d_date, d_md = dos
        sys_p = SYSTEM_PROMPT.format(date=t.date(), name=COMPANY_NAME[ctx_ticker],
                                     ticker=ctx_ticker)
        usr_p = USER_PROMPT.format(dossier_date=d_date, dossier=d_md,
                                   fresh_tail=fresh_tail(ctx_ticker, d_date, t),
                                   date=t.date(), headline=r.headline,
                                   text=r.text500 or "")
        async with sem:
            for attempt in range(5):
                try:
                    # Жёсткий asyncio-таймаут: hung-запрос DeepInfra (client
                    # timeout иногда не срабатывает на «тихом» соединении) →
                    # отменяется и ретраится, не клиня весь batch.
                    resp = await asyncio.wait_for(
                        client.chat.completions.create(
                            model=model, temperature=0.1, max_tokens=2000,
                            messages=[{"role": "system", "content": sys_p},
                                      {"role": "user", "content": usr_p}]),
                        timeout=200)
                    raw = resp.choices[0].message.content or ""
                    parsed = EvalResult.model_validate(extract_json(raw))
                    rec["result"] = parsed.model_dump()
                    break
                except (ValidationError, ValueError, json.JSONDecodeError) as e:
                    rec["error"] = f"parse: {str(e)[:200]}"
                    if attempt < 2:
                        continue
                    break
                except asyncio.TimeoutError:
                    rec["error"] = "asyncio_timeout_200s"
                    await asyncio.sleep(1)
                    continue
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


def load_processed(path: Path) -> set[str]:
    done = set()
    if path.exists():
        with path.open(encoding="utf-8") as f:
            for line in f:
                try:
                    rr = json.loads(line)
                    if rr.get("result") or rr.get("error") == "no_dossier":
                        done.add(rr["eid"])
                except Exception:
                    pass
    return done


async def amain(args) -> int:
    from openai import AsyncOpenAI
    parts = []
    if args.events in ("hard", "all"):
        parts.append(hard_events())
    if args.events in ("impact", "all"):
        parts.append(impact_events(args.impact_min))
    ev = pd.concat(parts, ignore_index=True).sort_values("dt")
    ev = stratified_limit(ev, args.limit) if args.limit else ev
    log.info("events: %d  (%s)", len(ev), ev["etype"].value_counts().to_dict())

    model_slug = args.model.split("/")[-1].replace(".", "_").replace("-", "_")
    suffix = "_shuffle" if args.shuffle else ""
    ckpt = CHECKPOINT_DIR / f"evals_{model_slug}{suffix}.jsonl"
    done = load_processed(ckpt)
    todo = [r for r in ev.itertuples(index=False) if r.eid not in done]
    log.info("todo: %d (done: %d) → %s", len(todo), len(done), ckpt.name)

    client = AsyncOpenAI(api_key=get_api_key(), base_url=DEEPINFRA_BASE_URL,
                         max_retries=0, timeout=240)
    sem = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()
    t0 = time.time()
    BATCH = 200
    for i in range(0, len(todo), BATCH):
        await asyncio.gather(*(eval_one(client, args.model, r, args.shuffle,
                                        sem, ckpt, lock)
                               for r in todo[i:i + BATCH]))
        log.info("  ... %d/%d (%.0fs)", min(i + BATCH, len(todo)), len(todo),
                 time.time() - t0)
    await client.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--events", choices=["hard", "impact", "all"], default="all")
    ap.add_argument("--impact-min", type=float, default=0.6)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--concurrency", type=int, default=16)
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
