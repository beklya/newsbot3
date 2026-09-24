r"""Sprint 9 Фаза 4 — оценка событий на фоне ДОСЬЕ v2 (реальный фундамент).

Apples-to-apples с Sprint 8: те же события и та же машинерия (prompts, JSON-схема,
shuffle-контроль), но контекст = досье v2 из data/kb/dossiers/ (настоящие цифры)
вместо склейки заголовков. Прямое сравнение genuine_surprise hit: 42.5% (Sprint 8)
vs Sprint 9.

Переиспользует из dossier_evaluate: hard_events/impact_events, EvalResult,
extract_json, fresh_tail, SYSTEM/USER промпты, SHUFFLE_MAP, eval-loop.

Usage:
    python scripts/kb_evaluate.py --events all --model deepseek-ai/DeepSeek-R1
    python scripts/kb_evaluate.py --events all --model deepseek-ai/DeepSeek-R1 --shuffle
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))

import scripts.dossier_evaluate as de  # noqa: E402
from scripts.kb_dossier import QUARTER_DATES as KB_QUARTERS, COMPANY_NAME as KB_NAMES  # noqa: E402
from scripts.dossier_build import get_api_key, DEEPINFRA_BASE_URL, DEFAULT_MODEL  # noqa: E402

KB_DIR = PROJECT_ROOT / "data" / "kb"
KB_DOSSIERS = KB_DIR / "dossiers"
KB_CK = KB_DIR / "checkpoints"

# --- Перенаправляем резолвинг досье dossier_evaluate на kb-слой ---
de.DOSSIER_DIR = KB_DOSSIERS
de.QUARTER_DATES = KB_QUARTERS
de.COMPANY_NAME = {**de.COMPANY_NAME, **KB_NAMES}


def build_event_set(impact_min: float) -> pd.DataFrame:
    """Событийный набор (hard + Y6 impact). Кэшируется в parquet — тяжёлый
    скан 871k архива + explode_corpus делается ОДИН раз."""
    cache = KB_DIR / f"events_cache_imp{impact_min}.parquet"
    if cache.exists():
        print(f"events cache hit: {cache.name}", flush=True)
        return pd.read_parquet(cache)
    print("building event set (heavy, once)...", flush=True)
    parts = [de.hard_events(), de.impact_events(impact_min)]
    ev = pd.concat(parts, ignore_index=True).sort_values("dt").reset_index(drop=True)
    ev.to_parquet(cache)
    print(f"event set cached → {cache.name} (n={len(ev)})", flush=True)
    return ev


async def _run_pass(client, args, ev, shuffle: bool) -> None:
    import time
    slug = args.model.split("/")[-1].replace(".", "_").replace("-", "_")
    suffix = "_shuffle" if shuffle else ""
    ckpt = KB_CK / f"kb_evals_{slug}{suffix}.jsonl"
    done = de.load_processed(ckpt)
    todo = [r for r in ev.itertuples(index=False) if r.eid not in done]
    print(f"[{'shuffle' if shuffle else 'honest'}] todo: {len(todo)} "
          f"(done {len(done)}) → {ckpt.name}", flush=True)
    sem = asyncio.Semaphore(args.concurrency)
    lock = asyncio.Lock()
    t0 = time.time()
    BATCH = 200
    for i in range(0, len(todo), BATCH):
        await asyncio.gather(*(de.eval_one(client, args.model, r, shuffle, sem, ckpt, lock)
                               for r in todo[i:i + BATCH]))
        print(f"  [{'shuffle' if shuffle else 'honest'}] "
              f"{min(i + BATCH, len(todo))}/{len(todo)} ({time.time() - t0:.0f}s)", flush=True)


async def amain(args) -> int:
    from openai import AsyncOpenAI
    ev = build_event_set(args.impact_min)
    if args.events == "hard":
        ev = ev[ev["src"] == "hard"]
    elif args.events == "impact":
        ev = ev[ev["src"] == "impact"]
    have = {p.name.rsplit("_", 1)[0] for p in KB_DOSSIERS.glob("*.md")}
    ev = ev[ev["ticker"].isin(have)]
    if args.limit:
        ev = de.stratified_limit(ev, args.limit)
    print(f"events: {len(ev)} ({ev['etype'].value_counts().to_dict()})", flush=True)

    KB_CK.mkdir(parents=True, exist_ok=True)
    client = AsyncOpenAI(api_key=get_api_key(), base_url=DEEPINFRA_BASE_URL,
                         max_retries=0, timeout=240)
    passes = [False, True] if args.both else [args.shuffle]
    for sh in passes:
        await _run_pass(client, args, ev, sh)
    await client.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--events", choices=["hard", "impact", "all"], default="all")
    ap.add_argument("--impact-min", type=float, default=0.6)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--shuffle", action="store_true")
    ap.add_argument("--both", action="store_true",
                    help="honest + shuffle в одном процессе (один холодный импорт)")
    ap.add_argument("--concurrency", type=int, default=12)
    return asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
