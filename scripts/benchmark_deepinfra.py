r"""
scripts/benchmark_deepinfra.py — измеряет производительность DeepInfra Llama 3.3 70B
на нашем prompt v1.0.0 + русских новостях из Phase 2 dataset.

Цель: оценить throughput и ETA для re-enrichment scope (b) = 19,642 events.

Опирается на тот же prompt + EnrichedNewsPayload validation что Sprint 3/4 enricher.

DeepInfra использует OpenAI-compatible API → стандартный openai SDK с custom base_url.
Endpoint: https://api.deepinfra.com/v1/openai/chat/completions
Model id: meta-llama/Llama-3.3-70B-Instruct

Запуск:
  $env:DEEPINFRA_API_KEY="sk-..."
  python scripts\benchmark_deepinfra.py
  python scripts\benchmark_deepinfra.py --n 30 --concurrency 5

Output:
  - Latency p50/p95 per request
  - Tokens/sec throughput
  - Events/sec sustained estimate
  - ETA для 19,642 events @ this rate
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Optional

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.enricher.prompt import PromptBuilder  # noqa: E402

DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DEEPINFRA_MODEL = "meta-llama/Llama-3.3-70B-Instruct"
DEFAULT_PROMPT_FILE = PROJECT_ROOT / "src" / "services" / "enricher" / "prompts" / "v1_0_0.md"
DEFAULT_TEST_PARQUET = PROJECT_ROOT / "data" / "reenrich_phase2" / "fold13_rolling_12mo_input.parquet"

# Target scope (Sprint 5.6 option b)
TARGET_EVENTS = 19_642

log = logging.getLogger("bench_deepinfra")


async def enrich_one(client, prompt_builder, headline: str, text: str, channel: str) -> dict:
    """One Groq-style chat.completions.create + measure latency + tokens."""
    rendered = prompt_builder.render(headline=headline, text=text, channel=channel)
    t0 = time.perf_counter()
    try:
        resp = await client.chat.completions.create(
            model=DEEPINFRA_MODEL,
            messages=[
                {"role": "system", "content": rendered.system},
                {"role": "user", "content": rendered.user},
            ],
            temperature=0.1,
            max_tokens=400,
            response_format={"type": "json_object"},
        )
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}", "latency_ms": (time.perf_counter() - t0) * 1000}
    latency_ms = (time.perf_counter() - t0) * 1000
    usage = resp.usage
    in_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
    out_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
    return {
        "error": None,
        "latency_ms": latency_ms,
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "total_tokens": in_tokens + out_tokens,
    }


async def run_benchmark(
    api_key: str, n_requests: int, concurrency: int,
    sample_parquet: Path, prompt_file: Path,
) -> None:
    # Load openai SDK
    try:
        from openai import AsyncOpenAI
    except ImportError:
        log.error("openai SDK not installed. Run: pip install openai")
        sys.exit(2)

    if not sample_parquet.exists():
        log.error("Test parquet not found: %s", sample_parquet)
        log.error("Run first: python scripts/extract_phase2_fold13_for_reenrich.py --rolling-months 12")
        sys.exit(2)

    df = pl.read_parquet(str(sample_parquet))
    log.info("Test events available: %d", df.height)
    sample = df.sample(n=min(n_requests, df.height), seed=42)
    log.info("Will send: %d requests at concurrency=%d", sample.height, concurrency)

    client = AsyncOpenAI(api_key=api_key, base_url=DEEPINFRA_BASE_URL)
    prompt_builder = PromptBuilder(prompt_file, "1.0.0")

    sem = asyncio.Semaphore(concurrency)

    async def _one(row):
        async with sem:
            return await enrich_one(
                client, prompt_builder,
                headline=row["headline"] or "",
                text=row["full_text"] or "",
                channel=row["channel"] or "",
            )

    t_start = time.perf_counter()
    log.info("=== Running benchmark... ===")
    tasks = [_one(row) for row in sample.iter_rows(named=True)]
    results = await asyncio.gather(*tasks)
    t_total = time.perf_counter() - t_start

    # Aggregate
    success = [r for r in results if r.get("error") is None]
    failed = [r for r in results if r.get("error") is not None]
    n_ok = len(success)
    n_err = len(failed)

    log.info("")
    log.info("=== Results ===")
    log.info("  Total time:       %.1f sec", t_total)
    log.info("  Success:          %d / %d", n_ok, len(results))
    log.info("  Errors:           %d", n_err)
    if failed:
        log.info("  First errors:")
        for r in failed[:3]:
            log.info("    %s", r["error"])

    if not success:
        log.error("No successful requests — cannot estimate throughput.")
        await client.close()
        return

    latencies = sorted(r["latency_ms"] for r in success)
    in_tokens = [r["input_tokens"] for r in success]
    out_tokens = [r["output_tokens"] for r in success]
    total_tokens_sum = sum(r["total_tokens"] for r in success)

    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0]
    p99 = latencies[int(len(latencies) * 0.99)] if len(latencies) > 1 else latencies[0]

    log.info("")
    log.info("  Latency (per request, with concurrency=%d):", concurrency)
    log.info("    p50:            %.0f ms", p50)
    log.info("    p95:            %.0f ms", p95)
    log.info("    p99:            %.0f ms", p99)
    log.info("    min/max:        %.0f / %.0f ms", min(latencies), max(latencies))
    log.info("")
    log.info("  Tokens:")
    log.info("    in mean:        %.0f", statistics.mean(in_tokens))
    log.info("    out mean:       %.0f", statistics.mean(out_tokens))
    log.info("    total per ev:   %.0f", statistics.mean([r["total_tokens"] for r in success]))
    log.info("")
    log.info("  Throughput (sustained):")
    events_per_sec = n_ok / t_total
    events_per_min = events_per_sec * 60
    events_per_day = events_per_sec * 86_400
    log.info("    %.2f events/sec  =  %.0f events/min  =  %.0f events/day",
             events_per_sec, events_per_min, events_per_day)
    tokens_per_sec = total_tokens_sum / t_total
    log.info("    %.0f tokens/sec", tokens_per_sec)

    log.info("")
    log.info("=== ETA для re-enrichment scope (b) = %d events ===", TARGET_EVENTS)
    eta_sec = TARGET_EVENTS / events_per_sec
    eta_hr = eta_sec / 3600
    eta_day = eta_hr / 24
    if eta_hr < 24:
        log.info("  %.1f hours (%.2f days)", eta_hr, eta_day)
    else:
        log.info("  %.1f days", eta_day)

    log.info("")
    log.info("Cost estimate (DeepInfra Llama 3.3 70B paid):")
    avg_tokens = total_tokens_sum / n_ok
    total_tokens = TARGET_EVENTS * avg_tokens
    # DeepInfra price ~$0.23/M in + $0.40/M out для 70B (как на 2026-05)
    cost_in = (TARGET_EVENTS * statistics.mean(in_tokens)) / 1_000_000 * 0.23
    cost_out = (TARGET_EVENTS * statistics.mean(out_tokens)) / 1_000_000 * 0.40
    log.info("    Total tokens:   %.1f M", total_tokens / 1_000_000)
    log.info("    Cost in:        $%.2f", cost_in)
    log.info("    Cost out:       $%.2f", cost_out)
    log.info("    Total:          $%.2f", cost_in + cost_out)

    await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="DeepInfra Llama 3.3 70B benchmark")
    parser.add_argument("--api-key", default=os.environ.get("DEEPINFRA_API_KEY"),
                        help="DeepInfra API key (or env DEEPINFRA_API_KEY)")
    parser.add_argument("--n", type=int, default=20,
                        help="Number of test requests (default 20)")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="Concurrent requests (default 5)")
    parser.add_argument("--sample", type=Path, default=DEFAULT_TEST_PARQUET,
                        help="Test parquet (default: fold13_rolling_12mo_input.parquet)")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_FILE)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    if not args.api_key:
        log.error("No API key. Set DEEPINFRA_API_KEY env var or pass --api-key.")
        log.error("Get key at: https://deepinfra.com/dash/api_keys")
        return 2

    if args.api_key.startswith("sk-"):
        log.info("API key: sk-...%s", args.api_key[-6:])
    else:
        log.info("API key: %s...%s (unusual format)", args.api_key[:6], args.api_key[-6:])

    asyncio.run(run_benchmark(
        api_key=args.api_key,
        n_requests=args.n,
        concurrency=args.concurrency,
        sample_parquet=args.sample,
        prompt_file=args.prompt,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
