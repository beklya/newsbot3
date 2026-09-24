r"""
scripts/deepinfra_runner.py — DeepInfra parallel re-enrichment runner.

Использует:
  - OpenAI SDK с base_url=https://api.deepinfra.com/v1/openai
  - Llama 3.3 70B Instruct (та же модель что Groq llama-3.3-70b-versatile)
  - Prompt v1.0.0 (тот же что в src/services/enricher/prompts/)
  - Append-only checkpoint (jsonl, одна строка на event)

Особенности:
  - Один API key (DEEPINFRA_API_KEY); лимит DeepInfra: 200 concurrent / model / account
  - Throughput ограничен --concurrency
  - Прямой asyncio.gather с Semaphore
  - Retry: 429 (concurrent overflow) и 5xx — exponential backoff
  - 400/403 → terminal, в checkpoint как error
  - Per-response checkpoint flush (resumable на KeyboardInterrupt)

Usage:
    $env:DEEPINFRA_API_KEY = "..."     # in PowerShell
    .\.venv\Scripts\python.exe scripts\deepinfra_runner.py `
        --input data\reenrich_phase2\fold13_rolling_12mo_input.parquet `
        --output-dir data\reenrich_phase2\checkpoints
        # default --concurrency 50, --prompt-version 1.0.0

Resume:
    Перезапустить ту же команду — все уже-processed IDs пропустятся.

Output:
    <output-dir>/checkpoint_llama_3_3_70b_versatile_v1_0_0.jsonl
    Полностью compatible с sprint4/reenrich/aggregate_checkpoint.py.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.enricher.llm_client import (  # noqa: E402
    filter_tickers_by_whitelist,
    parse_llm_json,
)
from src.services.enricher.prompt import PromptBuilder  # noqa: E402

# DeepInfra config
DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DEFAULT_DEEPINFRA_MODEL_ID = "meta-llama/Llama-3.3-70B-Instruct"

# Model → pricing (DeepInfra as of 2026-05; blended in+out approx)
MODEL_PRICING = {
    "meta-llama/Llama-3.3-70B-Instruct": (0.23, 0.40),   # $0.23/M in, $0.40/M out
    "meta-llama/Meta-Llama-3.1-70B-Instruct": (0.23, 0.40),
    "meta-llama/Meta-Llama-3.1-8B-Instruct": (0.03, 0.05),  # 6-8x cheaper
    "meta-llama/Llama-3.1-8B-Instruct": (0.03, 0.05),
}

# Model → checkpoint base name (without suffix). aggregate_checkpoint.py reads this.
MODEL_CHECKPOINT_BASE = {
    "meta-llama/Llama-3.3-70B-Instruct": "checkpoint_llama_3_3_70b_versatile",
    "meta-llama/Meta-Llama-3.1-70B-Instruct": "checkpoint_llama_3_1_70b",
    "meta-llama/Meta-Llama-3.1-8B-Instruct": "checkpoint_llama_3_1_8b",
    "meta-llama/Llama-3.1-8B-Instruct": "checkpoint_llama_3_1_8b",
}

# Prompt
DEFAULT_PROMPT_FILE = PROJECT_ROOT / "src" / "services" / "enricher" / "prompts" / "v1_0_0.md"
DEFAULT_PROMPT_VERSION = "1.0.0"
DEFAULT_INPUT = PROJECT_ROOT / "data" / "reenrich_phase2" / "fold13_rolling_12mo_input.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "reenrich_phase2" / "checkpoints"

# Whitelist (legacy + canonical имена, normalize_ticker в EnrichedNewsPayload validator)
WHITELIST_TICKERS = {
    "SBER", "GAZP", "LKOH", "YNDX", "ROSN", "GMKN", "NVTK",
    "TATN", "MGNT", "MTSS", "PLZL", "VTBR",
    "Si", "MX", "BR", "NG", "GOLD", "CNY", "USDRUB",
}

# Retry policy
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.5  # sec
RETRY_BACKOFF_MAX = 30.0
RETRY_ERROR_STATUSES = {429, 500, 502, 503, 504}

# Progress log frequency
PROGRESS_EVERY = 50

log = logging.getLogger("deepinfra_runner")


# ----------------------------------------------------------------------------
# Checkpoint I/O
# ----------------------------------------------------------------------------
def load_processed_ids(checkpoint_path: Path) -> set[str]:
    """Read existing checkpoint, return set of processed ids.

    Считаем event processed только если error is None (success). На retryable errors
    запись в checkpoint НЕ пишется.
    """
    if not checkpoint_path.exists():
        return set()
    processed: set[str] = set()
    n_lines = 0
    n_malformed = 0
    with open(checkpoint_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n_lines += 1
            try:
                rec = json.loads(line)
            except Exception:
                n_malformed += 1
                continue
            eid = rec.get("id")
            if eid:
                processed.add(eid)
    log.info(
        "Loaded checkpoint %s: %d lines, %d unique processed ids (%d malformed)",
        checkpoint_path.name, n_lines, len(processed), n_malformed,
    )
    return processed


def append_checkpoint(checkpoint_path: Path, record: dict, lock: asyncio.Lock):
    """Sync write — wrap в lock из main loop."""
    with open(checkpoint_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ----------------------------------------------------------------------------
# Enrichment call with retries
# ----------------------------------------------------------------------------
async def enrich_one_with_retries(
    client: Any,
    prompt: PromptBuilder,
    headline: str,
    text: str,
    channel: str,
    *,
    temperature: float = 0.1,
    max_tokens: int = 400,
) -> dict[str, Any]:
    """Call DeepInfra + parse + whitelist filter, with retry on 429/5xx."""
    from openai import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError

    rendered = prompt.render(headline=headline, text=text, channel=channel)
    model_id = getattr(prompt, "_runner_model_id", DEFAULT_DEEPINFRA_MODEL_ID)

    last_err: Optional[str] = None
    for attempt in range(MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            resp = await client.chat.completions.create(
                model=model_id,
                messages=[
                    {"role": "system", "content": rendered.system},
                    {"role": "user", "content": rendered.user},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            break  # success
        except RateLimitError as e:
            last_err = f"rate_limit: {str(e)[:200]}"
            if attempt >= MAX_RETRIES:
                return {"error": "rate_limit", "message": last_err}
        except APITimeoutError as e:
            last_err = f"timeout: {str(e)[:200]}"
            if attempt >= MAX_RETRIES:
                return {"error": "timeout", "message": last_err}
        except APIConnectionError as e:
            last_err = f"connection: {str(e)[:200]}"
            if attempt >= MAX_RETRIES:
                return {"error": "connection", "message": last_err}
        except APIStatusError as e:
            status = getattr(e, "status_code", 0)
            if status in RETRY_ERROR_STATUSES:
                last_err = f"api_status_{status}: {str(e)[:200]}"
                if attempt >= MAX_RETRIES:
                    return {"error": f"api_status_{status}", "message": last_err}
            else:
                # 400/403/404 — terminal
                return {"error": f"api_status_{status}", "message": str(e)[:300]}
        except Exception as e:
            return {"error": "unknown", "message": f"{type(e).__name__}: {e}"[:300]}

        # Exponential backoff
        wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
        await asyncio.sleep(wait)
    else:
        return {"error": "max_retries", "message": last_err or "unknown"}

    # --- Success path ---
    latency_ms = (time.monotonic() - t0) * 1000.0
    text_resp = resp.choices[0].message.content or ""
    usage = resp.usage
    in_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
    out_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
    total_tokens = in_tokens + out_tokens

    parsed = parse_llm_json(text_resp)
    if parsed is None:
        return {
            "error": "invalid_json",
            "raw_response": text_resp[:5000],
            "latency_ms": round(latency_ms, 1),
            "input_tokens": in_tokens,
            "output_tokens": out_tokens,
            "total_tokens": total_tokens,
        }

    # Whitelist filter — same logic as the enricher (filter_tickers_by_whitelist)
    raw_tickers = parsed.get("tickers", [])
    if isinstance(raw_tickers, list):
        parsed["tickers"] = filter_tickers_by_whitelist(
            raw_tickers, WHITELIST_TICKERS, event_id="batch",
        )

    return {
        "error": None,
        "parsed": parsed,
        "raw_response": text_resp[:5000],
        "latency_ms": round(latency_ms, 1),
        "input_tokens": in_tokens,
        "output_tokens": out_tokens,
        "total_tokens": total_tokens,
    }


# ----------------------------------------------------------------------------
# Worker
# ----------------------------------------------------------------------------
async def process_event(
    client: Any,
    prompt: PromptBuilder,
    row: dict,
    sem: asyncio.Semaphore,
    checkpoint_path: Path,
    checkpoint_lock: asyncio.Lock,
    stats: dict,
    prompt_version: str,
    temperature: float,
    max_tokens: int,
) -> None:
    async with sem:
        result = await enrich_one_with_retries(
            client, prompt,
            headline=row.get("headline") or "",
            text=row.get("full_text") or "",
            channel=row.get("channel") or "",
            temperature=temperature,
            max_tokens=max_tokens,
        )

    # Update stats
    if result.get("error") is None:
        stats["n_success"] += 1
    elif result.get("error") in {"rate_limit", "timeout", "connection",
                                   "api_status_500", "api_status_502", "api_status_503",
                                   "api_status_504", "max_retries"}:
        # Retryable терминальный errors: write to log, NO checkpoint (резюме)
        stats["n_retryable_err"] += 1
        log.warning("retryable_fail id=%s err=%s", row.get("id"), result.get("error"))
        return  # don't write checkpoint → next run re-tries
    else:
        # Terminal errors: 400/403/invalid_json/schema_violation — write to checkpoint
        stats["n_terminal_err"] += 1

    # Checkpoint write
    rec = {
        "id": row.get("id"),
        "tg_msg_id": row.get("tg_msg_id"),
        "channel": row.get("channel"),
        "datetime_msk": str(row.get("datetime_msk")) if row.get("datetime_msk") else None,
        "timestamp_utc": row.get("timestamp_utc"),
        "text_hash": row.get("text_hash"),
        "has_phase2_anchor": row.get("has_phase2_anchor", False),
        "model": getattr(prompt, "_runner_model_id", DEFAULT_DEEPINFRA_MODEL_ID),
        "prompt_version": prompt_version,
        "key_id": 0,  # DeepInfra single key
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "error": result.get("error"),
        "error_message": result.get("message"),
        "latency_ms": result.get("latency_ms"),
        "input_tokens": result.get("input_tokens", 0),
        "output_tokens": result.get("output_tokens", 0),
        "total_tokens": result.get("total_tokens", 0),
        "raw_response": result.get("raw_response", ""),
        "parsed": result.get("parsed"),
    }
    async with checkpoint_lock:
        append_checkpoint(checkpoint_path, rec, checkpoint_lock)

    # Token counters for cost estimate
    if result.get("input_tokens"):
        stats["total_in_tokens"] += result["input_tokens"]
    if result.get("output_tokens"):
        stats["total_out_tokens"] += result["output_tokens"]


# ----------------------------------------------------------------------------
# Stats reporter
# ----------------------------------------------------------------------------
async def stats_reporter(stats: dict, total: int, period_sec: float = 10.0):
    """Periodic progress logger; cancelled when main loop done."""
    last_done = 0
    while True:
        await asyncio.sleep(period_sec)
        done = stats["n_success"] + stats["n_terminal_err"]
        elapsed = time.time() - stats["t_start"]
        if elapsed <= 0:
            continue
        ev_per_sec = done / elapsed
        ev_per_min = ev_per_sec * 60
        remaining = max(0, total - done)
        eta_sec = remaining / ev_per_sec if ev_per_sec > 0 else 0
        eta_min = eta_sec / 60
        cost_in = stats["total_in_tokens"] / 1_000_000 * 0.23
        cost_out = stats["total_out_tokens"] / 1_000_000 * 0.40
        log.info(
            "[%d/%d done=%.1f%%] ok=%d term_err=%d retry=%d  %.2f ev/s  ETA %.1f min  cost so far ~$%.2f",
            done, total, 100.0 * done / max(total, 1),
            stats["n_success"], stats["n_terminal_err"], stats["n_retryable_err"],
            ev_per_sec, eta_min, cost_in + cost_out,
        )


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
async def run(args):
    from openai import AsyncOpenAI

    # 1. Load input
    df = pl.read_parquet(str(args.input))
    log.info("Input loaded: %d rows from %s", df.height, args.input)

    # 2. Setup output path
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Checkpoint filename: derive from model + prompt_version + optional suffix
    # Each (model, prompt_version, suffix) combo gets its own checkpoint to avoid
    # dedup against unrelated runs.
    model_base = MODEL_CHECKPOINT_BASE.get(args.model, "checkpoint_unknown_model")
    pv_tag = args.prompt_version.replace(".", "_")
    suffix_tag = f"_{args.checkpoint_suffix}" if args.checkpoint_suffix else ""
    checkpoint_filename = f"{model_base}_v{pv_tag}{suffix_tag}.jsonl"
    checkpoint_path = args.output_dir / checkpoint_filename
    log.info("Checkpoint: %s", checkpoint_path)
    log.info("Model: %s", args.model)

    # 3. Resume — filter out already-processed IDs
    processed = load_processed_ids(checkpoint_path)
    pending_df = df.filter(~pl.col("id").is_in(list(processed)))
    if args.limit:
        pending_df = pending_df.head(args.limit)
    rows = list(pending_df.iter_rows(named=True))
    log.info(
        "Pending: %d events  (already processed: %d  from prior runs)",
        len(rows), len(processed),
    )
    if not rows:
        log.info("Nothing to do. All events already processed.")
        return 0

    # 4. Cost preview
    est_in = len(rows) * 4100  # avg input tokens per event (system + user prompt)
    est_out = len(rows) * 130  # avg output tokens
    price_in, price_out = MODEL_PRICING.get(args.model, (0.23, 0.40))
    est_cost = (est_in / 1_000_000 * price_in) + (est_out / 1_000_000 * price_out)
    log.info(
        "Cost preview: ~%.1f M in + %.1f M out tokens  ≈ $%.2f ($%.2f/M in, $%.2f/M out)",
        est_in / 1_000_000, est_out / 1_000_000, est_cost, price_in, price_out,
    )

    # 5. Setup client
    client = AsyncOpenAI(
        api_key=args.api_key,
        base_url=DEEPINFRA_BASE_URL,
        timeout=args.timeout,
        max_retries=0,  # we handle retries ourselves
    )
    # Auto-locate prompt file by prompt_version if explicit --prompt not provided
    prompt_path = args.prompt
    if not prompt_path.exists():
        # Try to auto-find from version: e.g. "2.1.0" → prompts/v2_1_0.md
        pv_filename = "v" + args.prompt_version.replace(".", "_") + ".md"
        candidate = PROJECT_ROOT / "src" / "services" / "enricher" / "prompts" / pv_filename
        if candidate.exists():
            log.info("Auto-located prompt file: %s", candidate)
            prompt_path = candidate
        else:
            log.error("Prompt file not found: %s (and auto-search %s also missing)", args.prompt, candidate)
            return 2
    prompt = PromptBuilder(prompt_path, args.prompt_version)
    # Tag model_id onto prompt object so enrich_one knows which model to call
    prompt._runner_model_id = args.model
    log.info("DeepInfra client ready  concurrency=%d  model=%s  prompt=%s",
             args.concurrency, args.model, prompt_path.name)

    # 6. Run
    sem = asyncio.Semaphore(args.concurrency)
    checkpoint_lock = asyncio.Lock()
    stats = {
        "n_success": 0,
        "n_terminal_err": 0,
        "n_retryable_err": 0,
        "total_in_tokens": 0,
        "total_out_tokens": 0,
        "t_start": time.time(),
    }

    reporter_task = asyncio.create_task(stats_reporter(stats, len(rows)))

    try:
        await asyncio.gather(
            *[process_event(
                client, prompt, row, sem,
                checkpoint_path, checkpoint_lock, stats,
                args.prompt_version, args.temperature, args.max_output_tokens,
            ) for row in rows],
            return_exceptions=False,
        )
    finally:
        reporter_task.cancel()
        try:
            await reporter_task
        except asyncio.CancelledError:
            pass
        await client.close()

    # 7. Final summary
    elapsed = time.time() - stats["t_start"]
    total_tokens = stats["total_in_tokens"] + stats["total_out_tokens"]
    cost = stats["total_in_tokens"] / 1_000_000 * 0.23 + stats["total_out_tokens"] / 1_000_000 * 0.40
    log.info("")
    log.info("=== DONE ===")
    log.info("  Total pending:     %d", len(rows))
    log.info("  Success:           %d", stats["n_success"])
    log.info("  Terminal errors:   %d", stats["n_terminal_err"])
    log.info("  Retryable (skip):  %d (re-run чтобы повторить)", stats["n_retryable_err"])
    log.info("  Elapsed:           %.1f min", elapsed / 60)
    log.info("  Throughput:        %.2f events/sec  (concurrency=%d)",
             (stats["n_success"] + stats["n_terminal_err"]) / max(elapsed, 1), args.concurrency)
    log.info("  Tokens:            %.1f M total (%.1f M in + %.1f M out)",
             total_tokens / 1e6, stats["total_in_tokens"] / 1e6, stats["total_out_tokens"] / 1e6)
    log.info("  Cost:              $%.2f (in $%.2f + out $%.2f)",
             cost, stats["total_in_tokens"] / 1_000_000 * 0.23, stats["total_out_tokens"] / 1_000_000 * 0.40)
    log.info("")
    log.info("Checkpoint: %s", checkpoint_path)
    log.info("Aggregate when done:")
    log.info("  python sprint4/reenrich/aggregate_checkpoint.py --model llama-3.3-70b-versatile \\")
    log.info("      --checkpoint %s --sample %s --output data/reenrich_phase2/fold13_rolling_12mo_70b.parquet",
             checkpoint_path.name, args.input.name)
    return 0


def main():
    parser = argparse.ArgumentParser(description="DeepInfra parallel re-enrichment runner")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                        help="Input parquet (must have id, headline, full_text, channel cols)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--api-key", default=os.environ.get("DEEPINFRA_API_KEY"),
                        help="DeepInfra API key (or env DEEPINFRA_API_KEY)")
    parser.add_argument("--concurrency", type=int, default=50,
                        help="Concurrent requests (DeepInfra limit: 200/account/model, default 50)")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_FILE)
    parser.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument("--max-output-tokens", type=int, default=400)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--limit", type=int, default=None, help="Process only first N pending (for testing)")
    parser.add_argument("--model", default=DEFAULT_DEEPINFRA_MODEL_ID,
                        help=f"DeepInfra model ID (default: {DEFAULT_DEEPINFRA_MODEL_ID}). "
                             f"Supported: {list(MODEL_CHECKPOINT_BASE.keys())}")
    parser.add_argument("--checkpoint-suffix", default="",
                        help="Optional suffix for checkpoint filename — useful чтобы variants "
                             "разных promptов не дедупались по id (e.g. 'sample100' or 'v21_test')")
    args = parser.parse_args()
    if args.model not in MODEL_PRICING:
        print(f"WARNING: unknown model {args.model}, using default 70B pricing", file=sys.stderr)

    if not args.api_key:
        print("ERROR: No DeepInfra API key. Set DEEPINFRA_API_KEY env var or pass --api-key.", file=sys.stderr)
        print("Get key at: https://deepinfra.com/dash/api_keys", file=sys.stderr)
        return 2

    # Log setup — stdout + file
    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "deepinfra_runner.log"

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)
    fh = logging.FileHandler(str(log_file), mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    log.info("Logging to file: %s", log_file)
    if args.api_key.startswith("sk-"):
        log.info("API key: sk-...%s", args.api_key[-6:])
    else:
        log.info("API key: %s...%s", args.api_key[:6], args.api_key[-6:])

    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("Interrupted — checkpoint preserved. Re-run same command to resume.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
