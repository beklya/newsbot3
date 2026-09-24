"""Manual prompt testing — run news through Groq, see JSON / latency / warnings.

Usage:
    python scripts/test_groq_prompt.py --limit 10
    python scripts/test_groq_prompt.py --file fixtures/samples.jsonl
    python scripts/test_groq_prompt.py --interactive

No Redis side effects — pure sandbox for iterating on the prompt.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add src/ to path so we can import without installing
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload  # noqa: E402
from src.services.enricher import (  # noqa: E402
    EnricherSettings,
    GroqLLMClient,
    PromptBuilder,
    load_settings,
)


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _build_fake_raw_event(headline: str, text: str | None = None, channel: str = "@test") -> RawNewsEvent:
    """Construct a RawNewsEvent suitable for enrichment (for sandbox use)."""
    import hashlib

    full_text = text or headline
    text_hash = hashlib.sha256(full_text.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

    payload = RawNewsPayload(
        channel=channel,
        message_id=0,
        text=full_text,
        tg_published_at=now,
        received_at=now,
        text_hash=text_hash,
        has_media=False,
        is_reply=False,
        is_forward=False,
    )
    return RawNewsEvent(payload=payload)


async def run_one(client: GroqLLMClient, headline: str, text: str | None = None, channel: str = "@test"):
    print("\n" + "=" * 80)
    print(f"INPUT  : {headline[:120]}")
    if text and text != headline:
        print(f"  text : {text[:200]}")
    print(f"channel: {channel}")
    print("-" * 80)

    event = _build_fake_raw_event(headline, text, channel)
    result = await client.enrich(event)

    print(f"latency: {result.latency_ms:.1f} ms")
    print(f"tokens : in={result.input_tokens}  out={result.output_tokens}")

    # Show ratelimit headers — track TPM remaining / reset / cache hits
    if result.rate_limit_headers:
        h = result.rate_limit_headers
        rem_tok = h.get("x-ratelimit-remaining-tokens", "?")
        lim_tok = h.get("x-ratelimit-limit-tokens", "?")
        reset_tok = h.get("x-ratelimit-reset-tokens", "?")
        rem_req = h.get("x-ratelimit-remaining-requests", "?")
        print(
            f"limits : TPM {rem_tok}/{lim_tok} (reset {reset_tok})  "
            f"RPD remain={rem_req}"
        )

    # Pool snapshot
    pool_stats = client.pool.stats()
    if pool_stats["n_cooldown"] > 0:
        print(
            f"pool   : {pool_stats['n_ready']}/{pool_stats['n_total']} ready, "
            f"earliest in {pool_stats['earliest_ready_in_sec']:.1f}s"
        )

    if result.ok:
        p = result.payload
        print(f"OK     : is_financial={p.is_financial}  is_actionable={p.is_actionable}")
        print(f"  timeframe={p.expected_timeframe}  urgency={p.urgency}  category={p.category}")
        print(f"  summary: {p.summary}")
        if p.tickers:
            print(f"  tickers ({len(p.tickers)}):")
            for t in p.tickers:
                print(
                    f"    {t.ticker:>7}  {t.direction:>5}  "
                    f"conf={t.confidence:.2f}  impact={t.impact_strength:.2f}  "
                    f"sent={t.sentiment:8}  | {t.rationale}"
                )
        else:
            print("  tickers: []")
    else:
        err = result.error
        print(f"ERROR  : kind={err.kind.value}  retryable={err.retryable}")
        print(f"  msg  : {err.message[:300]}")
        if result.raw_response:
            print(f"  raw  : {result.raw_response[:300]}")


async def run_from_file(client: GroqLLMClient, file_path: Path, limit: int | None):
    """Process JSONL file — each line a record with 'headline' and optional 'full_text'."""
    n = 0
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            if limit is not None and n >= limit:
                break
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            headline = rec.get("headline") or rec.get("text", "")
            if not headline:
                continue
            text = rec.get("full_text") or rec.get("text") or headline
            channel = rec.get("channel", "@test")
            await run_one(client, headline, text, channel)
            n += 1


async def run_interactive(client: GroqLLMClient):
    print("Interactive mode. Type a news headline, Enter to send. Empty line to quit.")
    while True:
        try:
            headline = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            return
        if not headline:
            print("Bye.")
            return
        await run_one(client, headline)


# Built-in sample news for quick testing without files
BUILTIN_SAMPLES = [
    ("Банк России принял решение снизить ключевую ставку на 1 п.п. до 20%", None, "@cbr"),
    ("Газпром одобрил выплату дивидендов 51.03 рубля на акцию", None, "@gazp"),
    ("Аналитики БКС повысили целевую цену SBER до 350 рублей", None, "@bcs"),
    ("Мокрый снег и гололедица ожидаются в Москве с воскресенья", None, "@weather"),
    ("В порту Туапсе повреждена транспортная инфраструктура после атаки БПЛА", None, "@news"),
]


async def main():
    parser = argparse.ArgumentParser(description="Test Groq prompt on real news")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of events")
    parser.add_argument("--file", type=Path, default=None, help="JSONL file with news records")
    parser.add_argument("--interactive", action="store_true", help="Type headlines interactively")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    parser.add_argument(
        "--headline", type=str, default=None,
        help="Single headline to test (skip file/interactive/builtin)",
    )
    args = parser.parse_args()

    setup_logging(args.verbose)

    settings = load_settings()
    prompt = PromptBuilder(settings.prompt_file, settings.prompt_version)
    print(f"Loaded prompt v{settings.prompt_version} from {settings.prompt_file.name}")
    print(f"System preview: {prompt.system_preview}...")
    n_keys = len(settings.resolve_api_keys())
    print(
        f"Model: {settings.groq_model}  n_keys={n_keys}  "
        f"whitelist_validation={settings.validate_whitelist}"
    )

    client = GroqLLMClient(settings, prompt)

    try:
        if args.headline:
            await run_one(client, args.headline)
        elif args.interactive:
            await run_interactive(client)
        elif args.file:
            await run_from_file(client, args.file, args.limit)
        else:
            # Run built-in samples
            limit = args.limit if args.limit else len(BUILTIN_SAMPLES)
            for h, t, c in BUILTIN_SAMPLES[:limit]:
                await run_one(client, h, t, c)
    finally:
        await client.pool.close()


if __name__ == "__main__":
    asyncio.run(main())
