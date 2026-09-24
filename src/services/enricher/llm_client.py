"""Groq LLM client for the Enricher.

Responsibilities:
1. Call Groq Chat Completions API with retry/backoff.
2. Parse JSON from LLM response (resilient to markdown wrappers).
3. Validate against EnrichedNewsPayload schema.
4. Apply whitelist filter to tickers (configurable).

Design: Result-pattern instead of raising exceptions on LLM-side failures.
Enricher decides what to do with each error (retry / DLQ / drop).
Hard infrastructure errors (network, auth) propagate as exceptions.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from groq import AsyncGroq
from groq import APIConnectionError, APIStatusError, RateLimitError
from pydantic import ValidationError

from src.contracts.enriched_news import (
    EnrichedNewsPayload,
    SCHEMA_VERSION as ENRICHED_SCHEMA_VERSION,
)
from src.contracts.raw_news import RawNewsEvent
from src.services.enricher.config import EnricherSettings
from src.services.enricher.prompt import PromptBuilder
from src.services.enricher.key_pool import GroqKeyPool, extract_retry_after

log = logging.getLogger(__name__)


# ============================================================
# Result types
# ============================================================

class EnrichErrorKind(str, Enum):
    """Why enrichment failed (or partially succeeded)."""

    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    API_ERROR = "api_error"
    INVALID_JSON = "invalid_json"
    SCHEMA_VIOLATION = "schema_violation"
    EMPTY_FINANCIAL = "empty_financial"  # is_financial=True но tickers=[] после фильтрации


@dataclass(frozen=True)
class EnrichError:
    kind: EnrichErrorKind
    message: str
    retryable: bool


@dataclass(frozen=True)
class EnrichResult:
    """Outcome of enrich(). Either payload or error is set, not both."""

    payload: EnrichedNewsPayload | None
    error: EnrichError | None
    latency_ms: float
    raw_response: str
    input_tokens: int
    output_tokens: int
    # Optional metadata from Groq response headers — for observability/debug.
    # Set on successful calls; empty dict on failure paths that didn't reach response.
    rate_limit_headers: dict[str, str] | None = None

    @property
    def ok(self) -> bool:
        return self.payload is not None


# ============================================================
# JSON parsing — resilient to markdown wrappers and truncation
# ============================================================

_JSON_MARKDOWN_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_llm_json(raw: str) -> dict[str, Any] | None:
    """Extract a JSON object from arbitrary LLM output.

    Strategies in order:
    1. Wrapped in ```json ... ``` markdown fence.
    2. First {...} block in the text.
    3. Direct parse of stripped text.

    Returns None if all strategies fail.
    """
    if not raw:
        return None

    text = raw.strip()

    # Strategy 1: markdown fence
    m = _JSON_MARKDOWN_RE.search(text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # Strategy 2: first {...} block (greedy — to capture nested objects)
    m = _JSON_OBJECT_RE.search(text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass

    # Strategy 3: direct
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ============================================================
# Whitelist filter
# ============================================================

def filter_tickers_by_whitelist(
    tickers: list[dict[str, Any]],
    allowed: set[str],
    event_id: str,
) -> list[dict[str, Any]]:
    """Drop ticker entries whose .ticker is not in the whitelist.

    Logs a warning for each dropped ticker. Returns filtered list.
    Other validation (confidence range, direction enum) is left to pydantic.
    """
    kept = []
    for t in tickers:
        if not isinstance(t, dict):
            log.warning(
                "ticker_not_a_dict event_id=%s value=%r — dropping",
                event_id, t,
            )
            continue
        ticker_name = t.get("ticker", "")
        if ticker_name in allowed:
            kept.append(t)
        else:
            log.warning(
                "ticker_outside_whitelist event_id=%s ticker=%r — dropping",
                event_id, ticker_name,
            )
    return kept


# ============================================================
# Groq client
# ============================================================

class GroqLLMClient:
    """Async client for Groq Chat Completions with retry, parsing, validation."""

    def __init__(
        self,
        settings: EnricherSettings,
        prompt_builder: PromptBuilder,
        key_pool: GroqKeyPool | None = None,
    ):
        self.settings = settings
        self.prompt = prompt_builder
        if key_pool is not None:
            self.pool = key_pool
        else:
            api_keys = settings.resolve_api_keys()
            if not api_keys:
                raise ValueError(
                    "No Groq API keys configured. Set GROQ_API_KEYS or GROQ_API_KEY."
                )
            # Sprint 5.10: опциональный исходящий SOCKS5 proxy.
            proxy_url = settings.proxy_url if settings.proxy_enabled else None
            self.pool = GroqKeyPool(
                api_keys,
                timeout_sec=settings.groq_timeout_sec,
                proxy_url=proxy_url,
            )
        self._allowed_tickers: set[str] = set(settings.allowed_tickers)
        log.info(
            "GroqLLMClient initialized model=%s n_keys=%d whitelist_validation=%s",
            settings.groq_model, self.pool.size, settings.validate_whitelist,
        )

    @property
    def client(self) -> AsyncGroq:
        """Back-compat: первый клиент из пула. Используется в scripts/test_groq_prompt.py."""
        return self.pool._slots[0].client

    async def enrich(self, raw_event: RawNewsEvent) -> EnrichResult:
        """Process one RawNewsEvent → EnrichResult.

        Hard infrastructure errors (auth, DNS) propagate as exceptions.
        LLM-side failures (timeout, bad JSON, schema) come back as EnrichError.
        """
        # 1. Render prompt
        payload = raw_event.payload
        # Headline = first line of text, fallback to first 200 chars
        first_line = payload.text.split("\n", 1)[0].strip()
        headline = first_line if first_line else payload.text[:200]
        rendered = self.prompt.render(
            headline=headline,
            text=payload.text,
            channel=payload.channel,
        )

        # 2. Call Groq with retry/pool failover
        t_start = time.monotonic()
        try:
            response_text, input_tokens, output_tokens, rl_headers = await self._call_with_retry(
                system=rendered.system,
                user=rendered.user,
            )
        except (APIConnectionError, APIStatusError, RateLimitError) as e:
            latency_ms = (time.monotonic() - t_start) * 1000.0
            return self._classify_call_failure(e, latency_ms)

        latency_ms = (time.monotonic() - t_start) * 1000.0

        # 3. Parse JSON
        parsed = parse_llm_json(response_text)
        if parsed is None:
            log.warning(
                "invalid_json event_id=%s raw_preview=%r",
                raw_event.event_id, response_text[:200],
            )
            return EnrichResult(
                payload=None,
                error=EnrichError(
                    kind=EnrichErrorKind.INVALID_JSON,
                    message="Could not extract JSON from LLM response",
                    retryable=True,
                ),
                latency_ms=latency_ms,
                raw_response=response_text[:10_000],
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                rate_limit_headers=rl_headers,
            )

        # 4. Whitelist filter (configurable)
        if self.settings.validate_whitelist:
            raw_tickers = parsed.get("tickers", [])
            if isinstance(raw_tickers, list):
                parsed["tickers"] = filter_tickers_by_whitelist(
                    raw_tickers, self._allowed_tickers, raw_event.event_id,
                )

        # 5. Compose payload — add fields LLM doesn't produce
        parsed["raw_event_id"] = raw_event.event_id
        parsed["llm_provider"] = "groq"
        parsed["llm_model"] = self.settings.groq_model
        parsed["llm_latency_ms"] = latency_ms
        parsed["llm_input_tokens"] = input_tokens
        parsed["llm_output_tokens"] = output_tokens
        parsed["prompt_version"] = self.settings.prompt_version
        parsed["llm_raw_response"] = response_text[:10_000]
        # Sprint 6: propagate original Telegram timestamp for honest historical
        # replay. Predictor uses this (if set) as reference for features instead
        # of envelope.produced_at. Optional — None = behave as before.
        parsed["tg_published_at"] = raw_event.payload.tg_published_at

        # 6. Pydantic validation
        try:
            payload_obj = EnrichedNewsPayload.model_validate(parsed)
        except ValidationError as e:
            log.warning(
                "schema_violation event_id=%s errors=%s",
                raw_event.event_id, e.error_count(),
            )
            # NOT retryable: LLM детерминистично воспроизводит ту же ошибку на
            # том же тексте. Bug fix 2026-05-29 — раньше было retryable=True
            # что приводило к infinite PEL retries (один забагованный event
            # блокировал consumer).
            return EnrichResult(
                payload=None,
                error=EnrichError(
                    kind=EnrichErrorKind.SCHEMA_VIOLATION,
                    message=str(e)[:1_000],
                    retryable=False,
                ),
                latency_ms=latency_ms,
                raw_response=response_text[:10_000],
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                rate_limit_headers=rl_headers,
            )

        # 7. Post-validation business check: is_financial=True но tickers=[]
        if payload_obj.is_financial and len(payload_obj.tickers) == 0:
            log.warning(
                "empty_financial event_id=%s — is_financial=True но tickers пуст "
                "(возможно тикеры за пределами whitelist)",
                raw_event.event_id,
            )
            return EnrichResult(
                payload=None,
                error=EnrichError(
                    kind=EnrichErrorKind.EMPTY_FINANCIAL,
                    message="is_financial=True but tickers list is empty after whitelist filter",
                    retryable=False,  # не ретраим — LLM упорно повторит то же самое
                ),
                latency_ms=latency_ms,
                raw_response=response_text[:10_000],
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                rate_limit_headers=rl_headers,
            )

        return EnrichResult(
            payload=payload_obj,
            error=None,
            latency_ms=latency_ms,
            raw_response=response_text[:10_000],
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            rate_limit_headers=rl_headers,
        )

    async def _call_with_retry(
        self,
        system: str,
        user: str,
    ) -> tuple[str, int, int, dict[str, str]]:
        """Returns (response_text, input_tokens, output_tokens, rate_limit_headers).

        Стратегия: пытаемся через пул ключей. На 429 переключаем ключ на cooldown
        и пробуем другой. На сетевых ошибках — до groq_max_retries попыток на
        одном ключе.

        Sprint 5.4: на 403 (content filter) делаем ОДИН retry с fallback model
        (groq_fallback_model). Sprint 4.5 finding: tass/interfax 1.5%% даёт 403
        на 8b которые 70b пропускает, и наоборот. Один retry — этого хватает.

        Hard infrastructure errors (auth, схема) — пробрасываем как exceptions.
        """
        # Максимум попыток = размер пула + запас на сетевые ретраи
        max_attempts = max(self.pool.size + 1, self.settings.groq_max_retries + 1)

        last_exc: BaseException | None = None
        fallback_consumed = False  # Sprint 5.4: разрешаем 8b fallback один раз
        current_model = self.settings.groq_model

        for attempt in range(max_attempts):
            client = await self.pool.acquire()
            try:
                # with_raw_response даёт доступ к headers (для x-ratelimit-*)
                raw_resp = await client.chat.completions.with_raw_response.create(
                    model=current_model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=self.settings.groq_temperature,
                    max_tokens=self.settings.groq_max_output_tokens,
                    response_format={"type": "json_object"},
                )
                resp = await raw_resp.parse()
                text = resp.choices[0].message.content or ""
                usage = resp.usage
                in_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
                out_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
                # Extract ratelimit-related headers for observability
                rl_headers: dict[str, str] = {}
                try:
                    for k, v in raw_resp.headers.items():
                        kl = k.lower()
                        if kl.startswith("x-ratelimit-") or kl == "retry-after":
                            rl_headers[kl] = v
                except Exception:
                    pass
                return text, in_tokens, out_tokens, rl_headers
            except RateLimitError as e:
                last_exc = e
                retry_after = extract_retry_after(e)
                self.pool.mark_rate_limited(client, retry_after)
                continue
            except (APIConnectionError, APIStatusError) as e:
                last_exc = e
                if isinstance(e, APIStatusError):
                    status = getattr(e, "status_code", 0)
                    # Sprint 5.4: 403 content filter — пробуем fallback model один раз
                    if status == 403 and not fallback_consumed and current_model != self.settings.groq_fallback_model:
                        fallback_consumed = True
                        log.warning(
                            "content_filter_403 fallback model=%s → %s",
                            current_model, self.settings.groq_fallback_model,
                        )
                        current_model = self.settings.groq_fallback_model
                        continue
                    if 400 <= status < 500 and status != 429:
                        raise
                import asyncio
                await asyncio.sleep(min(2 ** attempt, 8))
                continue
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("unreachable")

    @staticmethod
    def _classify_call_failure(
        exc: BaseException | None,
        latency_ms: float,
    ) -> EnrichResult:
        """Map Groq SDK exceptions to EnrichErrorKind."""
        if isinstance(exc, RateLimitError):
            kind = EnrichErrorKind.RATE_LIMIT
            retryable = True
            msg = f"Groq rate limit: {exc}"
        elif isinstance(exc, APIConnectionError):
            kind = EnrichErrorKind.TIMEOUT
            retryable = True
            msg = f"Groq connection error: {exc}"
        elif isinstance(exc, APIStatusError):
            # 5xx — retryable, 4xx (except 429) — not
            status = getattr(exc, "status_code", 0)
            kind = EnrichErrorKind.API_ERROR
            retryable = status >= 500
            msg = f"Groq API status {status}: {exc}"
        else:
            kind = EnrichErrorKind.API_ERROR
            retryable = False
            msg = f"Unknown error: {exc!r}"

        return EnrichResult(
            payload=None,
            error=EnrichError(kind=kind, message=msg, retryable=retryable),
            latency_ms=latency_ms,
            raw_response="",
            input_tokens=0,
            output_tokens=0,
        )
