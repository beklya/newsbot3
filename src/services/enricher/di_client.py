"""DeepInfra LLM client — drop-in replacement for GroqLLMClient.

Uses DeepInfra's OpenAI-compatible API (matches deepinfra_runner.py).
Single API key, simple retry logic, same EnrichResult interface.

Sprint 6.1 finding: v7 trained on DI 70B distribution → using DI as prod
enricher matches train-serve distribution, yields +8.24 Sharpe swing on
identical 1766 VPS events vs Groq.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx
from openai import AsyncOpenAI, APIConnectionError, APIStatusError, APITimeoutError, RateLimitError
from pydantic import ValidationError

from src.contracts.enriched_news import EnrichedNewsPayload
from src.contracts.raw_news import RawNewsEvent
from src.services.enricher.config import EnricherSettings
from src.services.enricher.prompt import PromptBuilder
from src.services.enricher.llm_client import (
    EnrichErrorKind,
    EnrichError,
    EnrichResult,
    parse_llm_json,
    filter_tickers_by_whitelist,
)

log = logging.getLogger(__name__)

DEEPINFRA_BASE_URL = "https://api.deepinfra.com/v1/openai"
DEFAULT_DI_MODEL = "meta-llama/Llama-3.3-70B-Instruct"
DEFAULT_DI_FALLBACK = "meta-llama/Meta-Llama-3.1-8B-Instruct"


class DeepInfraLLMClient:
    """Async OpenAI-compatible client for DeepInfra Chat Completions."""

    def __init__(
        self,
        settings: EnricherSettings,
        prompt_builder: PromptBuilder,
    ):
        self.settings = settings
        self.prompt = prompt_builder
        api_key = getattr(settings, "deepinfra_api_key", None) or ""
        if not api_key:
            raise ValueError(
                "No DeepInfra API key. Set DEEPINFRA_API_KEY env var."
            )
        # Sprint 6.2 — disable httpx keep-alive + explicit per-phase timeouts.
        #
        # Under N=6 parallel workers we observed httpx pooling dead keep-alive
        # sockets to DI (stuck CLOSE-WAIT for minutes; all 6 workers blocked
        # waiting on the shared pool, no exception raised). The fix:
        #   1) max_keepalive_connections=0 — each request opens a fresh
        #      TCP+TLS to api.deepinfra.com. ~100-200ms handshake overhead
        #      is dwarfed by the 5-7s LLM inference time.
        #   2) explicit Timeout(connect, read, write, pool):
        #      - connect=10s  (fail fast on tunnel drops)
        #      - read=60s     (DI 70B occasionally takes 30-45s under load —
        #                     longer than groq_timeout_sec default 10s)
        #      - write=10s, pool=5s
        # Without an explicit http_client, AsyncOpenAI uses its default httpx
        # which keeps 20 idle sockets — works under serial load, breaks our
        # 6-worker fan-out the moment DI server drops a keep-alive.
        proxy_url = settings.proxy_url if settings.proxy_enabled else None
        limits = httpx.Limits(max_keepalive_connections=0)
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
        http_client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            proxy=proxy_url,  # None unless proxy_enabled
        )
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=DEEPINFRA_BASE_URL,
            timeout=timeout,
            http_client=http_client,
            max_retries=0,  # we handle retry ourselves
        )
        self.model = getattr(settings, "deepinfra_model", None) or DEFAULT_DI_MODEL
        self.fallback_model = getattr(settings, "deepinfra_fallback_model", None) or DEFAULT_DI_FALLBACK
        self._allowed_tickers: set[str] = set(settings.allowed_tickers)
        log.info(
            "DeepInfraLLMClient initialized model=%s fallback=%s "
            "whitelist_validation=%s proxy=%s",
            self.model, self.fallback_model,
            settings.validate_whitelist,
            bool(proxy_url),
        )

    @property
    def size(self) -> int:
        """Back-compat with GroqLLMClient.pool.size."""
        return 1

    @property
    def pool(self) -> "_DIPoolShim":
        """Back-compat shim — enricher heartbeat + __main__ call .pool.stats() / close().

        We're single-key, so return a stub object that returns empty stats and
        no-op close.  Avoids modifying production __main__.py.
        """
        return _DIPoolShim(self)

    async def enrich(self, raw_event: RawNewsEvent) -> EnrichResult:
        """Mirror of GroqLLMClient.enrich. Returns EnrichResult."""
        payload = raw_event.payload
        first_line = payload.text.split("\n", 1)[0].strip()
        headline = first_line if first_line else payload.text[:200]
        rendered = self.prompt.render(
            headline=headline,
            text=payload.text,
            channel=payload.channel,
        )

        t_start = time.monotonic()
        try:
            (response_text, input_tokens, output_tokens,
             used_model, rl_headers) = await self._call_with_retry(
                system=rendered.system,
                user=rendered.user,
            )
        except (APIConnectionError, APITimeoutError, APIStatusError,
                RateLimitError) as e:
            latency_ms = (time.monotonic() - t_start) * 1000.0
            return self._classify_call_failure(e, latency_ms)

        latency_ms = (time.monotonic() - t_start) * 1000.0

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

        if self.settings.validate_whitelist:
            raw_tickers = parsed.get("tickers", [])
            if isinstance(raw_tickers, list):
                parsed["tickers"] = filter_tickers_by_whitelist(
                    raw_tickers, self._allowed_tickers, raw_event.event_id,
                )

        # Compose payload — llm_provider stays "groq" because EnrichedNewsPayload
        # Literal allows only ["groq", "ollama"].  Real provider visible via
        # llm_model = "meta-llama/Llama-3.3-70B-Instruct" (vs Groq's
        # "llama-3.3-70b-versatile") and via prompt_version+enrich_model on
        # the runner side.
        parsed["raw_event_id"] = raw_event.event_id
        parsed["llm_provider"] = "groq"   # Literal limit — see contract
        parsed["llm_model"] = used_model
        parsed["llm_latency_ms"] = latency_ms
        parsed["llm_input_tokens"] = input_tokens
        parsed["llm_output_tokens"] = output_tokens
        parsed["prompt_version"] = self.settings.prompt_version
        parsed["llm_raw_response"] = response_text[:10_000]
        parsed["tg_published_at"] = raw_event.payload.tg_published_at

        try:
            payload_obj = EnrichedNewsPayload.model_validate(parsed)
        except ValidationError as e:
            log.warning(
                "schema_violation event_id=%s errors=%s",
                raw_event.event_id, e.error_count(),
            )
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

        if payload_obj.is_financial and len(payload_obj.tickers) == 0:
            log.warning("empty_financial event_id=%s", raw_event.event_id)
            return EnrichResult(
                payload=None,
                error=EnrichError(
                    kind=EnrichErrorKind.EMPTY_FINANCIAL,
                    message="is_financial=True but tickers list is empty",
                    retryable=False,
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
        self, system: str, user: str,
    ) -> tuple[str, int, int, str, dict[str, str]]:
        """Single-key retry on transient errors + 403 fallback to 8B."""
        max_attempts = self.settings.groq_max_retries + 1
        current_model = self.model
        fallback_used = False
        last_exc: BaseException | None = None

        for attempt in range(max_attempts):
            try:
                response = await self.client.chat.completions.create(
                    model=current_model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=self.settings.groq_temperature,
                    max_tokens=self.settings.groq_max_output_tokens,
                    response_format={"type": "json_object"},
                )
                text = response.choices[0].message.content or ""
                usage = response.usage
                in_tokens = getattr(usage, "prompt_tokens", 0) if usage else 0
                out_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
                return text, in_tokens, out_tokens, current_model, {}
            except APIStatusError as e:
                status = getattr(e, "status_code", 0)
                if status == 403 and not fallback_used:
                    log.warning(
                        "403 from DI %s on attempt %d, retrying with fallback %s",
                        current_model, attempt + 1, self.fallback_model,
                    )
                    current_model = self.fallback_model
                    fallback_used = True
                    continue
                if status == 429:
                    sleep = min(2 ** attempt, 30)
                    log.warning("429 from DI, backing off %ds", sleep)
                    await asyncio.sleep(sleep)
                    last_exc = e
                    continue
                if 500 <= status < 600:
                    sleep = min(2 ** attempt, 10)
                    log.warning("%d from DI, retrying in %ds", status, sleep)
                    await asyncio.sleep(sleep)
                    last_exc = e
                    continue
                raise
            except (APIConnectionError, APITimeoutError, RateLimitError) as e:
                last_exc = e
                sleep = min(2 ** attempt, 5)
                log.warning("DI transient error %s, retry in %ds",
                            type(e).__name__, sleep)
                await asyncio.sleep(sleep)

        if last_exc:
            raise last_exc
        raise RuntimeError("DI retries exhausted without exception")

    def _classify_call_failure(self, exc: BaseException,
                                latency_ms: float) -> EnrichResult:
        """Map exception to EnrichError (mirrors GroqLLMClient logic)."""
        if isinstance(exc, RateLimitError):
            kind = EnrichErrorKind.RATE_LIMIT
        elif isinstance(exc, (APIConnectionError, APITimeoutError)):
            kind = EnrichErrorKind.TIMEOUT
        elif isinstance(exc, APIStatusError):
            status = getattr(exc, "status_code", 0)
            if status == 403:
                kind = EnrichErrorKind.CONTENT_FILTER
            elif status >= 500:
                kind = EnrichErrorKind.SERVER_ERROR
            else:
                kind = EnrichErrorKind.OTHER
        else:
            kind = EnrichErrorKind.OTHER
        return EnrichResult(
            payload=None,
            error=EnrichError(kind=kind, message=str(exc)[:500], retryable=True),
            latency_ms=latency_ms,
            raw_response="",
            input_tokens=0,
            output_tokens=0,
            rate_limit_headers={},
        )


class _DIPoolShim:
    """Stub for GroqKeyPool interface (stats / close / size) — heartbeat back-compat."""
    def __init__(self, parent: "DeepInfraLLMClient"):
        self._parent = parent

    @property
    def size(self) -> int:
        return 1

    def stats(self) -> dict:
        # Mirror GroqKeyPool.stats() shape so heartbeat snapshot_fn (which expects
        # n_total / n_ready / n_cooldown) works without changes to __main__.py.
        return {
            "n_total": 1, "n_ready": 1, "n_cooldown": 0,
            "n_keys": 1, "provider": "deepinfra",
        }

    async def close(self) -> None:
        try:
            await self._parent.client.close()
        except Exception:
            pass
