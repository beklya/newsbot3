"""Shared fixtures for Enricher tests."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
import fakeredis.aioredis

from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.services.enricher.config import EnricherSettings


@pytest.fixture
def enricher_settings(tmp_path: Path, monkeypatch) -> EnricherSettings:
    """Settings with a minimal valid stub prompt file in tmp_path."""
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir()
    stub_prompt = prompts_dir / "v1_0_0.md"
    stub_prompt.write_text(
        "## SYSTEM\nYou are a test analyzer. Respond with JSON.\n\n"
        "## USER_TEMPLATE\nHeadline: {headline}\nText: {text}\nChannel: {channel}",
        encoding="utf-8",
    )
    monkeypatch.setenv("GROQ_API_KEY", "test_key_dummy")
    return EnricherSettings(
        groq_api_key="test_key_dummy",
        prompt_file=stub_prompt,
        prompt_version="1.0.0",
    )


@pytest_asyncio.fixture
async def fake_redis():
    """In-memory Redis (fakeredis) for pipeline integration tests."""
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


@pytest.fixture
def raw_event_factory():
    """Build a valid RawNewsEvent for tests. Each call → unique event_id."""

    def _factory(text: str = "Тестовая новость", channel: str = "@test") -> RawNewsEvent:
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        h = hashlib.sha256(text.encode("utf-8")).hexdigest()
        payload = RawNewsPayload(
            channel=channel,
            message_id=1,
            text=text,
            tg_published_at=now,
            received_at=now,
            text_hash=h,
            has_media=False,
            is_reply=False,
            is_forward=False,
        )
        return RawNewsEvent(payload=payload)

    return _factory


@pytest.fixture
def mock_llm_ok():
    """GroqLLMClient stub that returns a successful EnrichResult."""
    from src.services.enricher.llm_client import EnrichResult
    from src.contracts.enriched_news import EnrichedNewsPayload, TickerImpact

    def _make(tickers: list[dict] | None = None, **payload_overrides):
        ticker_impacts = [
            TickerImpact(
                ticker=t.get("ticker", "SBER"),
                direction=t.get("direction", "long"),
                sentiment=t.get("sentiment", "positive"),
                confidence=t.get("confidence", 0.7),
                impact_strength=t.get("impact_strength", 0.5),
                rationale=t.get("rationale", "test"),
            )
            for t in (tickers or [{"ticker": "SBER"}])
        ]
        payload = EnrichedNewsPayload(
            raw_event_id="will-be-set-by-pipeline",
            llm_provider="groq",
            llm_model="test-model",
            llm_latency_ms=500.0,
            llm_input_tokens=100,
            llm_output_tokens=50,
            prompt_version="1.0.0",
            is_financial=True,
            tickers=ticker_impacts,
            summary="Test summary",
            expected_timeframe="medium",
            urgency="medium",
            category="corporate",
            is_actionable=True,
            llm_raw_response="{}",
            **payload_overrides,
        )
        return EnrichResult(
            payload=payload,
            error=None,
            latency_ms=500.0,
            raw_response="{}",
            input_tokens=100,
            output_tokens=50,
            rate_limit_headers={},
        )

    return _make


@pytest.fixture
def mock_llm_err():
    """GroqLLMClient stub that returns a failure EnrichResult."""
    from src.services.enricher.llm_client import (
        EnrichResult, EnrichError, EnrichErrorKind,
    )

    def _make(kind: EnrichErrorKind, retryable: bool = False, msg: str = "test error"):
        return EnrichResult(
            payload=None,
            error=EnrichError(kind=kind, message=msg, retryable=retryable),
            latency_ms=200.0,
            raw_response="bad json",
            input_tokens=100,
            output_tokens=10,
            rate_limit_headers={},
        )

    return _make
