"""Tests for EnrichedNewsEvent contract v1.1.0."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.contracts.enriched_news import (
    SCHEMA_VERSION,
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)


def _valid_payload_kwargs(**overrides):
    base = dict(
        raw_event_id="01HWX...",
        llm_provider="groq",
        llm_model="llama-3.1-8b-instant",
        llm_latency_ms=523.5,
        llm_input_tokens=1500,
        llm_output_tokens=200,
        prompt_version="1.0.0",
        is_financial=True,
        tickers=[
            TickerImpact(
                ticker="SBER",
                direction="long",
                confidence=0.85,
                sentiment="positive",
                impact_strength=0.7,
                rationale="Test",
            )
        ],
        summary="Test summary",
        expected_timeframe="medium",
        urgency="high",
        category="cbr",
        is_actionable=True,
    )
    base.update(overrides)
    return base


def test_schema_version_is_1_1_0():
    assert SCHEMA_VERSION == "1.1.0"


def test_default_producer_is_enricher():
    event = EnrichedNewsEvent(payload=EnrichedNewsPayload(**_valid_payload_kwargs()))
    assert event.producer == "enricher"


def test_valid_payload_with_all_fields():
    payload = EnrichedNewsPayload(**_valid_payload_kwargs())
    assert payload.is_financial is True
    assert payload.expected_timeframe == "medium"
    assert payload.urgency == "high"
    assert payload.is_actionable is True


def test_prompt_version_required():
    kwargs = _valid_payload_kwargs()
    del kwargs["prompt_version"]
    with pytest.raises(ValidationError, match="prompt_version"):
        EnrichedNewsPayload(**kwargs)


def test_prompt_version_must_be_semver():
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**_valid_payload_kwargs(prompt_version="1.0"))
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**_valid_payload_kwargs(prompt_version="v1.0.0"))


def test_is_financial_required():
    kwargs = _valid_payload_kwargs()
    del kwargs["is_financial"]
    with pytest.raises(ValidationError, match="is_financial"):
        EnrichedNewsPayload(**kwargs)


def test_non_financial_news_with_empty_tickers_ok():
    """Легитимный кейс: новость о погоде."""
    payload = EnrichedNewsPayload(
        **_valid_payload_kwargs(
            is_financial=False,
            tickers=[],
            summary="Snow in Moscow",
            category="other",
            is_actionable=False,
        )
    )
    assert payload.is_financial is False
    assert payload.tickers == []


def test_expected_timeframe_required():
    kwargs = _valid_payload_kwargs()
    del kwargs["expected_timeframe"]
    with pytest.raises(ValidationError, match="expected_timeframe"):
        EnrichedNewsPayload(**kwargs)


def test_expected_timeframe_enum():
    for tf in ["instant", "short", "medium", "slow"]:
        EnrichedNewsPayload(**_valid_payload_kwargs(expected_timeframe=tf))
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**_valid_payload_kwargs(expected_timeframe="quick"))


def test_urgency_defaults_to_medium():
    kwargs = _valid_payload_kwargs()
    del kwargs["urgency"]
    payload = EnrichedNewsPayload(**kwargs)
    assert payload.urgency == "medium"


def test_category_defaults_to_other():
    kwargs = _valid_payload_kwargs()
    del kwargs["category"]
    payload = EnrichedNewsPayload(**kwargs)
    assert payload.category == "other"


def test_is_actionable_defaults_to_false():
    kwargs = _valid_payload_kwargs()
    del kwargs["is_actionable"]
    payload = EnrichedNewsPayload(**kwargs)
    assert payload.is_actionable is False


def test_ticker_impact_confidence_bounds():
    with pytest.raises(ValidationError):
        TickerImpact(
            ticker="SBER", direction="long", confidence=1.5,
            sentiment="positive", impact_strength=0.5, rationale="",
        )
    with pytest.raises(ValidationError):
        TickerImpact(
            ticker="SBER", direction="long", confidence=-0.1,
            sentiment="positive", impact_strength=0.5, rationale="",
        )


def test_max_20_tickers():
    too_many = [
        TickerImpact(
            ticker="SBER", direction="long", confidence=0.5,
            sentiment="positive", impact_strength=0.5, rationale="",
        )
    ] * 21
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**_valid_payload_kwargs(tickers=too_many))


def test_extra_fields_forbidden():
    """extra='forbid' защищает от опечаток и забытых deprecation."""
    kwargs = _valid_payload_kwargs(urgency_typo="high")  # noqa
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**kwargs)


def test_summary_max_300():
    EnrichedNewsPayload(**_valid_payload_kwargs(summary="X" * 300))
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(**_valid_payload_kwargs(summary="X" * 301))


def test_llm_raw_response_max_10000():
    """В 1.1.0 лимит подняли с 5000 до 10000."""
    EnrichedNewsPayload(
        **_valid_payload_kwargs(llm_raw_response="X" * 10_000)
    )
    with pytest.raises(ValidationError):
        EnrichedNewsPayload(
            **_valid_payload_kwargs(llm_raw_response="X" * 10_001)
        )


def test_event_carries_schema_version_in_envelope():
    event = EnrichedNewsEvent(payload=EnrichedNewsPayload(**_valid_payload_kwargs()))
    assert event.schema_version == "1.1.0"
