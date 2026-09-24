"""
tests/contracts/test_enriched_news_normalization.py — Sprint 4 / Commit 4.1

Интеграционные тесты для ticker normalization в EnrichedNewsEvent v1.1.0.

Что проверяется:
  - TickerImpact принимает legacy имена и нормализует
  - TickerImpact отбрасывает unknown тикеры через ValidationError
  - Существующие golden samples (Sprint 1) продолжают работать
  - Полный EnrichedNewsPayload с тикерами проходит валидацию
  - frozen=True не мешает validator'у на этапе создания

Цель: гарантировать, что patch контракта не сломал ничего существующего
и добавил ожидаемое поведение.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.contracts.enriched_news import (
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)


# =============================================================================
# Базовые фикстуры
# =============================================================================
def make_ticker_impact(ticker: str, **overrides) -> TickerImpact:
    """Helper: создаёт минимальный валидный TickerImpact с заданным ticker."""
    defaults = {
        "ticker": ticker,
        "direction": "long",
        "confidence": 0.7,
        "sentiment": "positive",
        "impact_strength": 0.5,
        "rationale": "test",
    }
    defaults.update(overrides)
    return TickerImpact(**defaults)


def make_minimal_payload(tickers_data: list[dict] | None = None) -> dict:
    """Helper: минимальный valid payload-словарь."""
    return {
        "raw_event_id": "01TEST00000000000000000000",
        "llm_provider": "groq",
        "llm_model": "llama-3.3-70b-versatile",
        "llm_latency_ms": 800.0,
        "llm_input_tokens": 500,
        "llm_output_tokens": 100,
        "prompt_version": "1.0.0",
        "is_financial": True,
        "tickers": tickers_data or [],
        "summary": "test",
        "expected_timeframe": "short",
        "llm_raw_response": "{}",
    }


# =============================================================================
# TickerImpact normalization
# =============================================================================
class TestTickerImpactNormalization:

    @pytest.mark.parametrize("legacy,canonical", [
        ("Si", "SI"),
        ("MX", "MIX"),
        ("YNDX", "YDEX"),
        ("GOLD", "GLDRUB"),
    ])
    def test_legacy_normalized_to_canonical(self, legacy, canonical):
        impact = make_ticker_impact(ticker=legacy)
        assert impact.ticker == canonical

    @pytest.mark.parametrize("canonical", [
        "SBER", "GAZP", "LKOH", "YDEX", "MIX", "SI", "BR", "NG",
        "GLDRUB", "USDRUB", "CNY", "VTBR", "MGNT", "MTSS", "ROSN",
        "GMKN", "NVTK", "TATN", "PLZL",
    ])
    def test_canonical_idempotent(self, canonical):
        impact = make_ticker_impact(ticker=canonical)
        assert impact.ticker == canonical

    @pytest.mark.parametrize("unknown_ticker", [
        "AAPL", "TSLA", "MOEX", "GOOGL", "XYZW", "",
    ])
    def test_unknown_raises_validation_error(self, unknown_ticker):
        with pytest.raises(ValidationError) as exc_info:
            make_ticker_impact(ticker=unknown_ticker)
        assert "ticker" in str(exc_info.value).lower() or \
               "unknown ticker" in str(exc_info.value).lower()

    def test_non_string_raises(self):
        with pytest.raises(ValidationError):
            make_ticker_impact(ticker=123)
        with pytest.raises(ValidationError):
            make_ticker_impact(ticker=None)

    def test_case_sensitive_lowercase_unknown(self):
        """lowercase 'sber' НЕ нормализуется."""
        with pytest.raises(ValidationError):
            make_ticker_impact(ticker="sber")
        with pytest.raises(ValidationError):
            make_ticker_impact(ticker="gazp")

    def test_normalization_preserves_other_fields(self):
        impact = make_ticker_impact(
            ticker="Si",
            direction="short",
            confidence=0.95,
            sentiment="negative",
            impact_strength=0.8,
            rationale="ЦБ повысил ставку — Si вверх",
        )
        assert impact.ticker == "SI"
        assert impact.direction == "short"
        assert impact.confidence == 0.95
        assert impact.sentiment == "negative"
        assert impact.impact_strength == 0.8
        assert impact.rationale == "ЦБ повысил ставку — Si вверх"

    def test_frozen_after_creation(self):
        """frozen=True блокирует mutation после создания."""
        impact = make_ticker_impact(ticker="SBER")
        with pytest.raises(ValidationError):
            impact.ticker = "GAZP"  # type: ignore


# =============================================================================
# EnrichedNewsPayload
# =============================================================================
class TestPayloadWithLegacyTickers:

    def test_payload_with_legacy_si_normalizes(self):
        payload_dict = make_minimal_payload(tickers_data=[
            {
                "ticker": "Si",
                "direction": "long",
                "confidence": 0.7,
                "sentiment": "positive",
                "impact_strength": 0.5,
                "rationale": "ЦБ повысил ставку",
            }
        ])
        payload = EnrichedNewsPayload.model_validate(payload_dict)
        assert len(payload.tickers) == 1
        assert payload.tickers[0].ticker == "SI"

    def test_payload_mixed_legacy_and_canonical(self):
        payload_dict = make_minimal_payload(tickers_data=[
            {"ticker": "Si", "direction": "long", "confidence": 0.6,
             "sentiment": "positive", "impact_strength": 0.5, "rationale": "r"},
            {"ticker": "MX", "direction": "short", "confidence": 0.7,
             "sentiment": "negative", "impact_strength": 0.6, "rationale": "r"},
            {"ticker": "SBER", "direction": "long", "confidence": 0.5,
             "sentiment": "neutral", "impact_strength": 0.4, "rationale": "r"},
            {"ticker": "GOLD", "direction": "short", "confidence": 0.6,
             "sentiment": "negative", "impact_strength": 0.7, "rationale": "r"},
        ])
        payload = EnrichedNewsPayload.model_validate(payload_dict)
        assert [t.ticker for t in payload.tickers] == ["SI", "MIX", "SBER", "GLDRUB"]

    def test_payload_with_unknown_ticker_raises(self):
        payload_dict = make_minimal_payload(tickers_data=[
            {"ticker": "SBER", "direction": "long", "confidence": 0.6,
             "sentiment": "positive", "impact_strength": 0.5, "rationale": "r"},
            {"ticker": "AAPL", "direction": "long", "confidence": 0.5,
             "sentiment": "positive", "impact_strength": 0.4, "rationale": "r"},
        ])
        with pytest.raises(ValidationError):
            EnrichedNewsPayload.model_validate(payload_dict)

    def test_payload_empty_tickers_still_valid(self):
        payload_dict = make_minimal_payload(tickers_data=[])
        payload = EnrichedNewsPayload.model_validate(payload_dict)
        assert payload.tickers == []


# =============================================================================
# Full EnrichedNewsEvent end-to-end
# =============================================================================
class TestFullEnrichedNewsEvent:

    def test_full_event_with_legacy_ticker(self):
        """End-to-end: legacy ticker в payload -> canonical в финальном event."""
        # FIX: trace должен быть list (а не dict), согласно MessageEnvelope в base.py
        event_dict = {
            "event_id": "01TEST00000000000000000000",
            "schema_version": "1.1.0",
            "producer": "enricher",
            "produced_at": "2026-05-16T15:00:00Z",
            "trace": [],
            "payload": make_minimal_payload(tickers_data=[
                {"ticker": "MX", "direction": "long", "confidence": 0.8,
                 "sentiment": "positive", "impact_strength": 0.6,
                 "rationale": "позитив для индекса"},
            ]),
        }
        event = EnrichedNewsEvent.model_validate(event_dict)
        assert event.payload.tickers[0].ticker == "MIX"
        assert event.schema_version == "1.1.0"
        assert event.producer == "enricher"
