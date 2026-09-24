"""Tests for direction filter (Sprint 4 B-filter port)."""
from __future__ import annotations

from src.contracts.enriched_news import TickerImpact
from src.services.decision.filter import (
    SIDE_TO_DIRECTION, DIRECTION_TO_SIDE,
    apply_direction_filter, get_ticker_impact,
)


def test_side_direction_mapping():
    assert SIDE_TO_DIRECTION["BUY"] == "long"
    assert SIDE_TO_DIRECTION["SELL"] == "short"
    assert DIRECTION_TO_SIDE["long"] == "BUY"
    assert DIRECTION_TO_SIDE["short"] == "SELL"


def test_get_ticker_impact_found(enriched_event_factory):
    ev = enriched_event_factory(ticker="GAZP", direction="long", confidence=0.7)
    ti = get_ticker_impact(ev, "GAZP")
    assert ti is not None and ti.direction == "long"


def test_get_ticker_impact_missing(enriched_event_factory):
    ev = enriched_event_factory(ticker="GAZP")
    assert get_ticker_impact(ev, "LKOH") is None


def test_filter_match_long(enriched_event_factory):
    ev = enriched_event_factory(direction="long", confidence=0.7)
    d = apply_direction_filter(ev, "GAZP", side="BUY", min_confidence=0.5)
    assert d.include is True


def test_filter_match_short(enriched_event_factory):
    ev = enriched_event_factory(direction="short", confidence=0.7)
    d = apply_direction_filter(ev, "GAZP", side="SELL", min_confidence=0.5)
    assert d.include is True


def test_filter_direction_mismatch(enriched_event_factory):
    ev = enriched_event_factory(direction="long", confidence=0.7)
    d = apply_direction_filter(ev, "GAZP", side="SELL", min_confidence=0.5)
    assert d.include is False
    assert "direction" in d.reject_reason


def test_filter_confidence_below_threshold(enriched_event_factory):
    ev = enriched_event_factory(direction="long", confidence=0.4)
    d = apply_direction_filter(ev, "GAZP", side="BUY", min_confidence=0.5)
    assert d.include is False
    assert "confidence" in d.reject_reason


def test_filter_neutral_direction_is_lenient(enriched_event_factory):
    """Sprint 4 design (restored 2026-06-06): neutral != negative endorsement
    → INCLUDE.  Walk-forward Sharpe 6.42 was reproduced ONLY with this lenient
    semantics; the previous STRICT-reject of neutral gave Sharpe 0 / 0 trades.
    """
    ev = enriched_event_factory(direction="neutral", confidence=0.9)
    d = apply_direction_filter(ev, "GAZP", side="BUY", min_confidence=0.5)
    assert d.include is True


def test_filter_ticker_not_mentioned_is_lenient(enriched_event_factory):
    """Sprint 4 design (restored 2026-06-06): absent signal != veto → INCLUDE.
    LLM may have legitimately not mentioned this ticker but XGBoost decided
    to trade based on price/technical features.  Walk-forward 6.42 only
    reproduces with this lenient behavior."""
    ev = enriched_event_factory(ticker="GAZP")
    d = apply_direction_filter(ev, "LKOH", side="BUY", min_confidence=0.5)
    assert d.include is True
