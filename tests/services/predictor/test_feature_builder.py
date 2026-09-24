"""Tests for feature_builder.build_features and vectorize.

Verify:
- 67-element vector matches feature_order exactly
- Schema mapping (positive→bullish, etc.) works
- Missing candle → fallback defaults (not NaN)
- News history counts honors 24h window
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.contracts.enriched_news import TickerImpact

from src.services.predictor.feature_builder import (
    ALL_CATEGORIES,
    ALL_SENTIMENTS,
    ALL_URGENCIES,
    build_features,
    vectorize,
)


def test_feature_dict_has_78_keys(
    enriched_event_factory, candle_cache_synthetic, empty_history,
):
    """All 78 features must be present after build_features.

    Sprint 6.1 Y4 added 11 70B-only features in section A1:
      is_actionable_int, is_financial_int (event-level bools),
      tf_{instant,short,medium,slow} (expected_timeframe one-hot — wait,
        feature_builder uses 4 tf flags but emits {instant,fast,medium,slow}
        — net +4),
      impact_strength (per-ticker scalar),
      dir_{long,short,neutral} (3 one-hot),
      sell_the_news.
    Net: 67 (Phase 2 baseline) + 11 = 78.
    """
    event = enriched_event_factory(ticker="GAZP")
    f = build_features(event, event.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    assert len(f) == 78, f"got {len(f)} features, expected 78. Keys: {sorted(f.keys())}"


def test_sentiment_mapping(enriched_event_factory, candle_cache_synthetic, empty_history):
    """positive→bullish, negative→bearish, neutral→neutral."""
    cases = [("positive", "bullish"), ("negative", "bearish"), ("neutral", "neutral")]
    for new_sent, legacy_sent in cases:
        event = enriched_event_factory(ticker="GAZP", sentiment=new_sent)
        f = build_features(event, event.payload.tickers[0],
                           candle_cache_synthetic, empty_history)
        for s in ALL_SENTIMENTS:
            expected = 1.0 if s == legacy_sent else 0.0
            assert f[f"sent_{s}"] == expected, f"sent_{s} for new_sent={new_sent}"


def test_category_one_hot(enriched_event_factory, candle_cache_synthetic, empty_history):
    """Category from new schema matches Phase 2 one-hots where applicable."""
    event = enriched_event_factory(category="corporate")
    f = build_features(event, event.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    assert f["cat_corporate"] == 1.0
    for c in ALL_CATEGORIES:
        if c == "corporate":
            continue
        assert f[f"cat_{c}"] == 0.0


def test_category_market_zeros_all(enriched_event_factory, candle_cache_synthetic, empty_history):
    """category='market' (new schema) doesn't match any Phase 2 cat → all zeros."""
    event = enriched_event_factory(category="market")
    f = build_features(event, event.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    for c in ALL_CATEGORIES:
        assert f[f"cat_{c}"] == 0.0


def test_urgency_one_hot(enriched_event_factory, candle_cache_synthetic, empty_history):
    for u in ALL_URGENCIES:
        event = enriched_event_factory(urgency=u)
        f = build_features(event, event.payload.tickers[0],
                           candle_cache_synthetic, empty_history)
        for u2 in ALL_URGENCIES:
            expected = 1.0 if u2 == u else 0.0
            assert f[f"urg_{u2}"] == expected


def test_confidence_passthrough(enriched_event_factory, candle_cache_synthetic, empty_history):
    """TickerImpact.confidence (0..1) → feature 'confidence' (0..1, no division)."""
    event = enriched_event_factory(confidence=0.72)
    f = build_features(event, event.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    assert f["confidence"] == pytest.approx(0.72)


def test_no_candles_fallback(enriched_event_factory, empty_history):
    """Ticker absent from CandleCache → neutral defaults, no NaN."""
    from src.services.predictor.candle_cache import CandleCache
    empty_cache = CandleCache(Path("nowhere"))
    event = enriched_event_factory(ticker="GAZP")
    f = build_features(event, event.payload.tickers[0], empty_cache, empty_history)
    # Verify no NaN
    for k, v in f.items():
        assert v == v, f"NaN in feature {k}"
    # Specific neutral defaults
    assert f["rsi_14"] == 50.0
    assert f["bb_position"] == 0.5
    assert f["vol_5m_intensity"] == 1.0
    assert f["atr_ratio_15_240"] == 1.0


def test_news_history_24h(
    enriched_event_factory, candle_cache_synthetic, empty_history,
):
    """News history counts previous events for the same ticker within 24h."""
    # Seed history with 3 events for GAZP
    base_ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    for delta_min in (10, 60, 180):
        prev = enriched_event_factory(
            ticker="GAZP",
            sentiment="positive",
            produced_at=(base_ts - timedelta(minutes=delta_min)).isoformat(timespec="milliseconds"),
        )
        empty_history.append(prev)

    current = enriched_event_factory(ticker="GAZP", produced_at=base_ts.isoformat(timespec="milliseconds"))
    f = build_features(current, current.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    assert f["news_count_24h"] == 3
    assert f["time_since_last_min"] == pytest.approx(10.0, abs=0.5)
    assert f["cum_sentiment_24h"] > 0  # All bullish positive sentiment


def test_text_features_from_summary(enriched_event_factory, candle_cache_synthetic, empty_history):
    """text_length, n_numbers, n_percent computed from summary."""
    event = enriched_event_factory()
    # Summary fixed in factory: "Test summary with 5% growth and 'quote'"
    f = build_features(event, event.payload.tickers[0],
                       candle_cache_synthetic, empty_history)
    summary = event.payload.summary
    assert f["text_length"] == len(summary)
    assert f["n_percent"] == 1.0  # one "%" character
    assert f["n_numbers"] >= 1  # "5" is a digit


def test_vectorize_orders_correctly():
    """vectorize fills np.ndarray in the feature_order sequence."""
    feature_order = ["a", "b", "c", "d"]
    feature_dict = {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0}
    vec = vectorize(feature_dict, feature_order)
    assert vec.tolist() == [1.0, 2.0, 3.0, 4.0]


def test_vectorize_missing_fills_zero():
    """Missing feature in dict → 0.0 in vector + warning."""
    feature_order = ["a", "b", "c"]
    feature_dict = {"a": 1.0, "c": 3.0}  # 'b' missing
    vec = vectorize(feature_dict, feature_order)
    assert vec.tolist() == [1.0, 0.0, 3.0]
