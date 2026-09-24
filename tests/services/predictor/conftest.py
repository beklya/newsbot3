"""Shared fixtures for Predictor tests."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio
import fakeredis.aioredis

from src.contracts.enriched_news import (
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)
from src.services.predictor.candle_cache import CandleCache
from src.services.predictor.config import PredictorSettings
from src.services.predictor.model_loader import ModelBundle
from src.services.predictor.news_history import NewsHistory


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


@pytest.fixture
def predictor_settings(tmp_path: Path) -> PredictorSettings:
    """Settings pointing to tmp dirs — bypass models_dir/prices_dir existence checks."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    prices_dir = tmp_path / "prices"
    prices_dir.mkdir()
    return PredictorSettings(
        models_dir=models_dir,
        prices_dir=prices_dir,
    )


@pytest.fixture
def enriched_event_factory():
    """Build a valid EnrichedNewsEvent for tests."""

    def _factory(
        *,
        ticker: str = "GAZP",
        direction: str = "long",
        sentiment: str = "positive",
        confidence: float = 0.7,
        is_financial: bool = True,
        category: str = "corporate",
        urgency: str = "medium",
        tickers: list[TickerImpact] | None = None,
        produced_at: str | None = None,
    ) -> EnrichedNewsEvent:
        if tickers is None:
            tickers = [TickerImpact(
                ticker=ticker, direction=direction, sentiment=sentiment,
                confidence=confidence, impact_strength=0.5, rationale="test",
            )]
        payload = EnrichedNewsPayload(
            raw_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
            llm_provider="groq",
            llm_model="llama-3.3-70b-versatile",
            llm_latency_ms=500.0,
            llm_input_tokens=100,
            llm_output_tokens=50,
            prompt_version="1.0.0",
            is_financial=is_financial,
            tickers=tickers,
            summary="Test summary with 5% growth and 'quote'",
            expected_timeframe="medium",
            urgency=urgency,
            category=category,
            is_actionable=True,
            llm_raw_response="{}",
        )
        kwargs = {"payload": payload}
        if produced_at:
            kwargs["produced_at"] = produced_at
        return EnrichedNewsEvent(**kwargs)

    return _factory


def _build_synthetic_candles(start: pd.Timestamp, n_bars: int = 600) -> pd.DataFrame:
    """Generate a trivial bar series: linearly rising price, constant volume.

    Used by tests that need a CandleCache without filesystem CSVs.
    """
    idx = pd.date_range(start, periods=n_bars, freq="1min")
    closes = np.linspace(100.0, 105.0, n_bars)
    df = pd.DataFrame({
        "open": closes,
        "high": closes + 0.5,
        "low": closes - 0.5,
        "close": closes,
        "volume": np.full(n_bars, 1000.0),
    }, index=idx)
    df.index.name = "ts"
    return df


@pytest.fixture
def candle_cache_synthetic() -> CandleCache:
    """CandleCache with synthetic bars anchored to wall-clock NOW.

    The cache window centers on the current minute (±5h on either side) so
    that test events with default produced_at=utcnow fall within bars —
    Sprint-6 stale-news gate passes naturally. Anchoring to wall-clock keeps
    fixtures honest about production behavior instead of disabling gates.

    Tests that exercise the gate explicitly (test_stale_news_gate.py) pass
    out-of-range produced_at to trigger the gate.
    """
    cache = CandleCache(Path("nowhere"))
    # Cache index = naive MSK (Phase 2 convention) = UTC + 3h.
    now_utc = datetime.now(timezone.utc).replace(microsecond=0, second=0, tzinfo=None)
    end_msk = pd.Timestamp(now_utc) + pd.Timedelta(hours=3) + pd.Timedelta(hours=5)
    start = (end_msk - pd.Timedelta(hours=10)).floor("min")
    for ticker in ("GAZP", "BR", "USDRUB", "MIX", "GLDRUB"):
        cache._candles[ticker] = _build_synthetic_candles(start, 600)
    return cache


@pytest.fixture
def empty_history() -> NewsHistory:
    return NewsHistory(lookback_hours=24, per_ticker_maxlen=50)


@pytest.fixture
def real_bundle() -> ModelBundle:
    """Load the actual Sprint 5.1 trained bundle from disk.

    Tests using this fixture will fail loudly if training was not run.
    """
    from src.services.predictor.model_loader import load_bundle
    project_root = Path(__file__).resolve().parents[3]
    return load_bundle(project_root / "data" / "models" / "predictor" / "v1")
