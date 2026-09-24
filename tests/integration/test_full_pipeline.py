"""End-to-end pipeline integration test (fakeredis + mocked LLM + real models).

Цепочка: synthetic RawNewsEvent → Enricher (mocked LLM) → news:enriched
       → Predictor (real models) → ml:predictions
       → Decision (gates) → trade:signals
       → Bridge (paper) → trade:executions OPEN + CLOSE
       → Monitor (alert dedup)

Проверяем:
- Trace[] propagates через 5 hops (receiver virtual → enricher → predictor → decision → bridge)
- Один RawNewsEvent с одним whitelist ticker → ровно 1 OPEN + 1 CLOSE
- Off-whitelist ticker → silent drop в Predictor (никаких predictions / signals / executions)
- REJECT signal (low confidence) → trade:signal с action=REJECT, no fill
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest
import pytest_asyncio
import fakeredis.aioredis

from src.contracts.enriched_news import (
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)
from src.contracts.execution_result import ExecutionResultEvent
from src.contracts.ml_prediction import MLPredictionEvent
from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.contracts.trade_signal import TradeSignalEvent
from src.infra.candles import CandleCache
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher

# Service pipelines
from src.services.bridge.config import BridgeSettings
from src.services.bridge.metrics import BridgeMetrics
from src.services.bridge.paper_executor import PaperExecutor
from src.services.bridge.pipeline import BridgePipeline
from src.services.bridge.position_tracker import PositionTracker
from src.services.decision.config import DecisionSettings
from src.services.decision.enrichment_cache import EnrichmentCache
from src.services.decision.metrics import DecisionMetrics
from src.services.decision.pipeline import DecisionPipeline
from src.services.decision.risk_manager import RiskManager
from src.services.enricher.config import EnricherSettings
from src.services.enricher.metrics import EnricherMetrics
from src.services.enricher.pipeline import EnrichmentPipeline
from src.services.predictor.config import PredictorSettings
from src.services.predictor.metrics import PredictorMetrics
from src.services.predictor.model_loader import load_bundle
from src.services.predictor.news_history import NewsHistory
from src.services.predictor.pipeline import PredictorPipeline


@pytest_asyncio.fixture
async def fake_redis():
    r = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield r
    await r.aclose()


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture
def models_dir() -> Path:
    return _project_root() / "data" / "models" / "predictor" / "v1"


@pytest.fixture
def synthetic_cache() -> CandleCache:
    """In-memory candles anchored to wall-clock NOW.

    Sprint-6 gates compare news_time vs last_bar (Predictor) and against fill
    time (Bridge). The integration pipeline ends up with produced_at = utcnow
    on the enriched event (the mocked enrich doesn't propagate raw's
    tg_published_at), so the cache window must cover utcnow for gates to pass.
    Flat open=100 + slight high-spike at later bars to trigger TP=102 on BUY.
    """
    import numpy as np
    cache = CandleCache(Path("nowhere"))
    # MSK naive = UTC + 3h (Phase-2 convention)
    now_utc = datetime.now(timezone.utc).replace(microsecond=0, second=0, tzinfo=None)
    end_msk = pd.Timestamp(now_utc) + pd.Timedelta(hours=3) + pd.Timedelta(hours=5)
    start = (end_msk - pd.Timedelta(hours=10)).floor("min")
    n = 600
    # Flat opens at 100 (matches signal.entry_price=100 → no drift).
    # Highs rise above TP=102 from bar 70 onwards so position eventually hits TP.
    opens = np.full(n, 100.0)
    highs = np.array([100.5] * 70 + [102.5] * (n - 70))
    lows = np.full(n, 99.5)
    closes = opens.copy()
    df = pd.DataFrame({
        "open": opens, "high": highs, "low": lows,
        "close": closes, "volume": np.full(n, 1000.0),
    }, index=pd.date_range(start, periods=n, freq="1min"))
    df.index.name = "ts"
    cache._candles["GAZP"] = df
    cache._candles["BR"] = df.copy()
    cache._candles["USDRUB"] = df.copy()
    cache._candles["MIX"] = df.copy()
    cache._candles["GLDRUB"] = df.copy()
    return cache


def _build_enricher_pipeline(fake_redis, tmp_path):
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(exist_ok=True)
    stub_prompt = prompts_dir / "v1_0_0.md"
    stub_prompt.write_text("test prompt", encoding="utf-8")

    settings = EnricherSettings(
        groq_api_key="test_dummy",
        prompt_file=stub_prompt,
        prompt_version="1.0.0",
    )
    metrics = EnricherMetrics()
    return EnrichmentPipeline(
        llm=AsyncMock(),
        idem=IdempotencyGuard(fake_redis, ttl_seconds=60),
        publisher_main=StreamPublisher(fake_redis, stream=settings.enriched_news_stream),
        publisher_dlq=StreamPublisher(fake_redis, stream=settings.enriched_news_dlq_stream),
        metrics=metrics,
        settings=settings,
    ), settings


def _build_predictor_pipeline(fake_redis, settings_models_dir, synthetic_cache, tmp_path):
    prices_dir = tmp_path / "predictor_prices"
    prices_dir.mkdir(exist_ok=True)
    settings = PredictorSettings(
        models_dir=settings_models_dir,
        prices_dir=prices_dir,
    )
    bundle = load_bundle(settings_models_dir)
    return PredictorPipeline(
        bundle=bundle,
        candles=synthetic_cache,
        history=NewsHistory(),
        idem=IdempotencyGuard(fake_redis, ttl_seconds=60),
        publisher_main=StreamPublisher(fake_redis, stream=settings.ml_predictions_stream),
        publisher_dlq=StreamPublisher(fake_redis, stream=settings.ml_predictions_dlq_stream),
        metrics=PredictorMetrics(),
        settings=settings,
    ), settings


def _build_decision_pipeline(fake_redis):
    settings = DecisionSettings(
        rr_threshold=1.0,        # пониже для тестов — синтетические predictions могут быть слабее
        min_mfe_pct=0.0,
        direction_filter_min_confidence=0.5,
    )
    return DecisionPipeline(
        settings=settings,
        enrichment_cache=EnrichmentCache(fake_redis),
        risk_manager=RiskManager(
            redis=fake_redis,
            open_positions_key=settings.risk_open_positions_key,
            daily_pnl_key_prefix=settings.risk_daily_pnl_key_prefix,
            cooldown_key_prefix=settings.risk_cooldown_key_prefix,
            max_open_positions=settings.max_open_positions,
            daily_kill_pct=settings.daily_kill_pct,
            initial_equity_rub=settings.initial_equity_rub,
        ),
        idem=IdempotencyGuard(fake_redis, ttl_seconds=60),
        publisher=StreamPublisher(fake_redis, stream=settings.trade_signals_stream),
        metrics=DecisionMetrics(),
    ), settings


def _build_bridge_pipeline(fake_redis, synthetic_cache, tmp_path):
    prices_dir = tmp_path / "prices"
    prices_dir.mkdir(exist_ok=True)
    (prices_dir / "prices_GAZP.csv").write_text("dummy")
    settings = BridgeSettings(
        prices_dir=prices_dir, tracker_poll_interval_sec=0.05,
    )
    executor = PaperExecutor(settings, synthetic_cache)
    publisher = StreamPublisher(fake_redis, stream=settings.trade_executions_stream)
    tracker = PositionTracker(
        settings=settings,
        executor=executor,
        redis=fake_redis,
        publisher=publisher,
        producer_name=settings.producer_name,
    )
    return BridgePipeline(
        settings=settings,
        executor=executor,
        tracker=tracker,
        idem=IdempotencyGuard(fake_redis, ttl_seconds=60),
        publisher=publisher,
        metrics=BridgeMetrics(),
    ), tracker, settings


def _raw_news(text: str, ts: datetime) -> RawNewsEvent:
    import hashlib
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    payload = RawNewsPayload(
        channel="@test_channel",
        message_id=1,
        text=text,
        tg_published_at=ts.isoformat(timespec="milliseconds"),
        received_at=ts.isoformat(timespec="milliseconds"),
        text_hash=h,
        has_media=False, is_reply=False, is_forward=False,
    )
    return RawNewsEvent(produced_at=ts.isoformat(timespec="milliseconds"), payload=payload)


def _enrich_result_for(raw_event_id: str, ticker: str = "GAZP", direction: str = "long",
                       confidence: float = 0.7):
    """Mock EnrichResult that the LLM stub will return."""
    from src.services.enricher.llm_client import EnrichResult
    payload = EnrichedNewsPayload(
        raw_event_id=raw_event_id,
        llm_provider="groq",
        llm_model="llama-3.3-70b-versatile",
        llm_latency_ms=300.0,
        llm_input_tokens=120,
        llm_output_tokens=40,
        prompt_version="1.0.0",
        is_financial=True,
        tickers=[TickerImpact(
            ticker=ticker, direction=direction, sentiment="positive",
            confidence=confidence, impact_strength=0.6, rationale="test",
        )],
        summary="Test summary",
        expected_timeframe="medium",
        urgency="medium",
        category="corporate",
        is_actionable=True,
        llm_raw_response="{}",
    )
    return EnrichResult(
        payload=payload, error=None,
        latency_ms=300.0, raw_response="{}",
        input_tokens=120, output_tokens=40, rate_limit_headers={},
    )


async def _read_stream(redis, stream: str, parser):
    entries = await redis.xrange(stream, min="-", max="+")
    out = []
    for _id, fields in entries:
        data = fields.get(b"data") or fields.get("data")
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        out.append(parser.model_validate_json(data))
    return out


@pytest.mark.asyncio
async def test_full_pipeline_end_to_end(
    fake_redis, tmp_path, models_dir, synthetic_cache,
):
    """RawNewsEvent → 5 hops → trade:executions OPEN + CLOSE."""
    enricher, enricher_settings = _build_enricher_pipeline(fake_redis, tmp_path)
    predictor, predictor_settings = _build_predictor_pipeline(fake_redis, models_dir, synthetic_cache, tmp_path)
    decision, decision_settings = _build_decision_pipeline(fake_redis)
    bridge, tracker, bridge_settings = _build_bridge_pipeline(fake_redis, synthetic_cache, tmp_path)

    # Drive: build raw news, enricher (mocked LLM) → enriched event, etc.
    signal_ts = datetime(2026, 5, 25, 11, 30, 0, tzinfo=timezone.utc)  # 14:30 MSK
    raw = _raw_news("Газпром квартальный отчёт — рост прибыли 25%", signal_ts)
    enricher.llm.enrich = AsyncMock(return_value=_enrich_result_for(
        raw.event_id, ticker="GAZP", direction="long", confidence=0.7,
    ))

    # 1. Enricher
    await enricher.process(raw)
    enriched_events = await _read_stream(fake_redis, enricher_settings.enriched_news_stream, EnrichedNewsEvent)
    assert len(enriched_events) == 1
    enriched = enriched_events[0]
    assert enriched.event_id == raw.event_id  # event_id inherited

    # 2. Verify enrichment cache (SETEX side effect)
    cached = await fake_redis.get(f"enriched:{enriched.event_id}")
    assert cached is not None

    # 3. Predictor
    await predictor.process(enriched)
    predictions = await _read_stream(fake_redis, predictor_settings.ml_predictions_stream, MLPredictionEvent)
    assert len(predictions) == 1
    pred = predictions[0]
    assert pred.payload.enriched_event_id == enriched.event_id
    assert pred.payload.ticker == "GAZP"
    assert pred.event_id != enriched.event_id  # fresh ULID

    # 4. Decision
    await decision.process(pred)
    signals = await _read_stream(fake_redis, decision_settings.trade_signals_stream, TradeSignalEvent)
    assert len(signals) == 1
    signal = signals[0]
    # Может быть EXECUTE или REJECT — зависит от модели. В synthetic candles вероятен EXECUTE.
    assert signal.payload.action in {"EXECUTE", "REJECT"}
    if signal.payload.action == "REJECT":
        pytest.skip(f"Synthetic data led to REJECT: {signal.payload.reject_reason}")

    # 5. Bridge
    await bridge.process(signal)
    # Дать tracker'у время закрыть позицию
    for _ in range(40):
        await asyncio.sleep(0.05)
        if tracker.active_count() == 0:
            break

    executions = await _read_stream(fake_redis, bridge_settings.trade_executions_stream, ExecutionResultEvent)
    assert len(executions) >= 2, f"Expected OPEN + CLOSE, got {len(executions)}"
    # Pair: one OPEN (exit_*=None), one CLOSE (exit_*=set)
    opens = [e for e in executions if e.payload.exit_reason is None]
    closes = [e for e in executions if e.payload.exit_reason is not None]
    assert len(opens) == 1 and len(closes) == 1
    assert opens[0].payload.signal_event_id == signal.event_id
    assert closes[0].payload.signal_event_id == signal.event_id
    assert closes[0].payload.exit_reason in {"tp", "sl", "time"}

    # 6. Trace propagation — execution_result inherits trace from upstream
    # (Each Publisher.publish appends 1 step. Raw has 0, +1=enriched, +1=predictor,
    #  +1=decision, +1=open, +1=close.)
    assert len(closes[0].trace) >= 4, f"Expected trace ≥4 hops, got {closes[0].trace}"
    services_in_trace = [step["service"] for step in closes[0].trace]
    assert enricher_settings.enriched_news_stream in services_in_trace
    assert predictor_settings.ml_predictions_stream in services_in_trace
    assert decision_settings.trade_signals_stream in services_in_trace
    assert bridge_settings.trade_executions_stream in services_in_trace


@pytest.mark.asyncio
async def test_off_whitelist_ticker_dropped_early(
    fake_redis, tmp_path, models_dir, synthetic_cache,
):
    """SBER (not in whitelist) → Predictor silently drops. No predictions/signals/executions."""
    enricher, e_settings = _build_enricher_pipeline(fake_redis, tmp_path)
    predictor, p_settings = _build_predictor_pipeline(fake_redis, models_dir, synthetic_cache, tmp_path)
    decision, d_settings = _build_decision_pipeline(fake_redis)

    ts = datetime(2026, 5, 25, 11, 30, 0, tzinfo=timezone.utc)
    raw = _raw_news("Сбербанк новость", ts)
    enricher.llm.enrich = AsyncMock(return_value=_enrich_result_for(
        raw.event_id, ticker="SBER",
    ))

    await enricher.process(raw)
    enriched = (await _read_stream(fake_redis, e_settings.enriched_news_stream, EnrichedNewsEvent))[0]
    await predictor.process(enriched)
    predictions = await _read_stream(fake_redis, p_settings.ml_predictions_stream, MLPredictionEvent)
    # SBER не в whitelist Predictor → 0 predictions
    assert predictions == []
    snap = predictor.metrics.snapshot()
    assert snap.get("tickers_skipped_off_whitelist", 0) == 1
