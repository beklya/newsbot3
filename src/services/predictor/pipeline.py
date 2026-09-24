"""PredictorPipeline — orchestrates Consumer → Idempotency → Features → Inference → Publish.

For each news:enriched event:
  1. Append to NewsHistory unconditionally (feature input downstream).
  2. Iterate ticker_impacts:
     - Whitelist filter (off-whitelist silently skipped + counter)
     - Idempotency claim with composite key f"{enriched_event_id}:{ticker}"
       (если другой Predictor instance уже обработал — skip)
     - Build 67-feature vector + features_hash (SHA-256 для drift detection)
     - Inference: 8 predictions per ticker (2 horizons × 4 targets)
     - Publish MLPredictionEvent (fresh ULID, payload.enriched_event_id = backref)

  3. Если 0 whitelist tickers → silent skip (no DLQ, no publish).

Error policy:
  - Missing candle для whitelist ticker → DLQ как "missing_market_data" (non-retryable).
  - Inference exception → retryable raise (no-ack, PEL replay).
  - Idempotency claim failure → already processed (no error).
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

import numpy as np
import pandas as pd

from src.contracts.base import MessageEnvelope, utcnow_iso
from src.contracts.enriched_news import EnrichedNewsEvent, TickerImpact
from src.contracts.ml_prediction import (
    MLPredictionEvent,
    MLPredictionPayload,
)
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher

from .candle_cache import CandleCache
from .config import PredictorSettings
from .feature_builder import build_features, vectorize
from .inference import predict_all_horizons
from .metrics import PredictorMetrics
from .model_loader import ModelBundle
from .news_history import NewsHistory

log = logging.getLogger(__name__)


class PredictionRetryable(Exception):
    """Raise to keep message in PEL for retry (inference errors etc)."""


class PredictorPipeline:
    def __init__(
        self,
        *,
        bundle: ModelBundle,
        candles: CandleCache,
        history: NewsHistory,
        idem: IdempotencyGuard,
        publisher_main: StreamPublisher,
        publisher_dlq: StreamPublisher,
        metrics: PredictorMetrics,
        settings: PredictorSettings,
    ):
        self.bundle = bundle
        self.candles = candles
        self.history = history
        self.idem = idem
        self.publisher_main = publisher_main
        self.publisher_dlq = publisher_dlq
        self.metrics = metrics
        self.settings = settings
        self._whitelist = frozenset(settings.whitelist_tickers)

    async def process(self, event: MessageEnvelope) -> None:
        """Entry point called by StreamConsumer for each EnrichedNewsEvent."""
        if not isinstance(event, EnrichedNewsEvent):
            log.error("pipeline_wrong_type %s", type(event).__name__)
            self.metrics.inc("errors.wrong_type")
            return

        self.metrics.inc("events_in")

        # 1. Always update NewsHistory — даже если 0 whitelist tickers,
        # event может быть нужен для future news_history features другого ticker'а.
        self.history.append(event)

        # 2. is_financial=False → no predictions (gate-keeping)
        if not event.payload.is_financial:
            self.metrics.inc("events_skipped_non_financial")
            return

        if not event.payload.tickers:
            self.metrics.inc("events_skipped_no_tickers")
            return

        # 3. Per-ticker iteration
        for t in event.payload.tickers:
            await self._process_one_ticker(event, t)

    async def _process_one_ticker(
        self, event: EnrichedNewsEvent, t: TickerImpact,
    ) -> None:
        if t.ticker not in self._whitelist:
            self.metrics.inc("tickers_skipped_off_whitelist")
            return

        # Composite idempotency key — per (news, ticker), не per news
        idem_key = f"{event.event_id}:{t.ticker}"
        claimed = await self.idem.claim(
            scope=self.settings.idempotency_scope, key=idem_key,
        )
        if not claimed:
            self.metrics.inc("tickers_skipped_idem")
            return

        # Missing market data → DLQ non-retryable
        if not self.candles.has(t.ticker):
            self.metrics.inc("errors.missing_market_data")
            await self._publish_dlq(event, t, "missing_market_data",
                                     f"No CandleCache entry for {t.ticker}")
            return

        # Sprint 6: stale-news gate.
        # If news_time falls into a gap in our cache (e.g., between historical
        # CSV ending 2026-04-20 and live bars starting today 12:31), Predictor's
        # last_close will be the OLD bar before the gap, while Bridge will fill
        # on a NEW bar after the gap. Big divergence ⇒ broken SL/TP.
        # We measure gap = news_time - (last bar at-or-before news_time). If
        # too large, skip prediction; no MLPredictionEvent is published.
        candles_df_check = self.candles.get(t.ticker)
        if candles_df_check is not None and len(candles_df_check) > 0:
            ref_ts_str_check = event.payload.tg_published_at or event.produced_at
            try:
                ref_utc_check = pd.to_datetime(ref_ts_str_check)
                if ref_utc_check.tzinfo is not None:
                    ref_msk_check = (
                        ref_utc_check.tz_convert("UTC") + pd.Timedelta(hours=3)
                    ).tz_localize(None)
                else:
                    ref_msk_check = ref_utc_check
                pos_check = candles_df_check.index.searchsorted(ref_msk_check, side="right") - 1
                if pos_check >= 0:
                    last_bar_msk_check = candles_df_check.index[pos_check]
                    gap_sec = (ref_msk_check - last_bar_msk_check).total_seconds()
                    if gap_sec > self.settings.max_news_to_last_bar_gap_sec:
                        log.warning(
                            "stale_candles_at_news_time ticker=%s event_id=%s news_msk=%s "
                            "last_bar=%s gap=%.0fs threshold=%ds — skip prediction",
                            t.ticker, event.event_id, ref_msk_check, last_bar_msk_check,
                            gap_sec, self.settings.max_news_to_last_bar_gap_sec,
                        )
                        self.metrics.inc("errors.stale_candles_at_news_time")
                        return
            except (ValueError, TypeError):
                # Defensive: if news_time can't be parsed, fall through to normal flow.
                pass

        # Build features + inference
        t_start = time.perf_counter()
        try:
            feature_dict = build_features(event, t, self.candles, self.history)
            vec = vectorize(feature_dict, self.bundle.feature_order)
            predictions = predict_all_horizons(self.bundle, vec, t.ticker)
        except Exception as e:
            log.exception("inference_error ticker=%s event_id=%s", t.ticker, event.event_id)
            self.metrics.inc("errors.inference")
            raise PredictionRetryable(str(e)) from e
        elapsed_ms = (time.perf_counter() - t_start) * 1000

        # last_close + last_bar_time — для MLPredictionPayload.
        # Sprint 6: for honest historical replay, use bar AT news_time (not
        # latest bar in cache). Decision uses last_close as reference price for
        # SL/TP computation — must match the time when news arrived, otherwise
        # SL/TP are computed on "now" prices but Bridge fills at historical
        # prices => all trades exit by time-stop (never hit TP/SL).
        candles_df = self.candles.get(t.ticker)
        if candles_df is None or len(candles_df) == 0:
            # Shouldn't happen — has() прошёл выше. Defensive.
            last_close = 0.0
            last_bar_time = utcnow_iso()
        else:
            ref_ts_str = event.payload.tg_published_at or event.produced_at
            ref_utc = pd.to_datetime(ref_ts_str)
            if ref_utc.tzinfo is not None:
                ref_msk = (ref_utc.tz_convert("UTC") + pd.Timedelta(hours=3)).tz_localize(None)
            else:
                ref_msk = ref_utc
            # Index search: position where bar.index <= ref_msk
            pos = candles_df.index.searchsorted(ref_msk, side="right") - 1
            if pos < 0:
                # news_time предшествует всем bars — defensive fallback на первый
                pos = 0
            elif pos >= len(candles_df):
                pos = len(candles_df) - 1
            last_close = float(candles_df["close"].iloc[pos])
            last_bar_time = candles_df.index[pos].isoformat()

        # features_hash for drift detection
        features_hash = hashlib.sha256(vec.tobytes()).hexdigest()

        payload = MLPredictionPayload(
            enriched_event_id=event.event_id,
            ticker=t.ticker,
            features_built_at=utcnow_iso(),
            features_hash=features_hash,
            feature_count=len(vec),
            # Sprint 6: propagate news_time для honest historical replay в Bridge.
            # None если enricher не выставил (старые events) — Bridge fallback'нется.
            news_time=event.payload.tg_published_at,
            last_bar_time=last_bar_time,
            last_close=last_close,
            predictions=predictions,
            inference_latency_ms=float(elapsed_ms),
            model_version=f"{self.settings.model_version}:{self.bundle.fingerprint}",
        )

        # Fresh ULID event_id (per plan 5.1 dezigne decision). Trace
        # сохраняем от upstream (enricher), Publisher добавит свой шаг.
        prediction_event = MLPredictionEvent(
            producer=self.settings.producer_name,
            trace=event.trace,
            payload=payload,
        )
        await self.publisher_main.publish(prediction_event)
        self.metrics.inc("predictions_out")
        self.metrics.record_latency_ms(elapsed_ms)

    async def _publish_dlq(
        self,
        event: EnrichedNewsEvent,
        ticker_impact: TickerImpact,
        error_kind: str,
        message: str,
    ) -> None:
        """DLQ — flat dict, not MessageEnvelope (matches enricher pattern)."""
        fields: dict[str, Any] = {
            "enriched_event_id": event.event_id,
            "ticker": ticker_impact.ticker,
            "error_kind": error_kind,
            "error_message": message[:1_000],
            "occurred_at": utcnow_iso(),
        }
        str_fields = {k: str(v) for k, v in fields.items()}
        await self.publisher_dlq.redis.xadd(
            self.publisher_dlq.stream,
            fields=str_fields,
            maxlen=self.settings.ml_predictions_dlq_maxlen,
            approximate=True,
        )
        self.metrics.inc("dlq_total")
