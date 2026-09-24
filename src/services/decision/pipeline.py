"""DecisionPipeline — ml:predictions → trade:signals.

Flow:
  1. Consume MLPredictionEvent
  2. Idempotency claim per prediction.event_id
  3. EnrichmentCache lookup → если cache miss → skip+counter (race condition)
  4. R:R logic (Phase 2) → side or reject
  5. DirectionFilter (Sprint 4 B-filter) → include or reject
  6. RiskManager gates: max_open, cooldown, daily_kill → reject if triggered
  7. Compute SL/TP levels + sizing
  8. Publish TradeSignalEvent
     - action=EXECUTE если все gates прошли
     - action=REJECT с reject_reason на любую отбраковку для post-mortem

Все REJECT events публикуются — это критично для analytics.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from src.contracts.base import MessageEnvelope
from src.contracts.instruments import try_normalize_ticker
from src.contracts.ml_prediction import MLPredictionEvent
from src.contracts.trade_signal import TradeSignalEvent, TradeSignalPayload
from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher

from .config import DecisionSettings
from .enrichment_cache import EnrichmentCache
from .filter import apply_direction_filter
from .market_hours import is_market_open_for_ticker
from .metrics import DecisionMetrics
from .risk_manager import RiskManager
from .rr_logic import compute_levels, evaluate_rr
from .sizing import compute_size

log = logging.getLogger(__name__)


class DecisionPipeline:
    def __init__(
        self,
        *,
        settings: DecisionSettings,
        enrichment_cache: EnrichmentCache,
        risk_manager: RiskManager,
        idem: IdempotencyGuard,
        publisher: StreamPublisher,
        metrics: DecisionMetrics,
    ) -> None:
        self.settings = settings
        self.cache = enrichment_cache
        self.risk = risk_manager
        self.idem = idem
        self.publisher = publisher
        self.metrics = metrics

    async def process(self, event: MessageEnvelope) -> None:
        if not isinstance(event, MLPredictionEvent):
            log.error("pipeline_wrong_type %s", type(event).__name__)
            self.metrics.inc("errors.wrong_type")
            return

        self.metrics.inc("events_in")
        t_start = time.perf_counter()

        # 1. Idempotency
        claimed = await self.idem.claim(
            scope=self.settings.idempotency_scope, key=event.event_id,
        )
        if not claimed:
            self.metrics.inc("events_skipped_idem")
            return

        ticker = event.payload.ticker

        # 2. Enrichment cache lookup
        enriched = await self.cache.get(event.payload.enriched_event_id)
        if enriched is None:
            self.metrics.inc("errors.enrichment_missing")
            log.warning("enrichment_missing enriched_event_id=%s ticker=%s",
                        event.payload.enriched_event_id, ticker)
            return

        # 2.4. Stale-features gate (Sprint 6).
        #
        # Belt-and-braces over Predictor's gate. If Predictor saw a stale
        # last_bar (cache gap) and somehow still emitted a prediction, REJECT
        # here. last_bar_time is naive MSK (Phase-2 CSV index format);
        # news_time/produced_at are UTC ISO. Convert both to UTC for diff.
        if self.settings.stale_features_max_gap_sec > 0:
            try:
                ref_ts_str_sf = event.payload.news_time or event.produced_at
                ref_dt_utc = pd.Timestamp(ref_ts_str_sf)
                if ref_dt_utc.tzinfo is None:
                    ref_dt_utc = ref_dt_utc.tz_localize("UTC")
                else:
                    ref_dt_utc = ref_dt_utc.tz_convert("UTC")
                last_bar_msk_naive = pd.Timestamp(event.payload.last_bar_time)
                if last_bar_msk_naive.tzinfo is not None:
                    # Defensive: if last_bar_time happens to be tz-aware, convert to UTC.
                    last_bar_utc = last_bar_msk_naive.tz_convert("UTC")
                else:
                    # Phase-2 convention: index is naive MSK. UTC = MSK - 3h.
                    last_bar_utc = (last_bar_msk_naive - pd.Timedelta(hours=3)).tz_localize("UTC")
                gap_sec_sf = (ref_dt_utc - last_bar_utc).total_seconds()
                if gap_sec_sf > self.settings.stale_features_max_gap_sec:
                    await self._publish_reject(
                        event, ticker,
                        f"stale_features:gap={int(gap_sec_sf)}s>{self.settings.stale_features_max_gap_sec}s",
                    )
                    self.metrics.inc("rejects.stale_features")
                    return
            except (ValueError, TypeError, AttributeError):
                # Best-effort — if parse fails, fall through (other gates still apply)
                pass

        # 2.5. Market hours gate (Sprint 6)
        #
        # Reject signals fired outside MOEX session hours OR on weekends. We
        # use news_time (or produced_at fallback) as the reference moment —
        # not wall-clock — so honest historical replay correctly decides
        # whether the market WAS open at the time of the news.
        if self.settings.market_hours_enabled:
            ref_ts_str = event.payload.news_time or event.produced_at
            try:
                ref_dt = datetime.fromisoformat(ref_ts_str)
            except ValueError:
                ref_dt = datetime.now(tz=timezone.utc)
            # try_normalize_ticker defensively — Predictor whitelist should already
            # filter unknown tickers, but be safe.
            if try_normalize_ticker(ticker) is None:
                await self._publish_reject(event, ticker, "unknown_ticker_no_asset_class")
                self.metrics.inc("rejects.market_closed")
                return
            is_open, mh_reason = is_market_open_for_ticker(
                ref_dt, ticker,
                skip_weekends=self.settings.market_hours_skip_weekends,
            )
            if not is_open:
                await self._publish_reject(event, ticker, f"market_closed:{mh_reason}")
                self.metrics.inc("rejects.market_closed")
                return

        # 3. R:R evaluation
        rr = evaluate_rr(
            event.payload.predictions,
            horizon_min=self.settings.horizon_min,
            rr_threshold=self.settings.rr_threshold,
            min_mfe_pct=self.settings.min_mfe_pct,
            min_mae_pct=self.settings.min_mae_pct,
        )
        if rr.side is None:
            await self._publish_reject(event, ticker, rr.reject_reason, rr_ratio=rr.rr_ratio)
            self.metrics.inc("rejects.rr_below_threshold")
            return

        # 4. DirectionFilter
        filter_result = apply_direction_filter(
            enriched, ticker, rr.side,
            min_confidence=self.settings.direction_filter_min_confidence,
        )
        if not filter_result.include:
            await self._publish_reject(event, ticker, filter_result.reject_reason,
                                       side=rr.side, rr_ratio=rr.rr_ratio)
            self.metrics.inc("rejects.direction_filter")
            return

        # 5. RiskManager gates
        if await self.risk.is_daily_kill_triggered():
            await self._publish_reject(event, ticker, "daily_kill_active",
                                       side=rr.side, rr_ratio=rr.rr_ratio)
            self.metrics.inc("rejects.daily_kill")
            return

        if await self.risk.is_cooldown_active(ticker):
            await self._publish_reject(event, ticker, "cooldown_active",
                                       side=rr.side, rr_ratio=rr.rr_ratio)
            self.metrics.inc("rejects.cooldown")
            return

        open_n = await self.risk.open_positions_count()
        if open_n >= self.settings.max_open_positions:
            await self._publish_reject(
                event, ticker,
                f"max_open_positions reached ({open_n}/{self.settings.max_open_positions})",
                side=rr.side, rr_ratio=rr.rr_ratio, open_positions=open_n,
            )
            self.metrics.inc("rejects.max_open")
            return

        # 6. Compute levels + sizing
        levels = compute_levels(
            last_close=event.payload.last_close,
            side=rr.side,
            pred_mfe_pct=rr.chosen_mfe_pct,
            pred_mae_pct=rr.chosen_mae_pct,
            tp_fraction=self.settings.tp_fraction,
            sl_buffer=self.settings.sl_buffer,
            sl_floor_pct=self.settings.sl_floor_pct,
            tp_floor_pct=self.settings.tp_floor_pct,
        )
        sizing = compute_size(
            ticker=ticker,
            side=rr.side,
            entry_price=levels.entry_price,
            sl_dist_abs=levels.sl_dist_abs,
            pred_mfe_pct=rr.chosen_mfe_pct,
            tp_fraction=self.settings.tp_fraction,
            equity_rub=self.settings.initial_equity_rub,
            risk_per_trade_pct=self.settings.risk_per_trade_pct,
            leverage=self.settings.leverage,
            lot_sizes=self.settings.lot_sizes,
        )

        # 7. Publish EXECUTE
        horizon_str = f"{self.settings.horizon_min}m"
        daily_pnl_pct = await self.risk.daily_pnl_pct()
        cooldown_active = await self.risk.is_cooldown_active(ticker)

        payload = TradeSignalPayload(
            prediction_event_id=event.event_id,
            action="EXECUTE",
            reject_reason="",
            ticker=ticker,
            side=rr.side,  # type: ignore[arg-type]
            horizon=horizon_str,  # type: ignore[arg-type]
            entry_price=levels.entry_price,
            stop_loss=levels.stop_loss,
            take_profit=levels.take_profit,
            quantity=sizing.quantity,
            risk_rub=sizing.risk_rub,
            expected_pnl_rub=sizing.expected_pnl_rub,
            rr_ratio=rr.rr_ratio,
            # Sprint 6: propagate news_time for honest historical replay в Bridge
            news_time=event.payload.news_time,
            open_positions=open_n,
            daily_pnl_pct=daily_pnl_pct,
            cooldown_active=cooldown_active,
        )

        signal_event = TradeSignalEvent(
            producer=self.settings.producer_name,
            trace=event.trace,
            payload=payload,
        )
        await self.publisher.publish(signal_event)
        self.metrics.inc("signals_execute")
        elapsed_ms = (time.perf_counter() - t_start) * 1000
        self.metrics.record_latency_ms(elapsed_ms)

        log.info(
            "execute event_id=%s ticker=%s side=%s rr=%.2f qty=%d risk=%.0f₽ tp=%.4f sl=%.4f (capped=%s)",
            event.event_id, ticker, rr.side, rr.rr_ratio,
            sizing.quantity, sizing.risk_rub,
            levels.take_profit, levels.stop_loss, sizing.capped_by or "none",
        )

    async def _publish_reject(
        self,
        prediction_event: MLPredictionEvent,
        ticker: str,
        reason: str,
        *,
        side: Optional[str] = None,
        rr_ratio: float = 0.0,
        open_positions: Optional[int] = None,
    ) -> None:
        """Publish action=REJECT signal. Empty execute-fields. Все REJECT-причины
        логируются в trade:signals для post-mortem analytics."""
        if open_positions is None:
            open_positions = await self.risk.open_positions_count()
        daily_pnl_pct = await self.risk.daily_pnl_pct()
        cooldown_active = await self.risk.is_cooldown_active(ticker)

        payload = TradeSignalPayload(
            prediction_event_id=prediction_event.event_id,
            action="REJECT",
            reject_reason=reason[:200],
            ticker=ticker,
            side=None,
            horizon=None,
            entry_price=None,
            stop_loss=None,
            take_profit=None,
            quantity=None,
            risk_rub=None,
            expected_pnl_rub=None,
            rr_ratio=None,
            # Sprint 6: propagate news_time для consistency (analytics)
            news_time=prediction_event.payload.news_time,
            open_positions=open_positions,
            daily_pnl_pct=daily_pnl_pct,
            cooldown_active=cooldown_active,
        )
        signal_event = TradeSignalEvent(
            producer=self.settings.producer_name,
            trace=prediction_event.trace,
            payload=payload,
        )
        await self.publisher.publish(signal_event)
        self.metrics.inc("signals_reject")
        log.info(
            "reject event_id=%s ticker=%s reason=%s",
            prediction_event.event_id, ticker, reason[:120],
        )
