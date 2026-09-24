"""PaperExecutor — simulate fills + bar-by-bar SL/TP/time-stop tracking.

Design (PHASE2 §2.3 + sprint4/exits/baseline.py):

OPEN flow (synchronous, milliseconds):
  1. Lookup next-min bar in candle cache: bars[ticker].loc[ts_open]
     где ts_open = signal.produced_at округлённое к следующей минуте.
  2. filled_price = entry_bar.open × (1 + slippage_half × side)
     (half slippage — slippage_rt_pct делится на 2 для entry vs exit)
  3. Возвращаем OPEN ExecutionResultPayload (status=FILLED, exit_*=None).

CLOSE flow (bar-by-bar, async):
  Each poll_interval_sec:
    - Получаем актуальный window bars[entry_ts:now]
    - Bar-by-bar walk: SL-first проверка (Phase 2 convention)
    - Если SL/TP hit → exit_price = level price + slippage_half (направление exit)
      → realized_pnl_rub = (exit-entry) × side × lot × qty - cost_rub
    - Если elapsed ≥ horizon_min → time-exit с last_close
  Все три исхода публикуют CLOSE ExecutionResultPayload.

Real bridge не использует этот executor — Sprint 6 заменит на QUIK Lua.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import pandas as pd

from src.contracts.base import utcnow_iso
from src.contracts.execution_result import (
    ExecutionResultEvent,
    ExecutionResultPayload,
)
from src.contracts.trade_signal import TradeSignalEvent
from src.infra.publisher import StreamPublisher

from src.infra.candles import CandleCache

from .config import BridgeSettings

log = logging.getLogger(__name__)


@dataclass
class OpenPosition:
    """Persistable state. Serialized как JSON в bridge:open_positions:<id>."""
    signal_event_id: str
    ticker: str
    side: str  # BUY/SELL
    entry_price: float
    sl: float
    tp: float
    quantity: int
    entry_ts_iso: str  # UTC ISO of fill
    horizon_min: int = 60
    notional_rub: float = 0.0

    def to_json(self) -> str:
        return json.dumps({
            "signal_event_id": self.signal_event_id,
            "ticker": self.ticker,
            "side": self.side,
            "entry_price": self.entry_price,
            "sl": self.sl,
            "tp": self.tp,
            "quantity": self.quantity,
            "entry_ts_iso": self.entry_ts_iso,
            "horizon_min": self.horizon_min,
            "notional_rub": self.notional_rub,
        })

    @classmethod
    def from_json(cls, s: str) -> "OpenPosition":
        d = json.loads(s)
        return cls(**d)

    @property
    def entry_ts_utc(self) -> datetime:
        return datetime.fromisoformat(self.entry_ts_iso)

    @property
    def entry_ts_msk_naive(self) -> pd.Timestamp:
        """Phase 2 candle index = naive MSK = UTC + 3h naive."""
        utc = self.entry_ts_utc
        if utc.tzinfo is not None:
            utc = utc.astimezone(timezone.utc).replace(tzinfo=None)
        return pd.Timestamp(utc + timedelta(hours=3))


@dataclass
class ExitOutcome:
    """Result of bar-by-bar SL/TP/time decision."""
    exit_reason: str   # "tp" | "sl" | "time" | "kill"
    exit_price: float
    exit_ts_utc: datetime
    realized_pnl_rub: float
    cost_rub: float
    duration_sec: int


class PaperExecutor:
    """Stateless OPEN simulation + bar-by-bar exit detection."""

    def __init__(self, settings: BridgeSettings, candles: CandleCache) -> None:
        self.settings = settings
        self.candles = candles

    def _slippage_half_pct(self, ticker: str) -> float:
        full = self.settings.slippage_rt_pct.get(ticker, 0.05)
        return full / 2.0 / 100.0  # half + convert from % to ratio

    def _brokerage_half_pct(self, ticker: str) -> float:
        full = self.settings.brokerage_rt_pct.get(ticker, 0.08)
        return full / 2.0 / 100.0

    def _cost_rub(self, ticker: str, notional_rub: float) -> float:
        """Total round-trip cost on a single notional, for one side."""
        slip = self._slippage_half_pct(ticker)
        brok = self._brokerage_half_pct(ticker)
        return notional_rub * (slip + brok)

    def open_position(self, signal: TradeSignalEvent) -> Optional[OpenPosition]:
        """Find next-min bar and simulate fill. Returns OpenPosition or None on failure."""
        p = signal.payload
        assert p.action == "EXECUTE" and p.side in {"BUY", "SELL"} and p.entry_price is not None

        # Sprint 6: prefer payload.news_time (original Telegram time) for honest
        # historical replay. Bridge looks up candles AT THAT moment for fill,
        # not "now". Fallback на signal.produced_at для backward compat.
        ref_ts_str = p.news_time or signal.produced_at
        ts_open_utc = datetime.fromisoformat(ref_ts_str)
        # Phase 2 candle index = naive MSK (UTC+3 stripped tz)
        if ts_open_utc.tzinfo is not None:
            ts_open_utc = ts_open_utc.astimezone(timezone.utc).replace(tzinfo=None)
        ts_open_msk = pd.Timestamp(ts_open_utc + timedelta(hours=3))

        # Next minute boundary
        next_min = (ts_open_msk + pd.Timedelta(seconds=60)).floor("min")
        bars = self.candles.get(p.ticker)
        if bars is None or len(bars) == 0:
            log.warning("open_failed_no_candles ticker=%s", p.ticker)
            return None

        idx = bars.index.searchsorted(next_min)
        if idx >= len(bars):
            # No future bar — either paper-static snapshot OR live data feed has stalled.
            # Sprint 6: distinguish via stale-bar gate. We measure freshness relative
            # to signal_time (NOT wall-clock) so honest historical replay still works
            # (signal at 2024, last bar at 2026 → searchsorted hits len(bars) only
            # when signal is AHEAD of all bars, i.e., QUIK is genuinely behind).
            last_bar_ts = bars.index[-1]
            gap_sec = (ts_open_msk - last_bar_ts).total_seconds()
            threshold_sec = self.settings.stale_bar_threshold_sec
            if gap_sec > threshold_sec:
                log.warning(
                    "open_rejected_stale_candles ticker=%s next_min=%s last_bar=%s gap=%.0fs threshold=%ds — REJECT fill",
                    p.ticker, next_min, last_bar_ts, gap_sec, threshold_sec,
                )
                return None
            log.warning(
                "open_no_future_bar ticker=%s next_min=%s last_bar=%s gap=%.0fs — fallback to last close (within threshold)",
                p.ticker, next_min, last_bar_ts, gap_sec,
            )
            entry_bar = bars.iloc[-1]
            entry_ts_msk = last_bar_ts
        else:
            entry_bar = bars.iloc[idx]
            entry_ts_msk = bars.index[idx]

        raw_price = float(entry_bar["open"])

        # Sprint 6: price-drift gate.
        # If actual fill price diverges significantly from signal.entry_price
        # (the reference price Decision used to compute SL/TP), SL/TP are no
        # longer aligned with entry and the trade is broken. This catches the
        # candle-cache-gap divergence we saw on 2026-06-01.
        ref_signal_price = p.entry_price
        if ref_signal_price is not None and ref_signal_price > 0:
            drift = abs(raw_price - ref_signal_price) / ref_signal_price
            if drift > self.settings.max_entry_drift_pct:
                log.warning(
                    "open_rejected_price_drift ticker=%s signal_price=%.4f bar_open=%.4f drift=%.4f%% threshold=%.4f%% — REJECT fill",
                    p.ticker, ref_signal_price, raw_price, drift * 100,
                    self.settings.max_entry_drift_pct * 100,
                )
                return None

        side_sign = 1 if p.side == "BUY" else -1
        # Long bought above mid; short sold below mid; slippage worsens entry
        filled = raw_price * (1 + side_sign * self._slippage_half_pct(p.ticker))

        # entry_ts UTC = MSK − 3h (back to UTC)
        entry_ts_utc = (entry_ts_msk.to_pydatetime() - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        notional_rub = filled * p.quantity * self.settings.lot_sizes.get(p.ticker, 1)

        # SL/TP unchanged from signal — Decision уже посчитал на last_close
        return OpenPosition(
            signal_event_id=signal.event_id,
            ticker=p.ticker,
            side=p.side,
            entry_price=filled,
            sl=p.stop_loss,
            tp=p.take_profit,
            quantity=p.quantity,
            entry_ts_iso=entry_ts_utc.isoformat(timespec="milliseconds"),
            horizon_min=int(p.horizon.rstrip("m")) if p.horizon else 60,
            notional_rub=notional_rub,
        )

    def build_open_payload(
        self, pos: OpenPosition, *, error_message: str = "",
    ) -> ExecutionResultPayload:
        return ExecutionResultPayload(
            signal_event_id=pos.signal_event_id,
            status="FILLED",
            error_message=error_message,
            filled_price=pos.entry_price,
            filled_quantity=pos.quantity,
            fill_time=pos.entry_ts_iso,
            bridge_latency_ms=0.0,  # paper — synchronous
            quik_ack_latency_ms=0.0,
        )

    def check_exit(
        self, pos: OpenPosition, now_msk: Optional[pd.Timestamp] = None,
    ) -> Optional[ExitOutcome]:
        """Bar-by-bar walk: SL-first (Phase 2 baseline.py convention).

        Returns ExitOutcome if SL/TP/time-stop hit, else None (position alive).
        """
        bars = self.candles.get(pos.ticker)
        if bars is None or len(bars) == 0:
            return None

        entry_ts = pos.entry_ts_msk_naive
        end_ts = entry_ts + pd.Timedelta(minutes=pos.horizon_min)
        # Right-bound: include up to min(now, end_ts)
        if now_msk is None:
            now_msk = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3))
        right = min(end_ts, now_msk)

        # Phase 2 baseline.py: bars[bars.index > entry_ts] (production-honest, без entry-bar lookback)
        window = bars[(bars.index > entry_ts) & (bars.index <= right)]
        side_sign = 1 if pos.side == "BUY" else -1

        for ts_bar, row in window.iterrows():
            high = float(row["high"]); low = float(row["low"])
            # SL-first
            if pos.side == "BUY":
                if low <= pos.sl:
                    return self._build_exit(pos, "sl", pos.sl, ts_bar)
                if high >= pos.tp:
                    return self._build_exit(pos, "tp", pos.tp, ts_bar)
            else:  # SELL
                if high >= pos.sl:
                    return self._build_exit(pos, "sl", pos.sl, ts_bar)
                if low <= pos.tp:
                    return self._build_exit(pos, "tp", pos.tp, ts_bar)

        # No SL/TP — check time-stop
        if now_msk >= end_ts:
            # time-exit at last available bar close (или fall-back на entry)
            if len(window) > 0:
                last_close = float(window.iloc[-1]["close"])
                last_ts = window.index[-1]
            else:
                last_close = pos.entry_price
                last_ts = end_ts
            return self._build_exit(pos, "time", last_close, last_ts)

        return None

    def _build_exit(
        self, pos: OpenPosition, reason: str, exit_price: float, ts_msk: pd.Timestamp,
    ) -> ExitOutcome:
        # apply slippage on exit (same direction as entry — slippage worsens both)
        side_sign = 1 if pos.side == "BUY" else -1
        exit_filled = exit_price * (1 - side_sign * self._slippage_half_pct(pos.ticker))

        lot = self.settings.lot_sizes.get(pos.ticker, 1)
        if pos.side == "BUY":
            gross = (exit_filled - pos.entry_price) * lot * pos.quantity
        else:
            gross = (pos.entry_price - exit_filled) * lot * pos.quantity

        # full round-trip cost (brokerage only — slippage уже в filled prices)
        notional = pos.entry_price * lot * pos.quantity
        brokerage_rt = self.settings.brokerage_rt_pct.get(pos.ticker, 0.08) / 100.0
        cost_rub = notional * brokerage_rt
        net = gross - cost_rub

        # ts_msk → UTC
        ts_utc = (ts_msk.to_pydatetime() - timedelta(hours=3)).replace(tzinfo=timezone.utc)
        entry_utc = pos.entry_ts_utc
        if entry_utc.tzinfo is None:
            entry_utc = entry_utc.replace(tzinfo=timezone.utc)
        duration_sec = max(0, int((ts_utc - entry_utc).total_seconds()))

        return ExitOutcome(
            exit_reason=reason,
            exit_price=exit_filled,
            exit_ts_utc=ts_utc,
            realized_pnl_rub=net,
            cost_rub=cost_rub,
            duration_sec=duration_sec,
        )

    def build_close_payload(self, pos: OpenPosition, outcome: ExitOutcome) -> ExecutionResultPayload:
        return ExecutionResultPayload(
            signal_event_id=pos.signal_event_id,
            status="FILLED",
            filled_price=pos.entry_price,
            filled_quantity=pos.quantity,
            fill_time=pos.entry_ts_iso,
            realized_pnl_rub=outcome.realized_pnl_rub,
            exit_reason=outcome.exit_reason,  # type: ignore[arg-type]
            exit_price=outcome.exit_price,
            exit_time=outcome.exit_ts_utc.isoformat(timespec="milliseconds"),
            duration_sec=outcome.duration_sec,
            bridge_latency_ms=0.0,
            quik_ack_latency_ms=0.0,
        )
