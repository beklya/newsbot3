"""RealExecutor — реальное исполнение через QUIK (mode="real").

Стратегия-агностик: исполняет EXECUTE TradeSignalEvent как есть (любой источник
сигналов). Симметричен PaperExecutor по выходным контрактам (OPEN/CLOSE
ExecutionResultPayload), но fills и выходы — настоящие.

Поток OPEN (async):
  1. trans_id_for(event_id) — идемпотентность; повтор сигнала не шлёт новую заявку.
  2. RiskGuard.gate (kill/hours/rate/collar) + clamp qty к потолку (1 лот first).
  3. ENTRY: marketable-limit (контроль цены) или market.
  4. Ждём реальный TRADE-callback (poll_status) до order_fill_timeout_sec.
  5. На fill → выставляем server-side OCO (одна QUIK-заявка «TP+SL»): переживает
     обрыв Python, одна нога снимает другую на стороне QUIK.
  6. OPEN ExecutionResultPayload с реальными fill-price/qty/commission + order_num'ами.

Поток CLOSE (через poll_status в tracker'е real-режима):
  - TRADE на trans_id OCO-заявки → позиция закрыта (sl/tp по сработавшей ноге);
  - либо time-stop: на горизонте Python шлёт CLOSE_MKT + снимает OCO.
  CLOSE ExecutionResultPayload с реальной exit-price/commission.

order_bridge.lua форматирует это в QUIK sendTransaction (см. quik_live/).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.contracts.base import utcnow_iso
from src.contracts.execution_result import ExecutionResultPayload
from src.contracts.trade_signal import TradeSignalEvent

from .config import BridgeSettings
from .quik_order_client import QuikOrderClient, OrderRequest, StatusEvent
from .risk_guard import RiskGuard

log = logging.getLogger(__name__)


@dataclass
class RealPosition:
    """Персистится в bridge:open_positions:<id> для recovery + reconcile из QUIK."""
    signal_event_id: str
    ticker: str
    side: str                 # BUY/SELL (вход)
    entry_price: float
    sl: float
    tp: float
    quantity: int
    entry_ts_iso: str
    horizon_min: int
    entry_trans_id: int
    entry_order_num: int
    oco_trans_id: int
    commission_entry: float = 0.0
    notional_rub: float = 0.0

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, s: str) -> "RealPosition":
        return cls(**json.loads(s))


class FillTimeout(Exception):
    pass


class RealExecutor:
    """Реальное исполнение. Stateless по позициям (состояние — в Redis/QUIK)."""

    def __init__(self, settings: BridgeSettings, client: QuikOrderClient,
                 guard: RiskGuard, ref_price_fn) -> None:
        self.s = settings
        self.client = client
        self.guard = guard
        # ref_price_fn(ticker) -> Optional[float]: текущая рыночная цена (last close
        # из CandleCache live-свечей quik_feed) для collar и marketable-limit.
        self.ref_price = ref_price_fn

    # --- helpers ---
    def _codes(self, ticker: str) -> tuple[str, str]:
        return (self.s.quik_class_codes.get(ticker, "TQBR"),
                self.s.quik_sec_codes.get(ticker, ticker))

    def _entry_limit_price(self, side: str, ref: float) -> tuple[str, float]:
        if self.s.entry_order_type == "M":
            return "M", 0.0
        off = self.s.marketable_limit_offset_pct / 100.0
        # marketable: BUY чуть выше рынка, SELL чуть ниже — гарантирует исполнение
        # но ограничивает проскальзывание тонкого стакана
        px = ref * (1 + off) if side == "BUY" else ref * (1 - off)
        return "L", round(px, 4)

    async def _await_trade(self, trans_id: int,
                           on_status) -> Optional[StatusEvent]:
        """Поллит status до TRADE по trans_id или таймаута. on_status — колбэк
        для маршрутизации «чужих» статусов (выходы других позиций) в tracker."""
        deadline = time.monotonic() + self.s.order_fill_timeout_sec
        seen_reject = None
        while time.monotonic() < deadline:
            for ev in self.client.poll_status():
                if ev.trans_id != trans_id:
                    if on_status:
                        on_status(ev)
                    continue
                if ev.kind == "TRADE":
                    return ev
                if ev.kind in ("TRANS_REPLY", "ORDER") and "reject" in ev.status.lower():
                    seen_reject = ev.status
                if ev.kind == "ERROR":
                    seen_reject = ev.status
            if seen_reject:
                raise FillTimeout(f"rejected: {seen_reject}")
            await asyncio.sleep(self.s.status_poll_interval_sec)
        raise FillTimeout(f"no fill in {self.s.order_fill_timeout_sec}s")

    # --- OPEN ---
    async def open_position(
        self, signal: TradeSignalEvent, on_status=None,
    ) -> tuple[Optional[RealPosition], ExecutionResultPayload]:
        p = signal.payload
        assert p.action == "EXECUTE" and p.side in {"BUY", "SELL"}
        t0 = time.monotonic()
        trans_id, is_new = self.client.trans_id_for(signal.event_id)
        if not is_new:
            # сигнал уже отправлялся (повторная доставка) — не дублируем заявку.
            log.warning("open_skip_already_sent event_id=%s trans_id=%s",
                        signal.event_id, trans_id)
            return None, self._reject(signal, "already_sent", trans_id)

        ref = self.ref_price(p.ticker)
        if ref is None or ref <= 0:
            ref = p.entry_price or 0.0
        qty, capped = self.guard.clamp_qty(p.quantity)
        if capped:
            log.warning("qty_capped %s %d→%d", p.ticker, p.quantity, qty)

        reason = await self.guard.gate(ref_price=p.entry_price, current_price=ref, qty=qty)
        if reason:
            log.warning("open_blocked_by_guard %s: %s", p.ticker, reason)
            return None, self._reject(signal, reason, trans_id)

        cls, sec = self._codes(p.ticker)
        otype, limit_px = self._entry_limit_price(p.side, ref)
        op = "B" if p.side == "BUY" else "S"
        self.client.send(OrderRequest(
            trans_id=trans_id, kind="ENTRY", account=self.s.quik_account,
            class_code=cls, sec_code=sec, operation=op, order_type=otype,
            quantity=qty, price=limit_px, client_code=self.s.quik_client_code,
            ts=utcnow_iso()))
        self.guard.register_sent()

        try:
            trade = await self._await_trade(trans_id, on_status)
        except FillTimeout as e:
            log.error("entry_fill_timeout %s trans_id=%s: %s", p.ticker, trans_id, e)
            return None, self._reject(signal, f"fill_timeout: {e}", trans_id,
                                      status="TIMEOUT")

        fill_px = trade.price
        fill_iso = utcnow_iso()
        lot = self.s.lot_sizes.get(p.ticker, 1)
        notional = fill_px * trade.quantity * lot

        # server-side OCO: сторона закрытия противоположна входу
        close_op = "S" if p.side == "BUY" else "B"
        oco_trans, _ = self.client.trans_id_for(signal.event_id + ":oco")
        self.client.send(OrderRequest(
            trans_id=oco_trans, kind="EXIT_OCO", account=self.s.quik_account,
            class_code=cls, sec_code=sec, operation=close_op, order_type="L",
            quantity=trade.quantity, stop_price=round(p.stop_loss, 4),
            tp_price=round(p.take_profit, 4),
            client_code=self.s.quik_client_code, ts=utcnow_iso()))

        pos = RealPosition(
            signal_event_id=signal.event_id, ticker=p.ticker, side=p.side,
            entry_price=fill_px, sl=p.stop_loss, tp=p.take_profit,
            quantity=trade.quantity, entry_ts_iso=fill_iso,
            horizon_min=int(p.horizon.rstrip("m")) if p.horizon else 60,
            entry_trans_id=trans_id, entry_order_num=trade.order_num,
            oco_trans_id=oco_trans, commission_entry=trade.commission,
            notional_rub=notional)

        payload = ExecutionResultPayload(
            signal_event_id=signal.event_id, status="FILLED",
            trans_id=trans_id, order_num=trade.order_num,
            filled_price=fill_px, filled_quantity=trade.quantity, fill_time=fill_iso,
            stop_order_num=oco_trans, tp_order_num=oco_trans,
            bridge_latency_ms=round((time.monotonic() - t0) * 1000, 1),
            quik_ack_latency_ms=0.0)
        log.info("OPEN real %s %s qty=%d @ %.4f comm=%.2f oco_trans=%d",
                 p.ticker, p.side, trade.quantity, fill_px, trade.commission, oco_trans)
        return pos, payload

    # --- CLOSE (по сработавшей OCO-ноге или time-stop) ---
    def build_close_payload(self, pos: RealPosition, trade: StatusEvent,
                            reason: str) -> ExecutionResultPayload:
        exit_px = trade.price
        lot = self.s.lot_sizes.get(pos.ticker, 1)
        if pos.side == "BUY":
            gross = (exit_px - pos.entry_price) * lot * pos.quantity
        else:
            gross = (pos.entry_price - exit_px) * lot * pos.quantity
        total_comm = pos.commission_entry + trade.commission
        net = gross - total_comm
        entry_utc = datetime.fromisoformat(pos.entry_ts_iso)
        if entry_utc.tzinfo is None:
            entry_utc = entry_utc.replace(tzinfo=timezone.utc)
        dur = max(0, int((datetime.now(timezone.utc) - entry_utc).total_seconds()))
        return ExecutionResultPayload(
            signal_event_id=pos.signal_event_id, status="FILLED",
            order_num=trade.order_num,
            filled_price=pos.entry_price, filled_quantity=pos.quantity,
            fill_time=pos.entry_ts_iso,
            realized_pnl_rub=round(net, 2), exit_reason=reason,  # type: ignore[arg-type]
            exit_price=exit_px, exit_time=utcnow_iso(), duration_sec=dur,
            bridge_latency_ms=0.0, quik_ack_latency_ms=0.0)

    def send_time_close(self, pos: RealPosition) -> int:
        """Снять OCO и закрыть рынком на горизонте. Возвращает trans_id close."""
        cls, sec = self._codes(pos.ticker)
        self.client.send(OrderRequest(
            trans_id=pos.oco_trans_id, kind="CANCEL", account=self.s.quik_account,
            class_code=cls, sec_code=sec, operation="S", order_type="M",
            quantity=0, cancel_order_num=pos.oco_trans_id, ts=utcnow_iso()))
        close_op = "S" if pos.side == "BUY" else "B"
        close_trans, _ = self.client.trans_id_for(pos.signal_event_id + ":close")
        self.client.send(OrderRequest(
            trans_id=close_trans, kind="CLOSE_MKT", account=self.s.quik_account,
            class_code=cls, sec_code=sec, operation=close_op, order_type="M",
            quantity=pos.quantity, client_code=self.s.quik_client_code,
            ts=utcnow_iso()))
        return close_trans

    def _reject(self, signal: TradeSignalEvent, reason: str, trans_id: int,
                status: str = "REJECTED") -> ExecutionResultPayload:
        return ExecutionResultPayload(
            signal_event_id=signal.event_id, status=status,  # type: ignore[arg-type]
            trans_id=trans_id, error_message=reason,
            bridge_latency_ms=0.0, quik_ack_latency_ms=0.0)
