"""Loopback-тест RealExecutor с mock-QUIK. Нулевой риск, без реального QUIK.

Проверяет: gate → ENTRY → реальный fill → server-side OCO → OPEN payload;
затем срабатывание OCO-ноги → CLOSE payload с реальной комиссией и PnL.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from src.contracts.trade_signal import TradeSignalEvent, TradeSignalPayload
from src.services.bridge.config import BridgeSettings
from src.services.bridge.quik_order_client import QuikOrderClient, StatusEvent
from src.services.bridge.risk_guard import RiskGuard
from src.services.bridge.real_executor import RealExecutor


class MockQuikLua:
    """Имитирует order_bridge.lua: ENTRY → fill; EXIT_OCO → стоит активной."""

    def __init__(self, orders: Path, status: Path):
        self.orders, self.status = Path(orders), Path(status)
        self._off = 0
        self.oco_by_trans: dict[int, dict] = {}

    def pump(self):
        if not self.orders.exists():
            return
        with self.orders.open(encoding="utf-8") as f:
            f.seek(self._off)
            new = [l for l in f.read().splitlines() if l.strip()]
            self._off = f.tell()
        out = []
        for ln in new:
            o = json.loads(ln)
            tid = o["trans_id"]
            out.append({"kind": "TRANS_REPLY", "trans_id": tid,
                        "order_num": 100000 + tid, "status": "accepted"})
            if o["kind"] == "ENTRY":
                px = o["price"] if o["price"] > 0 else 250.0
                out.append({"kind": "TRADE", "trans_id": tid, "order_num": 100000 + tid,
                            "status": "filled", "quantity": o["quantity"], "price": px,
                            "commission": round(px * o["quantity"] * 0.0006, 4)})
            elif o["kind"] == "EXIT_OCO":
                out.append({"kind": "ORDER", "trans_id": tid, "order_num": 100000 + tid,
                            "status": "active"})
                self.oco_by_trans[tid] = o
        self._append(out)

    def fill_oco(self, trans_id: int, price: float):
        """Симулировать срабатывание OCO-ноги (sl или tp)."""
        o = self.oco_by_trans[trans_id]
        self._append([{"kind": "TRADE", "trans_id": trans_id,
                       "order_num": 100000 + trans_id, "status": "filled",
                       "quantity": o["quantity"], "price": price,
                       "commission": round(price * o["quantity"] * 0.0006, 4)}])

    def _append(self, rows):
        if not rows:
            return
        with self.status.open("a", encoding="utf-8") as f:
            for d in rows:
                f.write(json.dumps(d) + "\n")


def _signal(factory, event_id="evt-r1", side="BUY", entry=250.0, sl=247.5, tp=255.0):
    return factory(event_id=event_id, ticker="SBER", side=side, entry_price=entry,
                   stop_loss=sl, take_profit=tp, quantity=1)


@pytest.fixture
def setup(tmp_path):
    s = BridgeSettings(prices_dir=tmp_path, max_qty_per_order=1, quik_account="L01-TEST",
                       quik_orders_path=tmp_path / "o.jsonl",
                       quik_status_path=tmp_path / "s.jsonl",
                       quik_transmap_path=tmp_path / "m.json",
                       entry_order_type="L", marketable_limit_offset_pct=0.1,
                       kill_switch_key=str(tmp_path / "nokill"),
                       status_poll_interval_sec=0.02, order_fill_timeout_sec=5.0)
    client = QuikOrderClient(s.quik_orders_path, s.quik_status_path, s.quik_transmap_path)
    guard = RiskGuard(s, redis=None)
    guard.check_trading_hours = lambda *a, **k: None  # детерминизм теста
    lua = MockQuikLua(s.quik_orders_path, s.quik_status_path)
    ex = RealExecutor(s, client, guard, ref_price_fn=lambda t: 250.0)
    return ex, lua, client


@pytest.mark.asyncio
async def test_open_fills_and_places_oco(setup, execute_signal_factory):
    ex, lua, client = setup

    async def pumper():
        for _ in range(50):
            lua.pump()
            await asyncio.sleep(0.02)

    pump_task = asyncio.create_task(pumper())
    pos, payload = await ex.open_position(_signal(execute_signal_factory))
    pump_task.cancel()
    lua.pump()  # прокачать OCO-заявку, отправленную после fill

    assert pos is not None
    assert payload.status == "FILLED"
    assert payload.filled_quantity == 1
    # marketable-limit BUY: 250 * 1.001 = 250.25
    assert payload.filled_price == pytest.approx(250.25, abs=0.01)
    assert payload.trans_id == pos.entry_trans_id
    # OCO выставлена
    assert pos.oco_trans_id in lua.oco_by_trans
    oco = lua.oco_by_trans[pos.oco_trans_id]
    assert oco["operation"] == "S"           # закрытие лонга
    assert oco["stop_price"] == pytest.approx(247.5)
    assert oco["tp_price"] == pytest.approx(255.0)
    assert pos.commission_entry > 0


@pytest.mark.asyncio
async def test_close_on_tp(setup, execute_signal_factory):
    ex, lua, client = setup

    async def pumper():
        for _ in range(50):
            lua.pump()
            await asyncio.sleep(0.02)

    pt = asyncio.create_task(pumper())
    pos, _ = await ex.open_position(_signal(execute_signal_factory))
    pt.cancel()
    lua.pump()  # прокачать OCO-заявку

    lua.fill_oco(pos.oco_trans_id, price=255.0)        # TP сработал
    trade = next(e for e in client.poll_status()
                 if e.kind == "TRADE" and e.trans_id == pos.oco_trans_id)
    close = ex.build_close_payload(pos, trade, reason="tp")
    assert close.exit_reason == "tp"
    assert close.exit_price == pytest.approx(255.0)
    # SBER lot=10: gross = (255 - 250.25)*10 = 47.5; minus комиссии входа+выхода
    expected = 47.5 - pos.commission_entry - round(255.0 * 0.0006, 4)
    assert close.realized_pnl_rub == pytest.approx(expected, abs=0.05)


@pytest.mark.asyncio
async def test_idempotent_resend_returns_none(setup, execute_signal_factory):
    ex, lua, client = setup

    async def pumper():
        for _ in range(50):
            lua.pump()
            await asyncio.sleep(0.02)

    pt = asyncio.create_task(pumper())
    pos1, _ = await ex.open_position(_signal(execute_signal_factory, "evt-dup"))
    pt.cancel()
    # повторная доставка того же сигнала — не должно слать новую заявку
    pos2, payload2 = await ex.open_position(_signal(execute_signal_factory, "evt-dup"))
    assert pos2 is None
    assert payload2.status == "REJECTED" and "already_sent" in payload2.error_message
