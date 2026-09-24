"""Loopback-тест транспорта QuikOrderClient ↔ (mock) order_bridge.lua.

Нулевой риск: реального QUIK нет, mock-responder на Python имитирует
sendTransaction + callbacks. Проверяет:
  - append заявок в orders.jsonl;
  - идемпотентность trans_id (event_id → один trans_id);
  - offset-чтение статусов (не реагируем на старые/уже прочитанные);
  - полный round-trip ENTRY → TRANS_REPLY + TRADE.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.services.bridge.quik_order_client import (
    QuikOrderClient, OrderRequest, StatusEvent,
)


class MockQuikLua:
    """Имитирует order_bridge.lua: читает новые orders, пишет callbacks."""

    def __init__(self, orders_path: Path, status_path: Path):
        self.orders_path = Path(orders_path)
        self.status_path = Path(status_path)
        self._offset = 0
        self._seen: set[int] = set()

    def pump(self) -> None:
        if not self.orders_path.exists():
            return
        with self.orders_path.open("r", encoding="utf-8") as f:
            f.seek(self._offset)
            new = [ln for ln in f.read().splitlines() if ln.strip()]
            self._offset = f.tell()
        out = []
        for ln in new:
            o = json.loads(ln)
            tid = o["trans_id"]
            if tid in self._seen:        # дедуп как настоящий QUIK по trans_id
                continue
            self._seen.add(tid)
            out.append({"kind": "TRANS_REPLY", "trans_id": tid,
                        "order_num": 100000 + tid, "status": "accepted", "ts": "t"})
            if o["kind"] == "ENTRY":
                notional = o["price"] * o["quantity"]
                out.append({"kind": "TRADE", "trans_id": tid,
                            "order_num": 100000 + tid, "status": "filled",
                            "quantity": o["quantity"], "price": o["price"],
                            "commission": round(notional * 0.0006, 4), "ts": "t"})
            else:  # STOP / TP — заявка встала, без исполнения
                out.append({"kind": "ORDER", "trans_id": tid,
                            "order_num": 100000 + tid, "status": "active", "ts": "t"})
        if out:
            with self.status_path.open("a", encoding="utf-8") as f:
                for d in out:
                    f.write(json.dumps(d) + "\n")


@pytest.fixture
def client_and_lua(tmp_path):
    orders = tmp_path / "orders.jsonl"
    status = tmp_path / "status.jsonl"
    tmap = tmp_path / "transmap.json"
    client = QuikOrderClient(orders, status, tmap)
    lua = MockQuikLua(orders, status)
    return client, lua


def _entry(tid: int) -> OrderRequest:
    return OrderRequest(trans_id=tid, kind="ENTRY", account="L01", class_code="TQBR",
                        sec_code="SBER", operation="B", order_type="L",
                        quantity=1, price=250.0)


def test_entry_roundtrip(client_and_lua):
    client, lua = client_and_lua
    tid, is_new = client.trans_id_for("evt-1")
    assert is_new and tid == 1
    client.send(_entry(tid))
    lua.pump()
    evs = client.poll_status()
    kinds = [e.kind for e in evs]
    assert "TRANS_REPLY" in kinds and "TRADE" in kinds
    trade = next(e for e in evs if e.kind == "TRADE")
    assert trade.trans_id == tid and trade.price == 250.0
    assert trade.quantity == 1 and trade.commission == pytest.approx(0.15)


def test_trans_id_idempotent(client_and_lua):
    client, lua = client_and_lua
    tid1, new1 = client.trans_id_for("evt-X")
    tid2, new2 = client.trans_id_for("evt-X")   # повторная доставка
    assert tid1 == tid2 and new1 is True and new2 is False


def test_idempotent_resend_no_double_fill(client_and_lua):
    client, lua = client_and_lua
    tid, _ = client.trans_id_for("evt-2")
    client.send(_entry(tid))
    client.send(_entry(tid))      # дубль (как повторная доставка сигнала)
    lua.pump()
    trades = [e for e in client.poll_status() if e.kind == "TRADE"]
    assert len(trades) == 1       # QUIK дедуплицировал по trans_id


def test_offset_no_reread(client_and_lua):
    client, lua = client_and_lua
    tid, _ = client.trans_id_for("evt-3")
    client.send(_entry(tid))
    lua.pump()
    first = client.poll_status()
    assert first
    second = client.poll_status()  # ничего нового
    assert second == []


def test_transmap_persists_across_restart(tmp_path):
    orders, status, tmap = (tmp_path / "o.jsonl", tmp_path / "s.jsonl",
                            tmp_path / "m.json")
    c1 = QuikOrderClient(orders, status, tmap)
    tid, _ = c1.trans_id_for("evt-persist")
    c2 = QuikOrderClient(orders, status, tmap)   # «рестарт»
    tid2, is_new = c2.trans_id_for("evt-persist")
    assert tid2 == tid and is_new is False
