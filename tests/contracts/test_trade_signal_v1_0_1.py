"""Tests for TradeSignalEvent v1.0.1 — REJECT-friendly contract.

Locks in the v1.0.1 contract semantics introduced в Sprint 5 / Commit 5.0:
- REJECT actions могут публиковаться с None в execute-only полях
- EXECUTE actions требуют все execute-only поля обязательно
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.contracts.trade_signal import (
    SCHEMA_VERSION,
    TradeSignalEvent,
    TradeSignalPayload,
)


def test_schema_version_bumped_to_1_0_1():
    assert SCHEMA_VERSION == "1.0.1"


def test_reject_signal_minimal_no_execute_fields_required():
    """REJECT — execute-поля Optional, может быть None."""
    payload = TradeSignalPayload(
        prediction_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        action="REJECT",
        reject_reason="confidence below threshold",
        ticker="GAZP",
        # side, horizon, entry_price, stop_loss, take_profit, quantity, risk_rub,
        # expected_pnl_rub, rr_ratio — все None
        open_positions=0,
        daily_pnl_pct=0.0,
        cooldown_active=False,
    )
    assert payload.action == "REJECT"
    assert payload.side is None
    assert payload.entry_price is None
    assert payload.quantity is None
    assert payload.rr_ratio is None


def test_execute_signal_requires_all_execute_fields():
    """EXECUTE без entry_price/side/etc должен упасть с ValidationError."""
    with pytest.raises(ValidationError) as exc:
        TradeSignalPayload(
            prediction_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
            action="EXECUTE",
            reject_reason="",
            ticker="GAZP",
            # все execute поля пропущены — должно упасть
            open_positions=0,
            daily_pnl_pct=0.0,
            cooldown_active=False,
        )
    msg = str(exc.value)
    assert "EXECUTE requires" in msg or "missing" in msg


def test_execute_signal_with_all_fields_ok():
    """EXECUTE с полным набором полей валиден."""
    payload = TradeSignalPayload(
        prediction_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        action="EXECUTE",
        reject_reason="",
        ticker="GAZP",
        side="BUY",
        horizon="60m",
        entry_price=271.5,
        stop_loss=270.2,
        take_profit=273.4,
        quantity=15,
        risk_rub=2500.0,
        expected_pnl_rub=5970.0,
        rr_ratio=1.5,  # < 2.0 — теперь это decision config, контракт пропускает
        open_positions=5,  # > 3 — теперь это decision config, контракт пропускает
        daily_pnl_pct=0.42,
        cooldown_active=False,
    )
    assert payload.action == "EXECUTE"
    assert payload.rr_ratio == 1.5
    assert payload.open_positions == 5


def test_rr_ratio_constraint_lifted():
    """rr_ratio >= 2.0 constraint снят в v1.0.1 (это теперь decision config)."""
    payload = TradeSignalPayload(
        prediction_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        action="REJECT",
        reject_reason="rr below threshold",
        ticker="GAZP",
        rr_ratio=0.5,
        open_positions=0,
        daily_pnl_pct=0.0,
        cooldown_active=False,
    )
    assert payload.rr_ratio == 0.5


def test_open_positions_le3_constraint_lifted():
    """open_positions <= 3 constraint снят в v1.0.1."""
    payload = TradeSignalPayload(
        prediction_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        action="REJECT",
        reject_reason="max positions",
        ticker="GAZP",
        open_positions=99,
        daily_pnl_pct=0.0,
        cooldown_active=False,
    )
    assert payload.open_positions == 99


def test_event_envelope_round_trips():
    """Full envelope serialize/deserialize work for both actions."""
    for action in ("EXECUTE", "REJECT"):
        kwargs = {
            "prediction_event_id": "01JABCDEFGHIJKLMNOPQRSTUVW",
            "action": action,
            "reject_reason": "" if action == "EXECUTE" else "test reject",
            "ticker": "GAZP",
            "open_positions": 0,
            "daily_pnl_pct": 0.0,
            "cooldown_active": False,
        }
        if action == "EXECUTE":
            kwargs.update({
                "side": "BUY",
                "horizon": "60m",
                "entry_price": 271.5,
                "stop_loss": 270.0,
                "take_profit": 273.0,
                "quantity": 10,
                "risk_rub": 1000.0,
                "expected_pnl_rub": 2000.0,
                "rr_ratio": 2.0,
            })
        payload = TradeSignalPayload(**kwargs)
        event = TradeSignalEvent(payload=payload)
        round_trip = TradeSignalEvent.model_validate_json(event.model_dump_json())
        assert round_trip.payload.action == action
