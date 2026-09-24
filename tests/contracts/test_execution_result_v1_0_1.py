"""Tests for ExecutionResultEvent v1.0.1 — exit fields contract.

Locks in v1.0.1 contract semantics introduced в Sprint 5 / Commit 5.0:
- Exit fields (realized_pnl_rub, exit_reason, exit_price, exit_time, duration_sec)
  все Optional — заполнены только в CLOSE event, в OPEN event = None.
- Bridge публикует два события на сделку (OPEN + CLOSE), оба валидны.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.contracts.execution_result import (
    SCHEMA_VERSION,
    ExecutionResultEvent,
    ExecutionResultPayload,
)


def test_schema_version_bumped_to_1_0_1():
    assert SCHEMA_VERSION == "1.0.1"


def test_open_event_no_exit_fields():
    """OPEN event — filled_* заполнены, exit_* = None."""
    payload = ExecutionResultPayload(
        signal_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        status="FILLED",
        filled_price=271.5,
        filled_quantity=15,
        fill_time="2026-05-25T10:00:00.000+00:00",
        bridge_latency_ms=50.0,
        quik_ack_latency_ms=10.0,
    )
    assert payload.filled_price == 271.5
    assert payload.realized_pnl_rub is None
    assert payload.exit_reason is None
    assert payload.exit_price is None
    assert payload.exit_time is None
    assert payload.duration_sec is None


def test_close_event_with_exit_fields():
    """CLOSE event — exit_* заполнены, realized_pnl_rub финальный."""
    payload = ExecutionResultPayload(
        signal_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        status="FILLED",
        filled_price=271.5,
        filled_quantity=15,
        fill_time="2026-05-25T10:00:00.000+00:00",
        realized_pnl_rub=2850.0,
        exit_reason="tp",
        exit_price=273.4,
        exit_time="2026-05-25T11:00:00.000+00:00",
        duration_sec=3600,
        bridge_latency_ms=50.0,
        quik_ack_latency_ms=10.0,
    )
    assert payload.realized_pnl_rub == 2850.0
    assert payload.exit_reason == "tp"
    assert payload.duration_sec == 3600


def test_exit_reason_literal_values():
    """exit_reason принимает только tp/sl/time/kill."""
    for reason in ("tp", "sl", "time", "kill"):
        payload = ExecutionResultPayload(
            signal_event_id="x",
            status="FILLED",
            exit_reason=reason,
            bridge_latency_ms=0.0,
            quik_ack_latency_ms=0.0,
        )
        assert payload.exit_reason == reason


def test_exit_reason_rejects_unknown():
    with pytest.raises(ValidationError):
        ExecutionResultPayload(
            signal_event_id="x",
            status="FILLED",
            exit_reason="unknown_reason",
            bridge_latency_ms=0.0,
            quik_ack_latency_ms=0.0,
        )


def test_exit_price_must_be_positive():
    with pytest.raises(ValidationError):
        ExecutionResultPayload(
            signal_event_id="x",
            status="FILLED",
            exit_price=0.0,
            bridge_latency_ms=0.0,
            quik_ack_latency_ms=0.0,
        )


def test_duration_sec_non_negative():
    with pytest.raises(ValidationError):
        ExecutionResultPayload(
            signal_event_id="x",
            status="FILLED",
            duration_sec=-1,
            bridge_latency_ms=0.0,
            quik_ack_latency_ms=0.0,
        )


def test_event_envelope_round_trips():
    """Full envelope serialize/deserialize for OPEN + CLOSE."""
    open_payload = ExecutionResultPayload(
        signal_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        status="FILLED",
        filled_price=271.5,
        filled_quantity=15,
        fill_time="2026-05-25T10:00:00.000+00:00",
        bridge_latency_ms=50.0,
        quik_ack_latency_ms=10.0,
    )
    close_payload = ExecutionResultPayload(
        signal_event_id="01JABCDEFGHIJKLMNOPQRSTUVW",
        status="FILLED",
        realized_pnl_rub=2850.0,
        exit_reason="tp",
        exit_price=273.4,
        exit_time="2026-05-25T11:00:00.000+00:00",
        duration_sec=3600,
        bridge_latency_ms=0.0,
        quik_ack_latency_ms=0.0,
    )
    for p in (open_payload, close_payload):
        event = ExecutionResultEvent(payload=p)
        round_trip = ExecutionResultEvent.model_validate_json(event.model_dump_json())
        assert round_trip.payload.signal_event_id == p.signal_event_id
