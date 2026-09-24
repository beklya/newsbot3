# src/contracts/execution_result.py
"""
ExecutionResultEvent v1.0.1
============================

Изменения vs 1.0.0 (Sprint 5 / Commit 5.0):
- Добавлены exit fields (Optional, default None):
    realized_pnl_rub, exit_reason, exit_price, exit_time, duration_sec
- Bridge публикует ДВА события на одну сделку (Sprint 5.3 design):
    1. OPEN: status=FILLED, filled_* заполнены, exit_* = None
    2. CLOSE: тот же signal_event_id (новый event_id), exit_* заполнены,
       realized_pnl_rub финальный
- exit_reason values matches sprint4/exits/baseline.py exit reasons.

Migration:
- BREAKING для строгих consumer'ов 1.0.0, NO_OP для нас: Sprint 4 ничего
  не публиковал в trade:executions.
"""
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict
from .base import MessageEnvelope

SCHEMA_VERSION = "1.0.1"

Status = Literal["FILLED", "PARTIAL", "REJECTED", "TIMEOUT", "ERROR"]
ExitReason = Literal["tp", "sl", "time", "kill"]


class ExecutionResultPayload(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    signal_event_id: str
    status: Status

    # QUIK details
    trans_id: int | None = None
    order_num: int | None = None
    error_message: str = ""

    # Реальное исполнение (entry side)
    filled_price: float | None = None
    filled_quantity: int | None = None
    fill_time: str | None = None

    # Stop / TP заявки (отдельные)
    stop_order_num: int | None = None
    tp_order_num: int | None = None

    # === Exit fields (v1.0.1, заполнены в CLOSE event) ===
    realized_pnl_rub: float | None = Field(
        default=None,
        description="Net PnL после costs. None для OPEN event, заполнено для CLOSE event.",
    )
    exit_reason: ExitReason | None = Field(
        default=None,
        description="Причина выхода: tp/sl/time/kill. None для OPEN event.",
    )
    exit_price: float | None = Field(
        default=None,
        gt=0.0,
        description="Фактическая цена выхода. None для OPEN event.",
    )
    exit_time: str | None = Field(
        default=None,
        description="ISO timestamp выхода. None для OPEN event.",
    )
    duration_sec: int | None = Field(
        default=None,
        ge=0,
        description="Длительность позиции в секундах. None для OPEN event.",
    )

    # Замеры
    bridge_latency_ms: float
    quik_ack_latency_ms: float


class ExecutionResultEvent(MessageEnvelope):
    schema_version: str = SCHEMA_VERSION
    producer: str = "bridge"
    payload: ExecutionResultPayload
