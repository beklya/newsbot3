# src/contracts/trade_signal.py
"""
TradeSignalEvent v1.0.1
=======================

Изменения vs 1.0.0 (Sprint 5 / Commit 5.0):
- Все EXECUTE-only поля сделаны Optional (default None) — чтобы REJECT events
  валидно сериализовались с пустыми entry/sl/tp/quantity полями. REJECT нужны
  для post-mortem analytics в news:enriched → trade:signals → analysis pipeline.
- Снят constraint `rr_ratio >= 2.0` — теперь это decision-config threshold,
  не contract invariant (Sprint 5+ может экспериментировать с RR_THRESHOLD).
- Снят constraint `open_positions <= 3` — MAX_OPEN_POSITIONS теперь decision-config.
- Добавлен model_validator: if action == "EXECUTE" → все execute-поля обязаны быть не None.

Migration:
- BREAKING для строгих consumer'ов 1.0.0, NO_OP для нас: Sprint 4 ничего
  не публиковал в trade:signals → нечего дренировать. Stream впервые
  заполнится Sprint 5.2.
"""
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict, model_validator
from .base import MessageEnvelope

SCHEMA_VERSION = "1.0.1"

Side = Literal["BUY", "SELL"]
Action = Literal["EXECUTE", "REJECT"]


class TradeSignalPayload(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    prediction_event_id: str

    action: Action  # REJECT тоже логируется (для пост-анализа)
    reject_reason: str = Field("", max_length=200)

    # Sprint 6: news_time — для honest historical replay. Bridge использует
    # это поле (если установлено) для lookup candles на момент новости вместо
    # signal.produced_at. Optional + default None для backward compat.
    news_time: str | None = Field(
        default=None,
        description="UTC ISO 8601 — original news time (propagated from MLPredictionPayload.news_time).",
    )

    # Параметры сделки (заполнены только если action == EXECUTE)
    ticker: str
    side: Side | None = None
    horizon: Literal["30m", "60m"] | None = None

    entry_price: float | None = Field(default=None, gt=0.0, description="Limit price (last_close + small offset)")
    stop_loss: float | None = Field(default=None, gt=0.0)
    take_profit: float | None = Field(default=None, gt=0.0)

    # Risk
    quantity: int | None = Field(default=None, ge=1, description="Лоты для фьючерсов / штуки для акций")
    risk_rub: float | None = Field(default=None, ge=0.0, description="Сколько рублей на риске")
    expected_pnl_rub: float | None = Field(default=None, description="Ожидаемая прибыль при достижении TP")
    rr_ratio: float | None = Field(default=None, ge=0.0, description="MFE/MAE — фактический threshold живёт в Decision config")

    # Risk gates state на момент решения (полезно даже для REJECT — context)
    open_positions: int = Field(..., ge=0, description="MAX_OPEN_POSITIONS — Decision config, не contract invariant")
    daily_pnl_pct: float
    cooldown_active: bool

    @model_validator(mode="after")
    def _require_execute_fields(self) -> "TradeSignalPayload":
        """Если action == EXECUTE — все execute-поля обязаны быть не None."""
        if self.action == "EXECUTE":
            missing = [
                name for name in (
                    "side", "horizon",
                    "entry_price", "stop_loss", "take_profit",
                    "quantity", "risk_rub", "expected_pnl_rub", "rr_ratio",
                )
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(
                    f"action=EXECUTE requires non-None fields, missing: {missing}"
                )
        return self


class TradeSignalEvent(MessageEnvelope):
    schema_version: str = SCHEMA_VERSION
    producer: str = "decision"
    payload: TradeSignalPayload
