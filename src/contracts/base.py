# src/contracts/base.py
from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict
from ulid import ULID  # pip install python-ulid


def utcnow_iso() -> str:
    """ISO 8601 с миллисекундами в UTC."""
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


class MessageEnvelope(BaseModel):
    """
    Общий конверт всех сообщений между сервисами.
    
    event_id — сквозной идентификатор от исходной новости до сделки.
    Это позволяет восстановить полную цепочку через Redis по одному ID.
    """
    model_config = ConfigDict(extra='forbid', frozen=True)

    event_id: str = Field(
        default_factory=lambda: str(ULID()),
        description="Сквозной ID цепочки (один на новость от Receiver до Bridge)",
    )
    schema_version: str = Field(..., description="Версия схемы конкретного payload")
    producer: str = Field(..., description="Имя сервиса-источника")
    produced_at: str = Field(default_factory=utcnow_iso)
    
    # Trace — массив timestamp'ов прохождения через каждый сервис
    trace: list[dict] = Field(default_factory=list)
    
    def add_trace(self, service: str, latency_ms: float | None = None) -> "MessageEnvelope":
        """Добавить шаг в trace (immutable — возвращает новый объект)."""
        new_trace = self.trace + [{
            "service": service,
            "at": utcnow_iso(),
            "latency_ms": latency_ms,
        }]
        return self.model_copy(update={"trace": new_trace})


Severity = Literal["info", "warn", "crit"]