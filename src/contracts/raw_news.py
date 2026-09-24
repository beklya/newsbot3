# src/contracts/raw_news.py
from pydantic import BaseModel, Field, ConfigDict
from .base import MessageEnvelope

SCHEMA_VERSION = "1.0.0"


class RawNewsPayload(BaseModel):
    """Сырая новость из Telegram до анализа."""
    model_config = ConfigDict(extra='forbid', frozen=True)

    channel: str = Field(..., description="@interfaxonline / @rian_ru / etc.")
    message_id: int = Field(..., description="Telegram message ID")
    text: str = Field(..., min_length=1, max_length=10_000)

    # Время от Telegram-сервера (server-side)
    tg_published_at: str = Field(..., description="ISO 8601 UTC")
    # Время приёма Receiver'ом (client-side)
    received_at: str = Field(..., description="ISO 8601 UTC")

    # SHA-256 от text — для дедупликации репостов
    text_hash: str = Field(..., min_length=64, max_length=64)

    # Опционально — медиа
    has_media: bool = False
    is_reply: bool = False
    is_forward: bool = False


class RawNewsEvent(MessageEnvelope):
    schema_version: str = SCHEMA_VERSION
    producer: str = "receiver"
    payload: RawNewsPayload