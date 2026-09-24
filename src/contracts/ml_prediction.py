# src/contracts/ml_prediction.py
from typing import Literal
from pydantic import BaseModel, Field, ConfigDict
from .base import MessageEnvelope

SCHEMA_VERSION = "1.0.0"

Horizon = Literal["30m", "60m"]


class MLPredictionPerHorizon(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    
    horizon: Horizon
    
    # Long-сторона
    predicted_mfe_long_pct: float
    predicted_mae_long_pct: float
    
    # Short-сторона
    predicted_mfe_short_pct: float
    predicted_mae_short_pct: float
    
    # R:R рассчитанный
    rr_long: float = Field(..., description="MFE_long / MAE_long")
    rr_short: float = Field(..., description="MFE_short / MAE_short")


class MLPredictionPayload(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    enriched_event_id: str
    ticker: str

    # Время сборки фичей
    features_built_at: str
    features_hash: str = Field(..., description="SHA-256 от feature vector — для drift detection")
    feature_count: int = Field(..., ge=67, le=80,
        description="67 Phase 2 features OR 77 with Sprint 6.1 Y4 70B-only extension")

    # Sprint 6: news_time — оригинальное время новости (для honest historical replay).
    # Optional + default None для backward compat. Если None — downstream services
    # используют envelope.produced_at как раньше.
    news_time: str | None = Field(
        default=None,
        description="UTC ISO 8601 — original news time (propagated from EnrichedNewsPayload.tg_published_at).",
    )

    # Live OHLCV reference
    last_bar_time: str = Field(..., description="Время последней использованной 1m-свечи")
    last_close: float

    predictions: list[MLPredictionPerHorizon] = Field(..., min_length=1, max_length=2)

    # Метаданные
    inference_latency_ms: float
    model_version: str = Field(..., description="Хеш модели — для отслеживания, какая версия делала предсказание")


class MLPredictionEvent(MessageEnvelope):
    schema_version: str = SCHEMA_VERSION
    producer: str = "predictor"
    payload: MLPredictionPayload