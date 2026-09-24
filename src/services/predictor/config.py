"""Configuration for the Predictor service (Sprint 5.1).

Consumes EnrichedNewsEvent from news:enriched, builds 67-feature vector
per (news, ticker), runs XGBoost MFE/MAE inference, publishes
MLPredictionEvent to ml:predictions.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root: src/services/predictor/config.py — parents[3]
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class PredictorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Redis / Memurai ---
    redis_url: str = Field(default="redis://localhost:6379")
    enriched_news_stream: str = Field(default="news:enriched")
    ml_predictions_stream: str = Field(default="ml:predictions")
    ml_predictions_dlq_stream: str = Field(default="ml:predictions:dlq")
    ml_predictions_maxlen: int = Field(default=50_000, ge=1_000)
    ml_predictions_dlq_maxlen: int = Field(default=10_000, ge=100)
    heartbeat_stream: str = Field(default="system:heartbeats")

    # --- Consumer ---
    consumer_group: str = Field(default="predictor")
    consumer_name: str = Field(default="predictor-1", description="Unique per process instance")
    consumer_block_ms: int = Field(default=5000)

    # --- Idempotency ---
    idempotency_scope: str = Field(default="ml_prediction")
    idempotency_ttl_sec: int = Field(default=86400, description="24h dedup window")

    # --- Heartbeat ---
    heartbeat_interval_sec: int = Field(default=30)

    # --- Producer identity ---
    producer_name: str = Field(default="predictor")

    # --- Models ---
    models_dir: Path = Field(
        default=PROJECT_ROOT / "data" / "models" / "predictor" / "v7_70b_v2",
        description=(
            "Каталог с {target}_{model_type}.joblib моделями + feature_order.json. "
            "Sprint 6.2 deploy: v7_70b_v2 trained on Y6 extended corpus "
            "(137k events Phase2 + Y6 2025-2026, all 4 channels, 78 features). "
            "Walk-forward 19 folds: Mean Sharpe +1.21, 16/19 positive PnL, "
            "+3.34M RUB total. See docs/SPRINT_6_1_DONE.md."
        ),
    )
    model_version: str = Field(
        default="xgb_v7_y6_extended_2026_06_07",
        description=(
            "MLPredictionPayload.model_version. Sprint 6.2: v7_70b_v2. "
            "Previous v1 (legacy 8B-trained) backed up in predictor/v1_legacy. "
            "Use rr_threshold=1.0, min_mfe_pct=0.0 in Decision (Phase 2's 2.0/0.15 "
            "calibration is for v1 magnitudes; v7 predicts roughly half-scale). "
            "Recommended provider: DeepInfra (matches training distribution)."
        ),
    )

    # --- Candle cache ---
    prices_dir: Path = Field(
        default=PROJECT_ROOT / "data" / "prices",
        description="Каталог с prices_{TICKER}.csv (19 файлов)",
    )

    # --- Live candles (Sprint 5.8) ---
    # При enabled=True Predictor подписывается на `candles:1m` stream
    # (производит quik_feed service) и поверх historical CSV
    # накатывает live bars from QUIK Workstation.
    live_candles_enabled: bool = Field(default=True)
    live_candles_stream: str = Field(default="candles:1m")

    # --- News history (для feature_builder) ---
    news_history_lookback_hours: int = Field(default=24, ge=1, le=72)
    news_history_per_ticker_maxlen: int = Field(default=50, ge=10, le=200)

    # --- Sprint 6: stale-news gate ---
    # Reject predictions where the news arrived in a gap of the candle cache
    # (e.g., enricher was down for a while, news_time precedes available live
    # bars but is hours after the latest CSV bar). On 2026-06-01 we hit this
    # exact case: Predictor used April-20 close=125.83 as last_close while
    # Bridge filled on today's 116.32 bar => catastrophic inverted SL/TP.
    #
    # Gap = ref_msk(news_time) - index_of(last_bar <= news_time).
    # If gap > N seconds, skip prediction entirely. Decision will see no
    # ml:predictions for that (news, ticker) and the trade never opens.
    max_news_to_last_bar_gap_sec: int = Field(
        default=1800, ge=60,
        description="Skip prediction if news_time - last_bar before news_time > N sec (Conservative=1800s/30min)",
    )

    # --- Whitelist (Sprint 5.1: 12 тикеров из Phase 2 winning subset).
    # Off-whitelist tickers тихо пропускаются с инкрементом counter'а.
    # Это единственный source of truth — Decision не дублирует фильтр.
    whitelist_tickers: List[str] = Field(
        default=[
            "YNDX", "GAZP", "NG", "BR", "PLZL", "GMKN",
            "TATN", "MGNT", "VTBR", "NVTK", "ROSN", "LKOH",
        ],
    )

    # --- Fail-fast validators ---
    @field_validator("models_dir")
    @classmethod
    def _models_dir_exists(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(
                f"Models dir not found: {v}\n"
                f"Run first: python scripts/train_predictor_fold13.py"
            )
        return v

    @field_validator("prices_dir")
    @classmethod
    def _prices_dir_exists(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(
                f"Prices dir not found: {v}\n"
                f"Ожидается 19 файлов prices_{{TICKER}}.csv от Phase 2"
            )
        return v


def load_settings() -> PredictorSettings:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env not found at: {ENV_FILE}\n"
            f"Quick fix from project root:\n"
            f"    copy .env.example .env\n"
        )
    return PredictorSettings()  # type: ignore[call-arg]
