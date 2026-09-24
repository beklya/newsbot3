"""Configuration for the Decision service (Sprint 5.2).

Consumes MLPredictionEvent from ml:predictions, looks up cached
EnrichedNewsEvent (via Redis key `enriched:<id>` populated by Enricher),
applies B_direction_filter + Phase 2 R:R logic + RiskManager,
publishes TradeSignalEvent v1.0.1 to trade:signals.

Parameter defaults reflect PHASE2.md §7.1 winning configuration:
  horizon=60m × RR≥2.0 × MIN_MFE_PCT=0.15 × MIN_CONFIDENCE=0.55.
Risk per trade 0.5% (paper-analog Phase 2 backtest).
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class DecisionSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Redis ---
    redis_url: str = Field(default="redis://localhost:6379")
    ml_predictions_stream: str = Field(default="ml:predictions")
    trade_signals_stream: str = Field(default="trade:signals")
    trade_signals_maxlen: int = Field(default=50_000, ge=1_000)
    heartbeat_stream: str = Field(default="system:heartbeats")

    # --- Enrichment cache (filled by Enricher 5.4 — SETEX enriched:<id>) ---
    enrichment_cache_key_prefix: str = Field(default="enriched:")
    enrichment_cache_ttl_sec: int = Field(default=300)

    # --- Consumer ---
    consumer_group: str = Field(default="decision")
    consumer_name: str = Field(default="decision-1")
    consumer_block_ms: int = Field(default=5000)

    # --- Idempotency ---
    idempotency_scope: str = Field(default="trade_signal")
    idempotency_ttl_sec: int = Field(default=86400)

    # --- Heartbeat ---
    heartbeat_interval_sec: int = Field(default=30)
    producer_name: str = Field(default="decision")

    # === Sprint 6.2 decision params (v7 calibrated; see docs/SPRINT_6_1_DONE.md) ===
    # Phase 2 defaults (rr=2.0, min_mfe=0.15) were tuned for v1_legacy magnitudes.
    # v7 trained on extended corpus predicts ~half-scale MFE/MAE; equivalent gate is
    # rr=1.0, min_mfe=0.0. Walk-forward 19 folds @ rr=1.0: Mean Sharpe +1.21,
    # 16/19 positive PnL folds, +3.34M total. Live VPS replay v7+DI swap: +4.11 Sharpe.
    horizon_min: int = Field(default=60, description="Используем 30m или 60m predictions из MLPredictionEvent")
    rr_threshold: float = Field(default=1.0, ge=0.5, le=10.0)
    min_mfe_pct: float = Field(default=0.0, ge=0.0)
    min_mae_pct: float = Field(default=0.05, ge=0.0, description="Floor для R:R деления")
    min_confidence: float = Field(default=0.55, ge=0.0, le=1.0, description="LLM confidence на per-ticker")
    direction_filter_min_confidence: float = Field(
        default=0.5, description="Sprint 4 B-filter threshold (отдельный от MIN_CONFIDENCE)",
    )

    # === Risk management (PHASE2 §7.1) ===
    initial_equity_rub: float = Field(default=500_000.0, gt=0.0)
    risk_per_trade_pct: float = Field(default=0.005, gt=0.0, le=0.1)
    leverage: int = Field(default=10, ge=1, le=20)
    max_open_positions: int = Field(default=3, ge=1, le=20)
    daily_kill_pct: float = Field(default=0.02, gt=0.0)
    cooldown_ticker_sec: int = Field(default=60, ge=0)

    # === SL/TP formula (PHASE2 §5.3) ===
    tp_fraction: float = Field(default=0.7, gt=0.0, le=1.0)
    sl_buffer: float = Field(default=1.2, gt=0.0)
    sl_floor_pct: float = Field(default=0.0005, description="Минимум SL distance 0.05%")
    tp_floor_pct: float = Field(default=0.001, description="Минимум TP distance 0.10%")

    # === Market hours gate (Sprint 6) ===
    # Drop signals fired outside MOEX trading hours. Reduces noise from overnight
    # news + skips weekend distribution-shift risk (Phase 2 backtest = weekday only).
    market_hours_enabled: bool = Field(
        default=True,
        description="If True, REJECT signals fired outside MOEX session hours.",
    )
    market_hours_skip_weekends: bool = Field(
        default=True,
        description="If True, Sat/Sun are closed regardless of MOEX weekend sessions.",
    )

    # === Stale-features gate (Sprint 6) ===
    # Belt-and-braces over Predictor's stale_candles_at_news_time gate. If the
    # MLPrediction reports last_bar_time too far before news_time (cache gap),
    # REJECT before R:R / sizing. Catches the case where Predictor was run with
    # an older code path that didn't filter.
    stale_features_max_gap_sec: int = Field(
        default=1800, ge=0,
        description="REJECT signal if news_time - last_bar_time > N sec (0 = disabled, default 1800 = 30 min).",
    )

    # === Lot sizes (PHASE2 §2.3) — used for sizing without going to QUIK ===
    lot_sizes: dict[str, int] = Field(
        default_factory=lambda: {
            "SBER": 10, "GAZP": 10, "LKOH": 1, "YDEX": 1, "ROSN": 10,
            "GMKN": 1, "NVTK": 1, "TATN": 1, "MGNT": 1, "MTSS": 10,
            "PLZL": 1, "VTBR": 10000,
            "SI": 1, "MIX": 1, "BR": 1, "NG": 1, "GLDRUB": 1, "CNY": 1,
            "USDRUB": 1000,
        }
    )

    # === Redis state keys ===
    risk_open_positions_key: str = Field(default="risk:open_positions")
    risk_daily_pnl_key_prefix: str = Field(default="risk:daily_pnl:")
    risk_cooldown_key_prefix: str = Field(default="risk:cooldown:")


def load_settings() -> DecisionSettings:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env not found at: {ENV_FILE}\n"
            f"Quick fix from project root:\n"
            f"    copy .env.example .env\n"
        )
    return DecisionSettings()  # type: ignore[call-arg]
