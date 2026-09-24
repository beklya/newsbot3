"""Configuration for the Monitor service (Sprint 5.4).

Stateless aggregator over `system:heartbeats` + DLQ stream + RiskManager state.
Emits log.warning / log.error alerts on:
  - missing_heartbeat — service не публиковал heartbeat > missing_threshold_sec
  - dlq_rate_spike    — XLEN delta > dlq_spike_pct за dlq_window_sec
  - daily_pnl_kill    — |daily_pnl_pct| ≥ kill_pct (kills cascaded across services)

Sprint 5.4: log only. Telegram alerts через Receiver telethon session — Sprint 6.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class MonitorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Redis ---
    redis_url: str = Field(default="redis://localhost:6379")
    heartbeat_stream: str = Field(default="system:heartbeats")
    dlq_streams: List[str] = Field(
        default_factory=lambda: [
            "news:enriched:dlq",
            "ml:predictions:dlq",
        ],
    )
    producer_name: str = Field(default="monitor")

    # --- Tracked services (для missing_heartbeat alert) ---
    tracked_services: List[str] = Field(
        default_factory=lambda: [
            "receiver", "enricher", "predictor", "decision", "bridge", "quik_feed",
        ],
    )

    # --- Thresholds ---
    missing_heartbeat_threshold_sec: int = Field(default=90, ge=10)

    # --- Startup grace ---
    # При cold start Monitor может видеть stale heartbeats от прошлой сессии
    # (или другие 6 сервисов ещё не успели опубликовать свой первый heartbeat).
    # В течение grace period suppressим missing_heartbeat alerts -- чтобы
    # не было false positives в первые секунды после launch.
    startup_grace_sec: int = Field(default=60, ge=0, le=600)
    dlq_window_sec: int = Field(default=300, ge=10, description="Окно для rate computation")
    dlq_spike_threshold: int = Field(
        default=10, ge=1,
        description="Абсолютный прирост XLEN за window_sec для alert",
    )

    # --- Risk gates (used for kill alert) ---
    initial_equity_rub: float = Field(default=500_000.0)
    daily_kill_pct: float = Field(default=0.02)
    risk_daily_pnl_key_prefix: str = Field(default="risk:daily_pnl:")

    # --- Loop tick ---
    poll_interval_sec: int = Field(default=30, ge=5, le=300)

    # --- Heartbeat ---
    heartbeat_interval_sec: int = Field(default=30)


def load_settings() -> MonitorSettings:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env not found at: {ENV_FILE}\n"
            f"Quick fix:  copy .env.example .env\n"
        )
    return MonitorSettings()  # type: ignore[call-arg]
