"""Configuration for the quik_feed service (Sprint 5.8 — live candles).

Reads minute candles from a QUIK-populated file (CSV append-only OR Excel
.xlsx via openpyxl), detects new completed bars, publishes to Redis
`candles:1m` stream.

Format detection: by file extension.
  - `.csv`  → CSVTailReader (recommended, used with candle_dump.lua)
  - `.xlsx` → ExcelReader (pure DDE→Excel without Lua glue)
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root: src/services/quik_feed/config.py — parents[3]
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class QuikFeedSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Redis ---
    redis_url: str = Field(default="redis://localhost:6379")
    candles_stream: str = Field(default="candles:1m")
    candles_stream_maxlen: int = Field(default=200_000, ge=1_000)
    heartbeat_stream: str = Field(default="system:heartbeats")

    # --- Source ---
    # Default путь предполагает Lua candle_dump.lua → CSV append. Если оператор
    # настроил pure DDE→Excel — задать .xlsx путь.
    quik_feed_source_path: Path = Field(
        default=PROJECT_ROOT / "quik_live" / "candles.csv",
        description="Файл .csv (Lua glue) или .xlsx (pure DDE) с минутными свечами",
    )
    quik_feed_sheet_name: str = Field(
        default="candles",
        description="(только .xlsx) — имя листа",
    )
    quik_feed_poll_sec: int = Field(default=5, ge=1, le=60)

    # --- Bootstrap ---
    # При старте: "all" = вычитать ВЕСЬ файл и опубликовать недостающее (дедуп по
    # last_ts из candles:1m) — СВЕРКА+дозагрузка пропущенных свечей после ребута
    # (candle_dump.lua на старте truncate'ит CSV и дампит всю сегодняшнюю сессию из
    # QUIK datasource). "tail" = только новые строки (старое поведение, без сверки).
    bootstrap_mode: Literal["tail", "all"] = Field(default="all")

    # --- Screener пропущенных свечей ---
    # На старте строим МНОЖЕСТВО (ticker,ts) уже опубликованных баров за окно
    # reconcile_lookback_days и публикуем ТОЛЬКО отсутствующие (вкл. дыры в СЕРЕДИНЕ,
    # а не только в хвосте, как делал бы дедуп по max-ts). Старше окна → считаем
    # опубликованным (не сверяем). reconcile_scan_max — потолок чтения candles:1m.
    reconcile_lookback_days: int = Field(default=4, ge=1, le=60)
    reconcile_scan_max: int = Field(default=150_000, ge=10_000)

    # --- Time zone ---
    # QUIK Workstation отдаёт naive MSK timestamps; converters в Predictor/Bridge
    # ожидают naive MSK (Phase 2 convention). Менять не надо.
    candle_tz_label: str = Field(
        default="naive_msk",
        description="Документация: timestamps в file интерпретируются как naive MSK",
    )

    # --- Heartbeat / Producer ---
    heartbeat_interval_sec: int = Field(default=30)
    producer_name: str = Field(default="quik_feed")

    # --- Whitelist (для filter unrecognized symbols из QUIK file).
    # Включает 19 + cross-asset. Off-list бары silently skipped.
    accepted_tickers: List[str] = Field(
        default=[
            # Trade whitelist (12)
            "YNDX", "GAZP", "NG", "BR", "PLZL", "GMKN",
            "TATN", "MGNT", "VTBR", "NVTK", "ROSN", "LKOH",
            # Phase 2 reference + cross-asset (7)
            "SBER", "MTSS", "Si", "MX", "CNY", "USDRUB", "GOLD",
            # Canonical aliases (Sprint 4.1 registry) — на случай canonical имён в файле
            "YDEX", "SI", "MIX", "GLDRUB",
        ],
    )

    @field_validator("quik_feed_source_path")
    @classmethod
    def _validate_source_format(cls, v: Path) -> Path:
        # File не обязан существовать на старте — QUIK его создаст. Но extension должен быть знакомым.
        if v.suffix.lower() not in (".csv", ".xlsx"):
            raise ValueError(
                f"quik_feed_source_path должен быть .csv или .xlsx, получено: {v.suffix}"
            )
        return v


def load_settings() -> QuikFeedSettings:
    if not ENV_FILE.exists():
        raise FileNotFoundError(f"\n\n.env not found at: {ENV_FILE}\n")
    return QuikFeedSettings()  # type: ignore[call-arg]
