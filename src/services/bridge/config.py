"""Configuration for the Bridge service (Sprint 5.3 — Paper mode).

Consumes TradeSignalEvent from trade:signals (EXECUTE only), simulates
fill using candle CSV cache, tracks position bar-by-bar до SL/TP/time-stop,
publishes ExecutionResultEvent v1.0.1 OPEN + CLOSE pair.

Real QUIK Lua bridge — deferred Sprint 6.

Costs (Sprint 6.3, Sber «Самостоятельный» 14.07.2025):
  Brokerage round-trip: equities 0.14% (incl. MOEX), futures 0.03%,
                        currencies CETS (USDRUB/CNY/GLDRUB) 0.40%
  Slippage round-trip:  liquid stocks 0.04%, illiquid 0.10%,
                        liquid futures 0.02%, mid futures 0.04%, USDRUB 0.05%
                        (Phase 2 empirical, unchanged)
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class BridgeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Mode ---
    mode: str = Field(default="paper", description="paper | real (real → Sprint 6)")

    # --- Redis ---
    redis_url: str = Field(default="redis://localhost:6379")
    trade_signals_stream: str = Field(default="trade:signals")
    trade_executions_stream: str = Field(default="trade:executions")
    trade_executions_maxlen: int = Field(default=50_000, ge=1_000)
    heartbeat_stream: str = Field(default="system:heartbeats")

    # --- Consumer ---
    consumer_group: str = Field(default="bridge")
    consumer_name: str = Field(default="bridge-1")
    consumer_block_ms: int = Field(default=5000)

    # --- Idempotency ---
    idempotency_scope: str = Field(default="trade_execution")
    idempotency_ttl_sec: int = Field(default=86400)

    # --- Heartbeat ---
    heartbeat_interval_sec: int = Field(default=30)
    producer_name: str = Field(default="bridge")

    # --- Candle source ---
    prices_dir: Path = Field(default=PROJECT_ROOT / "data" / "prices")

    # --- Live candles (Sprint 5.8) ---
    # При enabled=True Bridge подписывается на `candles:1m` stream (производит
    # quik_feed). Поверх historical CSV накатываются live bars от QUIK.
    # PaperExecutor + PositionTracker используют этот candle stream для fill
    # и bar-by-bar SL/TP check.
    live_candles_enabled: bool = Field(default=True)
    live_candles_stream: str = Field(default="candles:1m")

    # --- Position tracker ---
    tracker_poll_interval_sec: float = Field(
        default=5.0, ge=0.05,
        description="Как часто проверять SL/TP/time в asyncio.Task per position",
    )

    # --- Stale-bar gate (Sprint 6) ---
    # When the last candle in cache is older than this threshold (relative to
    # signal time, NOT wall-clock), refuse to fill — likely the data feed (QUIK
    # / quik_feed) is down. Default 600s = 10 minutes; tolerant of normal jitter
    # but catches QUIK hangs without filling on stale prices.
    stale_bar_threshold_sec: int = Field(
        default=600, ge=60,
        description="Reject fill if signal_time - last_bar > N seconds",
    )

    # --- Price-drift gate (Sprint 6) ---
    # Last line of defense against Predictor/Bridge candle-cache divergence.
    # Decision computes SL/TP based on Predictor's reference price (signal.entry_price).
    # Bridge fills at next-min bar open, which may differ. If divergence > N%,
    # SL/TP no longer make sense relative to entry => SHORT can hit "TP" instantly.
    # Observed 2026-06-01: signal.entry_price=125.83 (April 20 close),
    # actual fill=116.32 (today live) => -8% drift => -21963₽ paper loss.
    max_entry_drift_pct: float = Field(
        default=0.01, ge=0.0, le=1.0,
        description="Reject fill if |entry_bar.open - signal.entry_price| / signal.entry_price > N (Conservative=0.01=1%; set to 1.0 = effectively off).",
    )

    # --- Risk state keys (matches DecisionSettings — single source of truth) ---
    risk_open_positions_key: str = Field(default="risk:open_positions")
    risk_daily_pnl_key_prefix: str = Field(default="risk:daily_pnl:")
    risk_cooldown_key_prefix: str = Field(default="risk:cooldown:")
    cooldown_ticker_sec: int = Field(default=60, description="Cooldown после CLOSE для ticker'а")

    # --- Recovery state keys ---
    bridge_open_position_prefix: str = Field(
        default="bridge:open_positions:",
        description="JSON-сериализованное состояние position для restart recovery",
    )

    # --- Real QUIK execution (Sprint 6 bridge, mode="real") ---
    # File-based transport, симметрично quik_live/candle_dump.lua:
    # Python пишет orders в orders_path (append jsonl), order_bridge.lua читает
    # и шлёт через sendTransaction; callbacks пишутся в status_path, Python poll'ит.
    quik_orders_path: Path = Field(
        default=PROJECT_ROOT / "quik_live" / "orders.jsonl")
    quik_status_path: Path = Field(
        default=PROJECT_ROOT / "quik_live" / "status.jsonl")
    quik_transmap_path: Path = Field(
        default=PROJECT_ROOT / "quik_live" / "transmap.json",
        description="event_id → trans_id, идемпотентность отправки на рестарте")
    quik_account: str = Field(default="", description="Торговый счёт QUIK (обязателен в real)")
    quik_client_code: str = Field(default="", description="Код клиента QUIK (если требуется)")
    # тикер → (класс, код); фьючерсы — актуальный контракт (см. candle_dump.lua)
    quik_class_codes: Dict[str, str] = Field(
        default_factory=lambda: {
            "SBER": "TQBR", "GAZP": "TQBR", "LKOH": "TQBR", "YDEX": "TQBR",
            "ROSN": "TQBR", "GMKN": "TQBR", "NVTK": "TQBR", "TATN": "TQBR",
            "MGNT": "TQBR", "MTSS": "TQBR", "PLZL": "TQBR", "VTBR": "TQBR",
        })
    quik_sec_codes: Dict[str, str] = Field(
        default_factory=lambda: {t: t for t in (
            "SBER GAZP LKOH YDEX ROSN GMKN NVTK TATN MGNT MTSS PLZL VTBR".split())})

    # --- Real execution safety (критично для реальных денег) ---
    max_qty_per_order: int = Field(
        default=1, ge=1,
        description="Жёсткий потолок объёма заявки (лотов). Первый запуск = 1.")
    order_fill_timeout_sec: float = Field(
        default=30.0, gt=0.0,
        description="Сколько ждать реальный fill (TRADE callback) после отправки.")
    status_poll_interval_sec: float = Field(default=0.5, gt=0.0)
    entry_order_type: str = Field(
        default="L", description="L=marketable-limit (контроль цены) | M=market")
    marketable_limit_offset_pct: float = Field(
        default=0.1, ge=0.0,
        description="Зазор лимитной цены входа от reference (%), защита от тонкого стакана.")
    kill_switch_key: str = Field(
        default="bridge:kill",
        description="Redis-флаг или файл: при наличии — мгновенный стоп отправки заявок.")
    max_orders_per_min: int = Field(default=10, ge=1)

    # --- Costs — round-trip, % ---
    # Sprint 6.3 (2026-06-10): brokerage updated from PHASE2 §2.3 estimates to
    # the real Sber «Самостоятельный» tariff (14.07.2025, tariff_self.pdf):
    #   stocks TQBR  0.06%/leg + ~0.01% MOEX  → 0.14% RT
    #   futures FORTS 0.015%/leg              → 0.03% RT
    #   currencies CETS 0.20%/leg (incl. GLDRUB/CNY — they trade on CETS) → 0.40% RT
    # Slippage stays per-ticker Phase 2 empirical (separate field below).
    brokerage_rt_pct: Dict[str, float] = Field(
        default_factory=lambda: {
            "SBER": 0.14, "GAZP": 0.14, "LKOH": 0.14, "YDEX": 0.14,
            "ROSN": 0.14, "TATN": 0.14, "GMKN": 0.14,
            "NVTK": 0.14, "VTBR": 0.14, "MGNT": 0.14, "MTSS": 0.14, "PLZL": 0.14,
            "SI": 0.03, "MIX": 0.03, "BR": 0.03, "NG": 0.03,
            "GLDRUB": 0.40, "CNY": 0.40,
            "USDRUB": 0.40,
        }
    )
    slippage_rt_pct: Dict[str, float] = Field(
        default_factory=lambda: {
            "SBER": 0.04, "GAZP": 0.04, "LKOH": 0.04,
            "YDEX": 0.06, "ROSN": 0.06, "TATN": 0.06, "GMKN": 0.06,
            "NVTK": 0.10, "VTBR": 0.10, "MGNT": 0.10, "MTSS": 0.10, "PLZL": 0.10,
            "SI": 0.02, "MIX": 0.02, "BR": 0.02,
            "NG": 0.04, "GLDRUB": 0.04, "CNY": 0.04,
            "USDRUB": 0.05,
        }
    )
    lot_sizes: Dict[str, int] = Field(
        default_factory=lambda: {
            "SBER": 10, "GAZP": 10, "LKOH": 1, "YDEX": 1, "ROSN": 10,
            "GMKN": 1, "NVTK": 1, "TATN": 1, "MGNT": 1, "MTSS": 10,
            "PLZL": 1, "VTBR": 10000,
            "SI": 1, "MIX": 1, "BR": 1, "NG": 1, "GLDRUB": 1, "CNY": 1,
            "USDRUB": 1000,
        }
    )

    @field_validator("prices_dir")
    @classmethod
    def _prices_dir_exists(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(
                f"Prices dir not found: {v}\n"
                f"Bridge paper mode needs 19 CSV files from Phase 2."
            )
        return v


def load_settings() -> BridgeSettings:
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env not found at: {ENV_FILE}\n"
            f"Quick fix:  copy .env.example .env\n"
        )
    return BridgeSettings()  # type: ignore[call-arg]
