"""
scripts/generate_golden_samples.py — Sprint 4 / Commit 4.1.1
=============================================================

Генерирует 5 связанных golden JSON для тестирования контрактов.

КЛЮЧЕВОЕ:
  - Все события связаны через event_id chain:
      raw.event_id -> enriched.payload.raw_event_id
      enriched.event_id -> prediction.payload.enriched_event_id
      prediction.event_id -> signal.payload.prediction_event_id
      signal.event_id -> execution.payload.signal_event_id
  - EnrichedNewsEvent — v1.1.0 (с prompt_version, is_financial, expected_timeframe)
  - Тикер GAZP — canonical, validator Sprint 4.1 пропустит без изменений
  - RawNewsPayload.text_hash — 64 символа (SHA-256 длина)

Использование:
  python scripts/generate_golden_samples.py
"""

from __future__ import annotations

# === sys.path setup ===
# При прямом запуске (python scripts/generate_golden_samples.py) Python не подхватывает
# conftest.py из корня проекта. Добавляем корень в sys.path вручную, чтобы импорты
# `from src.contracts.* import ...` работали так же, как при pytest.
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
from datetime import datetime, timezone

from ulid import ULID

from src.contracts.enriched_news import (
    EnrichedNewsEvent,
    EnrichedNewsPayload,
    TickerImpact,
)
from src.contracts.execution_result import (
    ExecutionResultEvent,
    ExecutionResultPayload,
)
from src.contracts.ml_prediction import (
    MLPredictionEvent,
    MLPredictionPayload,
    MLPredictionPerHorizon,
)
from src.contracts.raw_news import RawNewsEvent, RawNewsPayload
from src.contracts.trade_signal import TradeSignalEvent, TradeSignalPayload


GOLDEN_DIR = _PROJECT_ROOT / "tests" / "contracts" / "golden"


def _now() -> str:
    """ISO 8601 UTC с миллисекундами — формат как в base.utcnow_iso()."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _new_id() -> str:
    return str(ULID())


def main() -> None:
    print(f"Generating golden samples in:\n  {GOLDEN_DIR}")
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # 1. RawNewsEvent — стартовая точка цепочки
    # =========================================================================
    raw = RawNewsEvent(
        event_id=_new_id(),
        produced_at=_now(),
        trace=[],
        payload=RawNewsPayload(
            channel="@interfaxonline",
            message_id=12345678,
            text=(
                "Газпром объявил о рекордной квартальной прибыли: 271 млрд рублей, "
                "дивиденды 19.6 рублей на акцию выше прогноза"
            ),
            tg_published_at=_now(),
            received_at=_now(),
            # SHA-256 ровно 64 hex символа (требование контракта)
            text_hash="a3f29c4b" + "0" * 56,
            has_media=False,
            is_reply=False,
            is_forward=False,
        ),
    )
    _write(raw, "raw_news_v1.json")

    # =========================================================================
    # 2. EnrichedNewsEvent v1.1.0 — связан с raw через raw_event_id
    # =========================================================================
    enriched = EnrichedNewsEvent(
        event_id=_new_id(),
        produced_at=_now(),
        trace=[],
        payload=EnrichedNewsPayload(
            raw_event_id=raw.event_id,
            llm_provider="groq",
            llm_model="llama-3.3-70b-versatile",
            llm_latency_ms=987.5,
            llm_input_tokens=124,
            llm_output_tokens=89,
            # === v1.1.0 поля ===
            prompt_version="1.0.0",
            is_financial=True,
            expected_timeframe="short",
            urgency="medium",
            category="corporate",
            is_actionable=True,
            # === Tickers (GAZP canonical, validator оставит без изменений) ===
            tickers=[
                TickerImpact(
                    ticker="GAZP",
                    direction="long",
                    confidence=0.82,
                    sentiment="positive",
                    impact_strength=0.75,
                    rationale=(
                        "Рекордная прибыль и повышенные дивиденды — "
                        "сильный позитив для котировок в ближайший час"
                    ),
                )
            ],
            summary="GAZP: позитивная новость о квартальной прибыли и дивидендах",
            llm_raw_response=(
                '{"tickers":[{"ticker":"GAZP","direction":"long",'
                '"confidence":0.82,"sentiment":"positive",'
                '"impact_strength":0.75}]}'
            ),
        ),
    )
    _write(enriched, "enriched_news_v1.json")

    # =========================================================================
    # 3. MLPredictionEvent — связан с enriched через enriched_event_id
    # =========================================================================
    prediction = MLPredictionEvent(
        event_id=_new_id(),
        produced_at=_now(),
        trace=[],
        payload=MLPredictionPayload(
            enriched_event_id=enriched.event_id,
            ticker="GAZP",
            features_built_at=_now(),
            # SHA-256 ровно 64 hex (хеш feature vector)
            features_hash="b8e1f9d2" + "0" * 56,
            feature_count=67,  # exact constraint в контракте
            last_bar_time=_now(),
            last_close=271.45,
            predictions=[
                MLPredictionPerHorizon(
                    horizon="60m",
                    predicted_mfe_long_pct=1.12,
                    predicted_mae_long_pct=0.47,
                    predicted_mfe_short_pct=0.31,
                    predicted_mae_short_pct=0.84,
                    rr_long=2.38,
                    rr_short=0.37,
                ),
                MLPredictionPerHorizon(
                    horizon="30m",
                    predicted_mfe_long_pct=0.84,
                    predicted_mae_long_pct=0.39,
                    predicted_mfe_short_pct=0.25,
                    predicted_mae_short_pct=0.68,
                    rr_long=2.15,
                    rr_short=0.37,
                ),
            ],
            inference_latency_ms=12.4,
            model_version="xgb_mfe_mae_phase2_v1.0.0",
        ),
    )
    _write(prediction, "ml_prediction_v1.json")

    # =========================================================================
    # 4. TradeSignalEvent — связан с prediction через prediction_event_id
    # =========================================================================
    signal = TradeSignalEvent(
        event_id=_new_id(),
        produced_at=_now(),
        trace=[],
        payload=TradeSignalPayload(
            prediction_event_id=prediction.event_id,
            action="EXECUTE",
            reject_reason="",
            ticker="GAZP",
            side="BUY",
            horizon="60m",
            entry_price=271.50,
            stop_loss=270.22,
            take_profit=273.40,
            quantity=15,
            risk_rub=2500.0,
            expected_pnl_rub=5970.0,
            rr_ratio=2.38,
            # Risk gates state
            open_positions=1,
            daily_pnl_pct=0.42,
            cooldown_active=False,
        ),
    )
    _write(signal, "trade_signal_v1.json")

    # =========================================================================
    # 5. ExecutionResultEvent — связан с signal через signal_event_id
    # =========================================================================
    execution = ExecutionResultEvent(
        event_id=_new_id(),
        produced_at=_now(),
        trace=[],
        payload=ExecutionResultPayload(
            signal_event_id=signal.event_id,
            status="FILLED",
            trans_id=1_000_001,
            order_num=987654321,
            error_message="",
            filled_price=271.55,
            filled_quantity=15,
            fill_time=_now(),
            stop_order_num=987654322,
            tp_order_num=987654323,
            bridge_latency_ms=247.0,
            quik_ack_latency_ms=98.5,
        ),
    )
    _write(execution, "execution_result_v1.json")

    print()
    print("Done. Generated chain:")
    print(f"  raw       = {raw.event_id}")
    print(f"  enriched  = {enriched.event_id}  (raw_event_id={enriched.payload.raw_event_id})")
    print(f"  predict   = {prediction.event_id}  (enriched_event_id={prediction.payload.enriched_event_id})")
    print(f"  signal    = {signal.event_id}  (prediction_event_id={signal.payload.prediction_event_id})")
    print(f"  execution = {execution.event_id}  (signal_event_id={execution.payload.signal_event_id})")


def _write(event, filename: str) -> None:
    path = GOLDEN_DIR / filename
    data = event.model_dump(mode="json")
    text = json.dumps(data, ensure_ascii=False, indent=2)
    path.write_text(text, encoding="utf-8")
    size = path.stat().st_size
    print(f"  [OK] {filename:<30s} {size:>5d} bytes")


if __name__ == "__main__":
    main()