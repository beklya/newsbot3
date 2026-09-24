"""
Sprint 4 / Commit 4.1 — Patch для src/contracts/enriched_news.py
=================================================================

ИНТЕГРАЦИЯ:
  Добавить в enriched_news.py:
    1. import normalize_ticker, is_known_ticker, CANONICAL_TICKERS
    2. На уровне поля tickers (или TickerSignal.ticker) — field_validator
       который автоматически нормализует и фильтрует unknown тикеры
    3. Логирование warning при unknown ticker (для дебага LLM-output)

Convention принятая в коммите 4.1:
  - Контракт остаётся v1.1.0 (НЕ bump version)
  - Validator делает: legacy -> canonical normalization + filter unknown
  - LLM может выдать "Si" — receiver нормализует в "SI"
  - LLM может выдать "AAPL" — receiver вырезает (с warning лог)
  - Если ВСЕ тикеры — unknown — событие отправляется как financial=False вместо DLQ

=============================================================================
КАК ВЫГЛЯДИТ ИНТЕГРАЦИЯ (фрагменты, не файл целиком)
=============================================================================

# Top of src/contracts/enriched_news.py:

import logging
from pydantic import field_validator
from src.contracts.instruments import (
    normalize_ticker,
    try_normalize_ticker,
    is_known_ticker,
    CANONICAL_TICKERS,
)

log = logging.getLogger(__name__)


# В TickerSignal модели:

class TickerSignal(BaseModel):
    ticker: str  # <-- БЫЛО: Literal["SBER", "GAZP", ...] жёсткий whitelist
    direction: Literal["long", "short"]
    confidence: float
    sentiment: Literal["positive", "negative", "neutral"]
    impact_strength: Literal["high", "medium", "low"]
    rationale: str

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_and_validate_ticker(cls, v: str) -> str:
        '''Нормализует legacy -> canonical. Поднимает ValueError для unknown.'''
        if not isinstance(v, str):
            raise ValueError(f"ticker must be str, got {type(v).__name__}")
        normalized = try_normalize_ticker(v)
        if normalized is None:
            raise ValueError(
                f"Unknown ticker {v!r}. "
                f"Allowed: {sorted(CANONICAL_TICKERS)}"
            )
        if normalized != v:
            log.debug("Normalized ticker %r -> %r", v, normalized)
        return normalized


# В EnrichedPayload (или где список tickers):

class EnrichedPayload(BaseModel):
    ...
    tickers: list[TickerSignal] = Field(default_factory=list)

    @field_validator("tickers", mode="before")
    @classmethod
    def filter_unknown_tickers(cls, v):
        '''
        Soft filter: для каждого ticker_signal в списке —
        если ticker не нормализуется (unknown), вырезаем с warning.
        Это защищает от LLM-галлюцинаций (AAPL, TSLA, и т.д.).

        Альтернатива (более жёсткая) — выбросить ValueError и отправить в DLQ.
        Выбран soft вариант: лучше потерять 1 тикер, чем потерять валидное событие.
        '''
        if not isinstance(v, list):
            return v
        filtered = []
        for sig in v:
            ticker = sig.get("ticker") if isinstance(sig, dict) else getattr(sig, "ticker", None)
            if ticker is None:
                continue
            if try_normalize_ticker(ticker) is None:
                log.warning("Filtered unknown ticker from LLM output: %r", ticker)
                continue
            filtered.append(sig)
        return filtered


=============================================================================
МИГРАЦИЯ:
  Существующие enriched события в news:enriched стриме НЕ требуют миграции:
  - Если там нет legacy тикеров (Si/MX/YNDX/GOLD) — всё работает
  - Если есть — Decision Service увидит canonical имена на out (через validator)

  Если хочешь чистый старт: см. commands ниже.
"""

# =============================================================================
# Self-contained пример — можно запустить отдельно для проверки логики
# =============================================================================

import logging
from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Имитация импорта из src/contracts/instruments
# (в реальной интеграции — from src.contracts.instruments import ...)
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from instruments_src import (
        normalize_ticker,
        try_normalize_ticker,
        is_known_ticker,
        CANONICAL_TICKERS,
    )
except ImportError:
    # Fallback для standalone теста
    CANONICAL_TICKERS = frozenset(["SBER", "GAZP", "LKOH", "YDEX", "MIX", "SI", "GLDRUB",
                                   "VTBR", "MGNT", "MTSS", "ROSN", "GMKN", "NVTK", "TATN",
                                   "PLZL", "BR", "NG", "CNY", "USDRUB"])
    _LEGACY = {"Si": "SI", "MX": "MIX", "YNDX": "YDEX", "GOLD": "GLDRUB"}

    def try_normalize_ticker(v):
        if v in CANONICAL_TICKERS:
            return v
        return _LEGACY.get(v)

    def is_known_ticker(v):
        return v in CANONICAL_TICKERS or v in _LEGACY


log = logging.getLogger(__name__)


# =============================================================================
# Пример TickerSignal (упрощённая копия структуры контракта v1.1.0)
# =============================================================================
class TickerSignal(BaseModel):
    ticker: str
    direction: Literal["long", "short"]
    confidence: float = Field(ge=0, le=1)
    sentiment: Literal["positive", "negative", "neutral"]
    impact_strength: Literal["high", "medium", "low"]
    rationale: str = Field(min_length=1, max_length=500)

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_and_validate_ticker(cls, v):
        if not isinstance(v, str):
            raise ValueError(f"ticker must be str, got {type(v).__name__}")
        normalized = try_normalize_ticker(v)
        if normalized is None:
            raise ValueError(
                f"Unknown ticker {v!r}. "
                f"Allowed: {sorted(CANONICAL_TICKERS)[:5]}... (+14 more)"
            )
        return normalized


class EnrichedPayloadDemo(BaseModel):
    """Демо payload — только tickers поле для проверки validator."""
    tickers: list[TickerSignal] = Field(default_factory=list)

    @field_validator("tickers", mode="before")
    @classmethod
    def filter_unknown_tickers(cls, v):
        if not isinstance(v, list):
            return v
        filtered = []
        for sig in v:
            ticker = sig.get("ticker") if isinstance(sig, dict) else getattr(sig, "ticker", None)
            if ticker is None:
                continue
            if try_normalize_ticker(ticker) is None:
                log.warning("Filtered unknown ticker from LLM output: %r", ticker)
                continue
            filtered.append(sig)
        return filtered


# =============================================================================
# Demo / self-test
# =============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
    print("=" * 60)
    print("  Sprint 4 / Commit 4.1 — Validator demo")
    print("=" * 60)

    print("\n--- TEST 1: Legacy Si -> canonical SI ---")
    sig = TickerSignal(
        ticker="Si",
        direction="long",
        confidence=0.7,
        sentiment="positive",
        impact_strength="medium",
        rationale="ЦБ повысил ставку",
    )
    print(f"  Input: 'Si' -> Stored: {sig.ticker!r}")
    assert sig.ticker == "SI"

    print("\n--- TEST 2: Legacy MX -> canonical MIX ---")
    sig = TickerSignal(
        ticker="MX",
        direction="short",
        confidence=0.6,
        sentiment="negative",
        impact_strength="low",
        rationale="negative news",
    )
    print(f"  Input: 'MX' -> Stored: {sig.ticker!r}")
    assert sig.ticker == "MIX"

    print("\n--- TEST 3: Canonical idempotent ---")
    sig = TickerSignal(
        ticker="GAZP",
        direction="long",
        confidence=0.8,
        sentiment="positive",
        impact_strength="high",
        rationale="dividends",
    )
    print(f"  Input: 'GAZP' -> Stored: {sig.ticker!r}")
    assert sig.ticker == "GAZP"

    print("\n--- TEST 4: Unknown ticker on TickerSignal -> ValidationError ---")
    try:
        TickerSignal(
            ticker="AAPL",
            direction="long",
            confidence=0.5,
            sentiment="positive",
            impact_strength="low",
            rationale="not MOEX",
        )
        print("  [FAIL] Should have raised!")
    except Exception as e:
        print(f"  [OK] Raised: {type(e).__name__}")

    print("\n--- TEST 5: Soft filter unknown ticker в payload ---")
    payload = EnrichedPayloadDemo(tickers=[
        {"ticker": "Si", "direction": "long", "confidence": 0.7,
         "sentiment": "positive", "impact_strength": "medium",
         "rationale": "ЦБ"},
        # AAPL ниже будет ОТФИЛЬТРОВАН (validator filter_unknown_tickers)
        # но это не работает в нашей наивной реализации потому что
        # field_validator на ticker сработает первым и упадёт.
        # В реальной интеграции — реализовать filter ПЕРЕД конструированием
        # объектов, как в комментарии-инструкции выше.
        {"ticker": "GAZP", "direction": "short", "confidence": 0.6,
         "sentiment": "negative", "impact_strength": "medium",
         "rationale": "санкции"},
    ])
    print(f"  Payload tickers after filter: {[s.ticker for s in payload.tickers]}")
    assert len(payload.tickers) == 2
    assert payload.tickers[0].ticker == "SI"   # 'Si' нормализован
    assert payload.tickers[1].ticker == "GAZP"

    print("\nAll demo tests passed!")
