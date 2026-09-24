"""
src/contracts/instruments.py — Реестр инструментов (Sprint 4 / Commit 4.1)
============================================================================

Single source of truth для всех тикер-mappings, lot_size и asset metadata.

Используется:
  - EnrichedNewsEvent.tickers — validator нормализует legacy имена в canonical
  - TradeSignalEvent.instrument — валидация перед отправкой в Bridge
  - Backtest engine — PnL расчёты с правильным lot_size
  - Phase 3 Bridge — отправка ордеров в QUIK (CLASS_CODE + SEC_CODE)

Источники истины:
  - lot_size: Phase 2 backtest_mfe.py:108-114 (LOT_SIZES dict)
  - asset_class: спецификации MOEX
  - is_usd_denominated: BR/NG/GLDRUB — фьючерсы на USD-номинированные базовые активы

Convention:
  - canonical name = имя как в prices/*.csv и как в MOEX terminal
  - legacy_phase2 = имя в Phase 2 trades.parquet (отличается у 4 тикеров)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class InstrumentMeta:
    """Метаданные одного торгуемого инструмента."""
    canonical: str
    legacy_phase2: str
    csv_file: str
    asset_class: str            # equity / futures / commodity / currency
    is_usd_denominated: bool    # PnL в долларах (BR/NG/GLDRUB)
    lot_size: int               # Контрактный множитель (Phase 2 LOT_SIZES)
    available_from: Optional[date]
    notes: str = ""


INSTRUMENTS: dict[str, InstrumentMeta] = {
    # ── Equity lot_size=10 ──────────────────────────────────────────────
    "SBER":   InstrumentMeta("SBER", "SBER", "prices_SBER.csv", "equity", False, 10, None),
    "GAZP":   InstrumentMeta("GAZP", "GAZP", "prices_GAZP.csv", "equity", False, 10, None),
    "ROSN":   InstrumentMeta("ROSN", "ROSN", "prices_ROSN.csv", "equity", False, 10, None),
    "MTSS":   InstrumentMeta("MTSS", "MTSS", "prices_MTSS.csv", "equity", False, 10, None),

    # ── Equity lot_size=1 ───────────────────────────────────────────────
    "LKOH":   InstrumentMeta("LKOH", "LKOH", "prices_LKOH.csv", "equity", False, 1, None),
    "GMKN":   InstrumentMeta("GMKN", "GMKN", "prices_GMKN.csv", "equity", False, 1, None),
    "NVTK":   InstrumentMeta("NVTK", "NVTK", "prices_NVTK.csv", "equity", False, 1, None),
    "TATN":   InstrumentMeta("TATN", "TATN", "prices_TATN.csv", "equity", False, 1, None),
    "MGNT":   InstrumentMeta("MGNT", "MGNT", "prices_MGNT.csv", "equity", False, 1, None),
    "PLZL":   InstrumentMeta("PLZL", "PLZL", "prices_PLZL.csv", "equity", False, 1, None),
    "YDEX":   InstrumentMeta(
        "YDEX", "YNDX", "prices_YDEX.csv", "equity", False, 1,
        available_from=date(2024, 7, 24),
        notes="Reorganization 2024: Yandex N.V. -> Yandex (MOEX)."
    ),

    # ── VTBR lot=10000 ──────────────────────────────────────────────────
    "VTBR":   InstrumentMeta(
        "VTBR", "VTBR", "prices_VTBR.csv", "equity", False, 10000, None,
        notes="VTBR lot_size=10000 (особенность MOEX)."
    ),

    # ── Rouble-denominated futures lot=1 ────────────────────────────────
    "MIX":    InstrumentMeta(
        "MIX", "MX", "prices_MIX.csv", "futures", False, 1, None,
        notes="Phase 2 legacy: MX. Фьючерс на индекс Мосбиржи."
    ),
    "SI":     InstrumentMeta(
        "SI", "Si", "prices_SI.csv", "futures", False, 1, None,
        notes="Phase 2 legacy: 'Si' (case-sensitive!)."
    ),
    "CNY":    InstrumentMeta("CNY", "CNY", "prices_CNY.csv", "currency", False, 1, None),

    # ── USD-denominated futures (PSEUDO-RUB в Phase 2) ──────────────────
    "BR":     InstrumentMeta(
        "BR", "BR", "prices_BR.csv", "futures", True, 1, None,
        notes="Brent. Phase 2 PnL в долларах."
    ),
    "NG":     InstrumentMeta(
        "NG", "NG", "prices_NG.csv", "futures", True, 1, None,
        notes="NatGas. Phase 2 PnL в долларах."
    ),
    "GLDRUB": InstrumentMeta(
        "GLDRUB", "GOLD", "prices_GLDRUB.csv", "commodity", True, 1,
        available_from=date(2023, 7, 12),
        notes="Phase 2 legacy: GOLD. Появился 2023-07-12."
    ),

    # ── USDRUB lot=1000 ─────────────────────────────────────────────────
    "USDRUB": InstrumentMeta(
        "USDRUB", "USDRUB", "prices_USDRUB.csv", "currency", False, 1000, None,
        notes="lot_size=1000 (необычно для валютной пары)."
    ),
}

# Reverse mapping для legacy -> canonical
_LEGACY_TO_CANONICAL: dict[str, str] = {
    meta.legacy_phase2: canonical
    for canonical, meta in INSTRUMENTS.items()
}

# Множества для быстрых проверок
CANONICAL_TICKERS: frozenset[str] = frozenset(INSTRUMENTS.keys())
ALL_KNOWN_NAMES: frozenset[str] = frozenset(INSTRUMENTS.keys()) | frozenset(_LEGACY_TO_CANONICAL.keys())


# =============================================================================
# Public API
# =============================================================================
def normalize_ticker(ticker: str) -> str:
    """
    Приводит тикер к каноническому имени.

    Принимает:
      - canonical имя ("SI", "MIX", "YDEX", "GLDRUB") — возвращает как есть
      - legacy Phase 2 имя ("Si", "MX", "YNDX", "GOLD") — конвертит в canonical

    Поднимает KeyError если тикер не известен.

    Примеры:
      normalize_ticker("Si")    -> "SI"
      normalize_ticker("MX")    -> "MIX"
      normalize_ticker("GAZP")  -> "GAZP"
      normalize_ticker("XYZW")  -> KeyError
    """
    if ticker in INSTRUMENTS:
        return ticker
    if ticker in _LEGACY_TO_CANONICAL:
        return _LEGACY_TO_CANONICAL[ticker]
    raise KeyError(
        f"Unknown ticker: {ticker!r}. "
        f"Canonical: {sorted(INSTRUMENTS.keys())}. "
        f"Legacy: {sorted(_LEGACY_TO_CANONICAL.keys())}."
    )


def try_normalize_ticker(ticker: str) -> Optional[str]:
    """Soft version: возвращает None если тикер неизвестен (для пермиссивной фильтрации)."""
    if ticker in INSTRUMENTS:
        return ticker
    return _LEGACY_TO_CANONICAL.get(ticker)


def is_known_ticker(ticker: str) -> bool:
    """True если ticker — известное canonical или legacy имя."""
    return ticker in ALL_KNOWN_NAMES


def get_instrument_meta(ticker: str) -> InstrumentMeta:
    """Метаданные по любому имени (canonical или legacy)."""
    return INSTRUMENTS[normalize_ticker(ticker)]


def get_lot_size(ticker: str) -> int:
    """Контрактный множитель."""
    return get_instrument_meta(ticker).lot_size


def is_usd_denominated(ticker: str) -> bool:
    return get_instrument_meta(ticker).is_usd_denominated


def all_canonical_tickers() -> list[str]:
    return list(INSTRUMENTS.keys())


def rouble_denominated_tickers() -> list[str]:
    return [c for c, m in INSTRUMENTS.items() if not m.is_usd_denominated]


# =============================================================================
# Self-check при импорте
# =============================================================================
def _self_check() -> None:
    for canonical, meta in INSTRUMENTS.items():
        if meta.legacy_phase2 != canonical and meta.legacy_phase2 in INSTRUMENTS:
            raise RuntimeError(
                f"Mapping collision: legacy '{meta.legacy_phase2}' "
                f"(of canonical '{canonical}') is ALSO a canonical name."
            )
    legacy_names = [m.legacy_phase2 for m in INSTRUMENTS.values()]
    if len(legacy_names) != len(set(legacy_names)):
        from collections import Counter
        dups = {k: v for k, v in Counter(legacy_names).items() if v > 1}
        raise RuntimeError(f"Duplicate legacy_phase2 names: {dups}")


_self_check()
