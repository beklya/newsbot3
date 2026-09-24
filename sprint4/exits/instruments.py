"""
Sprint 4 / Commit 4.0 / 4.1 — Реестр инструментов
====================================================

Single source of truth для всех тикер-mappings + контрактных множителей.

REVISION 2 (после анализа backtest_mfe.py):
  Добавлено поле lot_size — критично для правильного расчёта PnL.
  Phase 2 формула: gross_pnl = (exit - entry) × lot_size × n_lots
  Значения lot_size взяты дословно из backtest_mfe.py:108-114.

Зачем lot_size:
  - SBER, GAZP, MTSS, ROSN = 10 (1 лот = 10 акций на MOEX)
  - VTBR = 10000 (большой множитель — особенность инструмента)
  - USDRUB = 1000
  - Все фьючерсы (BR, NG, MIX, SI, GLDRUB, CNY) = 1 (один контракт)
  - LKOH, YDEX, GMKN, NVTK, TATN, MGNT, PLZL = 1 (премиум-цена, лот=1)

Использование:
  from instruments import normalize_ticker, get_instrument_meta
  meta = get_instrument_meta("MTSS")
  pnl = side * (exit - entry) * meta.lot_size * n_lots
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
    asset_class: str
    is_usd_denominated: bool
    lot_size: int                  # *** контрактный множитель — из Phase 2 backtest_mfe.py
    available_from: Optional[date]
    notes: str = ""


# =============================================================================
# LOT_SIZES — точная копия из backtest_mfe.py:108-114
# =============================================================================
# SBER=10, GAZP=10, LKOH=1, YNDX=1, ROSN=10,
# GMKN=1, NVTK=1, TATN=1, MGNT=1, MTSS=10,
# PLZL=1, VTBR=10000,
# Si=1, MX=1, BR=1, NG=1, GOLD=1, CNY=1,
# USDRUB=1000

INSTRUMENTS: dict[str, InstrumentMeta] = {
    # ── Акции lot_size=10 ────────────────────────────────────────────────
    "SBER":   InstrumentMeta("SBER", "SBER", "prices_SBER.csv", "equity", False, 10, None),
    "GAZP":   InstrumentMeta("GAZP", "GAZP", "prices_GAZP.csv", "equity", False, 10, None),
    "ROSN":   InstrumentMeta("ROSN", "ROSN", "prices_ROSN.csv", "equity", False, 10, None),
    "MTSS":   InstrumentMeta("MTSS", "MTSS", "prices_MTSS.csv", "equity", False, 10, None),

    # ── Акции lot_size=1 ─────────────────────────────────────────────────
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

    # ── VTBR — особый случай, lot=10000 ──────────────────────────────────
    "VTBR":   InstrumentMeta(
        "VTBR", "VTBR", "prices_VTBR.csv", "equity", False, 10000, None,
        notes="VTBR имеет специфический lot_size=10000 (см. MOEX spec)."
    ),

    # ── Sprint 9 — MOEXBC blue-chip additions ───────────────────────────
    "SNGS":   InstrumentMeta("SNGS", "SNGS", "prices_SNGS.csv", "equity", False, 100, None),
    "MOEX":   InstrumentMeta("MOEX", "MOEX", "prices_MOEX.csv", "equity", False, 10, None),
    "OZON":   InstrumentMeta(
        "OZON", "OZON", "prices_OZON.csv", "equity", False, 1,
        available_from=date(2020, 11, 24)),
    "T":      InstrumentMeta(
        "T", "T", "prices_T.csv", "equity", False, 1, None,
        notes="Т-Технологии (переименование из TCSG в 2024)."),
    "X5":     InstrumentMeta(
        "X5", "X5", "prices_X5.csv", "equity", False, 1,
        available_from=date(2025, 1, 9),
        notes="Листинг на MOEX после редомициляции; цена с 2025-01."),

    # ── Фьючерсы lot_size=1 (рублёвые) ───────────────────────────────────
    "MIX":    InstrumentMeta("MIX", "MX", "prices_MIX.csv", "futures", False, 1, None,
                             notes="Phase 2 legacy: MX. Фьючерс на индекс Мосбиржи."),
    "SI":     InstrumentMeta("SI", "Si", "prices_SI.csv", "futures", False, 1, None,
                             notes="Phase 2 legacy: 'Si' (case-sensitive)."),
    "CNY":    InstrumentMeta("CNY", "CNY", "prices_CNY.csv", "currency", False, 1, None),

    # ── Долларовые фьючерсы (PSEUDO-RUB!) lot_size=1 ─────────────────────
    "BR":     InstrumentMeta("BR", "BR", "prices_BR.csv", "futures", True, 1, None,
                             notes="Brent. Phase 2 PnL в долларах (псевдо-руб)."),
    "NG":     InstrumentMeta("NG", "NG", "prices_NG.csv", "futures", True, 1, None,
                             notes="NatGas. Phase 2 PnL в долларах (псевдо-руб)."),
    "GLDRUB": InstrumentMeta(
        "GLDRUB", "GOLD", "prices_GLDRUB.csv", "commodity", True, 1,
        available_from=date(2023, 7, 12),
        notes="Phase 2 legacy: GOLD. Появился 2023-07-12. PnL в долларах."
    ),

    # ── USDRUB lot_size=1000 ─────────────────────────────────────────────
    "USDRUB": InstrumentMeta(
        "USDRUB", "USDRUB", "prices_USDRUB.csv", "currency", False, 1000, None,
        notes="lot_size=1000 (необычно для валютной пары)."
    ),
}


# =============================================================================
# Reverse mapping
# =============================================================================
_LEGACY_TO_CANONICAL: dict[str, str] = {
    meta.legacy_phase2: canonical
    for canonical, meta in INSTRUMENTS.items()
}


# =============================================================================
# Public API
# =============================================================================
def normalize_ticker(ticker: str) -> str:
    if ticker in INSTRUMENTS:
        return ticker
    if ticker in _LEGACY_TO_CANONICAL:
        return _LEGACY_TO_CANONICAL[ticker]
    raise KeyError(f"Unknown ticker: {ticker!r}")


def get_instrument_meta(ticker: str) -> InstrumentMeta:
    return INSTRUMENTS[normalize_ticker(ticker)]


def get_lot_size(ticker: str) -> int:
    """Контрактный множитель из Phase 2 LOT_SIZES."""
    return get_instrument_meta(ticker).lot_size


def is_usd_denominated(ticker: str) -> bool:
    return get_instrument_meta(ticker).is_usd_denominated


def all_canonical_tickers() -> list[str]:
    return list(INSTRUMENTS.keys())


def rouble_denominated_tickers() -> list[str]:
    return [c for c, m in INSTRUMENTS.items() if not m.is_usd_denominated]


# =============================================================================
# Self-check
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


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    print("=" * 80)
    print(f"  Instrument Registry - {len(INSTRUMENTS)} tickers")
    print("=" * 80)
    print(f"{'Canonical':<10s} {'Legacy':<8s} {'Class':<10s} {'USD':<5s} {'lot':>6s}  CSV file")
    print("-" * 80)
    for canonical, meta in INSTRUMENTS.items():
        usd = "YES" if meta.is_usd_denominated else "no"
        legacy = meta.legacy_phase2 if meta.legacy_phase2 != canonical else "-"
        print(f"{canonical:<10s} {legacy:<8s} {meta.asset_class:<10s} {usd:<5s} "
              f"{meta.lot_size:>6d}  {meta.csv_file}")
    print()
    print(f"USD-denominated (pseudo-RUB): {sorted([c for c, m in INSTRUMENTS.items() if m.is_usd_denominated])}")
    print(f"Lot sizes > 1: {[(c, m.lot_size) for c, m in INSTRUMENTS.items() if m.lot_size > 1]}")
