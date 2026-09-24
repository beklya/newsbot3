"""
sprint4/analysis/price_moves_lookup.py — unified price-moves lookup для 4.7/4.8/4.9/4.10.

Wraps `sprint4/exits/prices_cache.py` для use case:
    "для event (ticker, ts_msk) — какой pct_change цены на горизонте H минут после ts?"

Использование:
    from price_moves_lookup import PriceMovesLookup

    pml = PriceMovesLookup()
    pml.warmup()  # загружает 19 parquet'ов в память

    # Single lookup
    delta = pml.compute_one(ticker="SBER", ts_msk=datetime(2025, 6, 15, 11, 30),
                            horizon_min=15)
    # → -0.0023 (т.е. -0.23%) или None если нет бара

    # Multi-horizon одним вызовом
    deltas = pml.compute_multi(ticker="SBER", ts_msk=datetime(...),
                                horizons_min=[5, 15, 30, 60, 120])
    # → {5: -0.001, 15: -0.0023, 30: None, ...}

Conventions:
  - ts_msk: naive datetime MSK (как в prices_cache)
  - ticker: legacy ИЛИ canonical — normalize_ticker нормализует
  - Pct change = (close_at_ts_plus_H - close_at_ts) / close_at_ts
  - Возвращает None если нет бара в ts или ts+H (выходные, клиринг, праздники)
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))  # для prices_cache + instruments
from prices_cache import PricesCache  # noqa: E402

# Импорт normalize_ticker из canonical registry (Sprint 4.1)
sys.path.insert(0, str(PROJECT_ROOT))
from src.contracts.instruments import try_normalize_ticker  # noqa: E402

log = logging.getLogger("price_moves_lookup")

# Default cache directory — переиспользуем кэш от sprint 4.0
DEFAULT_CACHE_DIR = PROJECT_ROOT / "sprint4" / "exits" / "data" / "cache"


class PriceMovesLookup:
    """Wrapper над PricesCache с per-event lookup семантикой."""

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR):
        self.cache = PricesCache(cache_dir=cache_dir)

    def warmup(self, tickers: list[str] | None = None) -> None:
        """Загрузить parquet'ы всех 19 (или указанных) тикеров в память."""
        self.cache.warmup(tickers)

    def _normalize(self, ticker: str) -> str | None:
        """Legacy/canonical → canonical. None если ticker не в registry."""
        return try_normalize_ticker(ticker)

    def compute_one(
        self, ticker: str, ts_msk: datetime, horizon_min: int,
    ) -> Optional[float]:
        """Returns pct_change или None."""
        canonical = self._normalize(ticker)
        if canonical is None:
            return None
        try:
            base = self.cache.get_bar_at(canonical, ts_msk)
        except FileNotFoundError:
            return None
        if base is None:
            return None
        base_close = float(base["close"])
        if base_close == 0:
            return None

        future_ts = ts_msk + timedelta(minutes=horizon_min)
        try:
            future = self.cache.get_bar_at(canonical, future_ts)
        except FileNotFoundError:
            return None
        if future is None:
            return None
        future_close = float(future["close"])
        return (future_close - base_close) / base_close

    def compute_multi(
        self, ticker: str, ts_msk: datetime, horizons_min: list[int],
    ) -> dict[int, Optional[float]]:
        """Multi-horizon lookup за один вызов. Возвращает {horizon: pct_change_or_None}."""
        canonical = self._normalize(ticker)
        result: dict[int, Optional[float]] = {h: None for h in horizons_min}
        if canonical is None:
            return result
        try:
            base = self.cache.get_bar_at(canonical, ts_msk)
        except FileNotFoundError:
            return result
        if base is None:
            return result
        base_close = float(base["close"])
        if base_close == 0:
            return result

        for h in horizons_min:
            future_ts = ts_msk + timedelta(minutes=h)
            try:
                future = self.cache.get_bar_at(canonical, future_ts)
            except FileNotFoundError:
                continue
            if future is None:
                continue
            future_close = float(future["close"])
            result[h] = (future_close - base_close) / base_close
        return result


# Standard horizons (минуты), используются в 4.7/4.8
STANDARD_HORIZONS_MIN = [5, 10, 15, 30, 45, 60, 90, 120, 180]


def smoke_test() -> int:
    """Quick self-test: load SBER, lookup at known timestamp."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    pml = PriceMovesLookup()
    log.info("warming up SBER...")
    pml.warmup(["SBER"])

    ts = datetime(2025, 6, 16, 11, 0)  # Mon trading session
    deltas = pml.compute_multi("SBER", ts, STANDARD_HORIZONS_MIN)
    log.info("SBER @ %s deltas:", ts)
    for h, d in deltas.items():
        if d is None:
            log.info("  %3dmin: None", h)
        else:
            log.info("  %3dmin: %+.4f%%", h, d * 100)
    return 0


if __name__ == "__main__":
    sys.exit(smoke_test())
