"""
Sprint 4 / Commit 4.0 — Кэш минутных котировок
================================================

Загружает prices_*.csv → parquet, предоставляет быстрый lookup для симулятора.

Зачем:
  - CSV-файлы 40-70 MB каждый, всего 19 → ~1.3 GB. Парсинг каждый раз медленный.
  - В discovery поняли, что в CSV есть готовая колонка 'datetime' (не нужно
    собирать из 'date'+'time' с int-парсингом).
  - Симулятору нужен быстрый bar-slice: get_bars(ticker, ts_from, ts_to) → DataFrame.

Convention:
  - TZ: naive MSK (как было подтверждено в discovery)
  - Колонки в parquet: ts (datetime64), open, high, low, close, vol
  - Индекс: ts (sorted, unique, monotonic)

Использование:
  cache = PricesCache(cache_dir=Path("data/cache"))
  bars = cache.get_bars("BR", datetime(2025, 6, 15, 10, 0), datetime(2025, 6, 15, 11, 0))
  # → DataFrame с минутными барами в этом окне
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from instruments import get_instrument_meta, normalize_ticker, all_canonical_tickers

log = logging.getLogger(__name__)


# =============================================================================
# Конфигурация
# =============================================================================
PRICES_DIR = Path(r"D:\quik_sber\newsbot\prices")
DEFAULT_CACHE_DIR = Path("data/cache")


# =============================================================================
# Главный класс
# =============================================================================
class PricesCache:
    """
    Загружает CSV минутки → parquet кэш, держит DataFrame в памяти для быстрого slice.

    Lazy loading: бары тикера загружаются только при первом обращении к get_bars().
    """

    def __init__(self, cache_dir: Path = DEFAULT_CACHE_DIR, prices_dir: Path = PRICES_DIR):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.prices_dir = Path(prices_dir)
        # ticker (canonical) → DataFrame с колонками ts, open, high, low, close, vol
        self._cache: dict[str, pd.DataFrame] = {}

    # -------------------------------------------------------------------------
    # Загрузка / конвертация
    # -------------------------------------------------------------------------
    def _csv_path(self, ticker: str) -> Path:
        """Путь к исходному CSV для canonical тикера."""
        meta = get_instrument_meta(ticker)
        return self.prices_dir / meta.csv_file

    def _parquet_path(self, ticker: str) -> Path:
        """Путь к parquet-кэшу."""
        return self.cache_dir / f"{ticker}.parquet"

    def _load_from_csv(self, ticker: str) -> pd.DataFrame:
        """
        Парсит CSV, возвращает чистый DataFrame.

        Discovery показал, что CSV содержит:
          - ticker, per, date (int YYYYMMDD), time (int HHMMSS)
          - open, high, low, close, vol
          - datetime (готовая строка "YYYY-MM-DD HH:MM:SS")  ← используем её!
        """
        csv_path = self._csv_path(ticker)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found for {ticker}: {csv_path}")

        log.info("Loading CSV %s (%.1f MB)...",
                 csv_path.name, csv_path.stat().st_size / 1024 / 1024)

        # Читаем только нужные колонки → экономия памяти и времени
        df = pd.read_csv(
            csv_path,
            usecols=["datetime", "open", "high", "low", "close", "vol"],
            parse_dates=["datetime"],
        )
        df = df.rename(columns={"datetime": "ts"})

        # Sanity checks
        if df["ts"].isna().any():
            n_bad = int(df["ts"].isna().sum())
            log.warning("%s: %d rows with unparseable datetime, dropping", ticker, n_bad)
            df = df.dropna(subset=["ts"])

        # Monotonic + unique
        if not df["ts"].is_monotonic_increasing:
            log.warning("%s: ts is not monotonic, sorting", ticker)
            df = df.sort_values("ts").reset_index(drop=True)

        n_dup = int(df["ts"].duplicated().sum())
        if n_dup > 0:
            log.warning("%s: %d duplicate timestamps, keeping first", ticker, n_dup)
            df = df.drop_duplicates(subset=["ts"], keep="first").reset_index(drop=True)

        # ts → index для быстрого slicing
        df = df.set_index("ts")

        # Sanity на ranges
        if (df["high"] < df["low"]).any():
            n_bad = int((df["high"] < df["low"]).sum())
            log.warning("%s: %d rows with high<low (data corruption?)", ticker, n_bad)

        log.info("%s loaded: %d bars, range %s → %s",
                 ticker, len(df), df.index.min(), df.index.max())
        return df

    def _ensure_parquet(self, ticker: str) -> Path:
        """
        Гарантирует существование parquet-кэша.
        Если parquet старше CSV (по mtime) — пересоздаёт.
        """
        ticker = normalize_ticker(ticker)
        parquet_path = self._parquet_path(ticker)
        csv_path = self._csv_path(ticker)

        need_rebuild = (
            not parquet_path.exists()
            or parquet_path.stat().st_mtime < csv_path.stat().st_mtime
        )

        if need_rebuild:
            log.info("Building parquet cache for %s...", ticker)
            df = self._load_from_csv(ticker)
            df.to_parquet(parquet_path, compression="snappy")
            log.info("Parquet saved: %s (%.1f MB)",
                     parquet_path.name, parquet_path.stat().st_size / 1024 / 1024)

        return parquet_path

    def load(self, ticker: str) -> pd.DataFrame:
        """
        Загружает (из кэша или CSV) DataFrame для тикера и держит в памяти.
        Повторные вызовы возвращают тот же объект.
        """
        ticker = normalize_ticker(ticker)
        if ticker in self._cache:
            return self._cache[ticker]

        parquet_path = self._ensure_parquet(ticker)
        log.debug("Reading parquet for %s...", ticker)
        df = pd.read_parquet(parquet_path)
        # На всякий случай удостоверимся, что индекс — datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        self._cache[ticker] = df
        return df

    def warmup(self, tickers: Optional[list[str]] = None) -> None:
        """
        Превентивно загружает кэш для списка тикеров (или всех 19).
        Полезно вызывать в начале backtest, чтобы первый get_bars не лагал.
        """
        if tickers is None:
            tickers = all_canonical_tickers()
        for t in tickers:
            try:
                self.load(t)
            except FileNotFoundError as e:
                log.warning("Skip %s: %s", t, e)

    # -------------------------------------------------------------------------
    # Lookup для симулятора
    # -------------------------------------------------------------------------
    def get_bars(
        self,
        ticker: str,
        ts_from: datetime,
        ts_to: datetime,
        inclusive: str = "both",
    ) -> pd.DataFrame:
        """
        Возвращает бары тикера в окне [ts_from, ts_to].

        Args:
            ticker: canonical или legacy имя — нормализуется автоматически
            ts_from: начало окна (naive MSK)
            ts_to: конец окна (naive MSK)
            inclusive: "both" | "left" | "right" | "neither" — границы окна

        Returns:
            DataFrame с колонками open, high, low, close, vol, индекс = ts.
            Пустой DataFrame если в окне нет баров.

        Raises:
            FileNotFoundError если для тикера нет CSV.
            ValueError если ts_from > ts_to.
        """
        if ts_from > ts_to:
            raise ValueError(f"ts_from > ts_to: {ts_from} vs {ts_to}")

        df = self.load(ticker)

        # Используем .loc — быстрый slice по индексу для DatetimeIndex
        # .loc включает обе границы по умолчанию, для других вариантов фильтруем
        result = df.loc[ts_from:ts_to]
        if inclusive == "left":
            result = result[result.index < ts_to]
        elif inclusive == "right":
            result = result[result.index > ts_from]
        elif inclusive == "neither":
            result = result[(result.index > ts_from) & (result.index < ts_to)]
        # inclusive="both" — уже как есть

        return result

    def get_bar_at(self, ticker: str, ts: datetime) -> Optional[pd.Series]:
        """
        Возвращает бар точно в момент ts (минуту в минуту).
        None если бара нет (бывает на выходные, клиринг и т.п.).
        """
        df = self.load(ticker)
        try:
            return df.loc[ts]
        except KeyError:
            return None


# =============================================================================
# CLI: warmup всех 19 тикеров
# =============================================================================
def main() -> None:
    """Запуск: создаёт parquet-кэш для всех 19 инструментов."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    cache = PricesCache()

    print("=" * 70)
    print(f"  Warming up cache for {len(all_canonical_tickers())} instruments")
    print(f"  CSV dir:    {cache.prices_dir}")
    print(f"  Cache dir:  {cache.cache_dir}")
    print("=" * 70)

    ok, failed = 0, []
    for t in all_canonical_tickers():
        try:
            df = cache.load(t)
            ok += 1
            print(f"  ✓ {t:<8s}  {len(df):>10,} bars  {df.index.min()} → {df.index.max()}")
        except FileNotFoundError as e:
            failed.append((t, str(e)))
            print(f"  ✗ {t:<8s}  {e}")

    print()
    print(f"OK: {ok}/{len(all_canonical_tickers())}")
    if failed:
        print(f"FAILED: {len(failed)}")
        for t, err in failed:
            print(f"  {t}: {err}")

    # Smoke test: запрос одного окна
    print()
    print("Smoke test: get_bars('SBER', 2025-06-15 10:00, 2025-06-15 10:10)")
    bars = cache.get_bars(
        "SBER",
        datetime(2025, 6, 15, 10, 0),
        datetime(2025, 6, 15, 10, 10),
    )
    print(bars)


if __name__ == "__main__":
    main()
