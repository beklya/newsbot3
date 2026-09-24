"""Readers для QUIK live data file: CSV (Lua glue) и Excel (pure DDE).

Single interface `CandleReader`:
    read_new_bars(since_per_ticker) -> Iterator[CandleBar]

Каждый poll cycle берёт текущую timestamp снимка last_ts_per_ticker и
возвращает только bars где ts > last_seen.

CSV reader robust к file growth (read-only seek с remembered offset).
Excel reader использует openpyxl read_only mode — handles auto-saved sessions.
"""
from __future__ import annotations

import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CandleBar:
    """One completed 1-minute OHLCV bar."""
    ticker: str
    ts: datetime  # naive MSK (Phase 2 convention)
    open: float
    high: float
    low: float
    close: float
    volume: float

    def is_valid(self) -> bool:
        """Sanity: prices > 0, high ≥ low, high ≥ open/close, low ≤ open/close."""
        if self.open <= 0 or self.close <= 0 or self.high <= 0 or self.low <= 0:
            return False
        if self.high < self.low:
            return False
        if self.high < self.open or self.high < self.close:
            return False
        if self.low > self.open or self.low > self.close:
            return False
        return True


def _parse_ts(value: object) -> Optional[datetime]:
    """Parse timestamp from CSV string or Excel cell. None on error."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)  # naive MSK
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # ISO-like: "2026-05-29 14:37:00" or "2026-05-29T14:37:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


class CandleReader(ABC):
    """Read bars from QUIK-populated file. Stateful: tracks last_ts per ticker."""

    @abstractmethod
    def read_new_bars(
        self, since_per_ticker: Dict[str, datetime],
    ) -> Iterator[CandleBar]:
        """Yield bars where ts > since_per_ticker.get(ticker, MIN_DATE)."""
        ...

    @abstractmethod
    def source_exists(self) -> bool:
        ...


class CSVTailReader(CandleReader):
    """Reads append-only CSV with header `ticker,ts,open,high,low,close,volume`.

    На каждом poll cycle перечитывает файл с last offset (не от начала).
    Tolerant к header-only-on-start, partial last line (длина < полной),
    отсутствию файла.
    """

    def __init__(self, path: Path, bootstrap_mode: str = "tail"):
        self.path = path
        self._offset = 0  # file seek position для tail-mode
        self._header_seen = False
        self._bootstrap_mode = bootstrap_mode  # "tail" — skip existing, "all" — read all on start
        # для "tail" mode при первом вызове skip'нем всё что уже в файле

    def source_exists(self) -> bool:
        return self.path.exists() and self.path.is_file()

    def read_new_bars(
        self, since_per_ticker: Dict[str, datetime],
    ) -> Iterator[CandleBar]:
        if not self.source_exists():
            return

        try:
            f = self.path.open("r", encoding="utf-8", newline="")
        except (PermissionError, OSError) as e:
            log.warning("csv_reader_io_error path=%s err=%s", self.path.name, e)
            return

        try:
            # Детект усечения/ротации: candle_dump.lua на старте truncate'ит CSV.
            # Если файл стал короче запомненного offset — сбрасываем на 0 и
            # перечитываем заново (сверка), дедуп по since_per_ticker отсечёт уже
            # опубликованные бары.
            try:
                cur_size = self.path.stat().st_size
            except OSError:
                cur_size = None
            if cur_size is not None and cur_size < self._offset:
                self._offset = 0
                self._header_seen = False

            # На самом первом cycle для bootstrap_mode='tail' пропускаем
            # уже существующий контент (только новые строки далее). Для 'all' —
            # читаем с начала (сверка+дозагрузка недостающих свечей после ребута).
            if self._offset == 0 and self._bootstrap_mode == "tail":
                f.seek(0, 2)  # end of file
                self._offset = f.tell()
                return

            f.seek(self._offset)
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 7:
                    continue
                if row[0] == "ticker" and not self._header_seen:
                    self._header_seen = True
                    continue
                bar = self._parse_row(row)
                if bar is None:
                    continue
                last = since_per_ticker.get(bar.ticker)
                if last is not None and bar.ts <= last:
                    continue
                yield bar
            self._offset = f.tell()
        finally:
            f.close()

    def _parse_row(self, row: list[str]) -> Optional[CandleBar]:
        try:
            ticker = row[0].strip()
            ts = _parse_ts(row[1])
            if ts is None or not ticker:
                return None
            bar = CandleBar(
                ticker=ticker,
                ts=ts,
                open=float(row[2]),
                high=float(row[3]),
                low=float(row[4]),
                close=float(row[5]),
                volume=float(row[6]),
            )
        except (ValueError, IndexError):
            return None
        return bar if bar.is_valid() else None


class ExcelReader(CandleReader):
    """Reads .xlsx через openpyxl read_only. Re-reads весь sheet каждый cycle,
    фильтрует по last_seen ts. Подходит для DDE без Lua glue.

    Лист (sheet_name) с колонками A..G:
      A=ticker, B=ts, C=open, D=high, E=low, F=close, G=volume
    """

    def __init__(self, path: Path, sheet_name: str = "candles"):
        self.path = path
        self.sheet_name = sheet_name

    def source_exists(self) -> bool:
        return self.path.exists() and self.path.is_file()

    def read_new_bars(
        self, since_per_ticker: Dict[str, datetime],
    ) -> Iterator[CandleBar]:
        if not self.source_exists():
            return

        try:
            import openpyxl
        except ImportError:
            log.error("openpyxl not installed — Excel reader unusable")
            return

        try:
            wb = openpyxl.load_workbook(
                str(self.path), read_only=True, data_only=True,
            )
        except (PermissionError, OSError, openpyxl.utils.exceptions.InvalidFileException) as e:
            # Excel может удерживать file lock на autosave
            log.debug("excel_reader_io_error path=%s err=%s", self.path.name, e)
            return

        try:
            if self.sheet_name not in wb.sheetnames:
                log.warning("excel sheet '%s' not found, sheets=%s",
                            self.sheet_name, wb.sheetnames)
                return
            ws = wb[self.sheet_name]
            header_skipped = False
            for row in ws.iter_rows(values_only=True):
                if row is None or len(row) < 7:
                    continue
                if not header_skipped:
                    header_skipped = True
                    # Skip header (assume row 1 is header). Если в первой строке
                    # реальные данные — следующая итерация подхватит остальные.
                    if isinstance(row[0], str) and row[0].strip().lower() == "ticker":
                        continue
                bar = self._parse_excel_row(row)
                if bar is None:
                    continue
                last = since_per_ticker.get(bar.ticker)
                if last is not None and bar.ts <= last:
                    continue
                yield bar
        finally:
            wb.close()

    def _parse_excel_row(self, row: tuple) -> Optional[CandleBar]:
        try:
            ticker = str(row[0]).strip() if row[0] is not None else ""
            ts = _parse_ts(row[1])
            if ts is None or not ticker:
                return None
            bar = CandleBar(
                ticker=ticker,
                ts=ts,
                open=float(row[2]),
                high=float(row[3]),
                low=float(row[4]),
                close=float(row[5]),
                volume=float(row[6]) if row[6] is not None else 0.0,
            )
        except (ValueError, TypeError, IndexError):
            return None
        return bar if bar.is_valid() else None


def build_reader(path: Path, sheet_name: str, bootstrap_mode: str) -> CandleReader:
    """Factory by file extension."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return CSVTailReader(path, bootstrap_mode=bootstrap_mode)
    if suffix == ".xlsx":
        return ExcelReader(path, sheet_name=sheet_name)
    raise ValueError(f"Unsupported source extension: {suffix}")
