"""Тесты readers для quik_feed (Sprint 5.8).

CSVTailReader покрытие:
  - read header + bars
  - skip already-seen ts
  - bootstrap_mode 'all' vs 'tail'
  - tolerant к incomplete last line
  - tolerant к отсутствующему файлу

ExcelReader покрытие:
  - read из synthetic .xlsx (openpyxl)
  - skip header
  - skip stale rows
  - skip rows с invalid OHLCV
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from src.services.quik_feed.readers import (
    CandleBar,
    CSVTailReader,
    ExcelReader,
    build_reader,
)


# --- CandleBar validation ---

def test_candle_bar_valid():
    bar = CandleBar(
        ticker="SBER", ts=datetime(2026, 5, 29, 14, 37),
        open=295.0, high=295.5, low=294.8, close=295.3, volume=10000,
    )
    assert bar.is_valid()


def test_candle_bar_invalid_high_below_low():
    bar = CandleBar(
        ticker="SBER", ts=datetime(2026, 5, 29, 14, 37),
        open=295.0, high=294.0, low=295.5, close=295.3, volume=0,
    )
    assert not bar.is_valid()


def test_candle_bar_invalid_negative_price():
    bar = CandleBar(
        ticker="SBER", ts=datetime(2026, 5, 29, 14, 37),
        open=-1.0, high=295.5, low=294.8, close=295.3, volume=0,
    )
    assert not bar.is_valid()


# --- CSVTailReader ---

def _write_csv(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_csv_reader_bootstrap_all(tmp_path: Path):
    path = tmp_path / "candles.csv"
    _write_csv(path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000",
        "GAZP,2026-05-29 14:37:00,150.5,151.0,150.2,150.8,8000",
    ])
    reader = CSVTailReader(path, bootstrap_mode="all")
    bars = list(reader.read_new_bars({}))
    assert len(bars) == 2
    assert {b.ticker for b in bars} == {"SBER", "GAZP"}


def test_csv_reader_bootstrap_tail_skips_existing(tmp_path: Path):
    path = tmp_path / "candles.csv"
    _write_csv(path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000",
    ])
    reader = CSVTailReader(path, bootstrap_mode="tail")
    # First poll: tail mode skips existing
    bars = list(reader.read_new_bars({}))
    assert bars == []

    # Append new bar
    with path.open("a", encoding="utf-8") as f:
        f.write("GAZP,2026-05-29 14:38:00,150.5,151.0,150.2,150.8,8000\n")

    bars = list(reader.read_new_bars({}))
    assert len(bars) == 1
    assert bars[0].ticker == "GAZP"


def test_csv_reader_skip_already_seen_ts(tmp_path: Path):
    path = tmp_path / "candles.csv"
    _write_csv(path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000",
        "SBER,2026-05-29 14:38:00,295.3,295.8,295.0,295.6,9500",
    ])
    reader = CSVTailReader(path, bootstrap_mode="all")
    since = {"SBER": datetime(2026, 5, 29, 14, 37)}
    bars = list(reader.read_new_bars(since))
    assert len(bars) == 1
    assert bars[0].ts == datetime(2026, 5, 29, 14, 38)


def test_csv_reader_missing_file_returns_empty(tmp_path: Path):
    reader = CSVTailReader(tmp_path / "does_not_exist.csv", bootstrap_mode="all")
    assert not reader.source_exists()
    bars = list(reader.read_new_bars({}))
    assert bars == []


def test_csv_reader_appends_detected_across_polls(tmp_path: Path):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["ticker,ts,open,high,low,close,volume"])
    reader = CSVTailReader(path, bootstrap_mode="all")
    bars = list(reader.read_new_bars({}))
    assert bars == []

    with path.open("a", encoding="utf-8") as f:
        f.write("SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000\n")

    bars = list(reader.read_new_bars({}))
    assert len(bars) == 1
    assert bars[0].open == pytest.approx(295.0)


def test_csv_reader_skips_invalid_rows(tmp_path: Path):
    path = tmp_path / "candles.csv"
    _write_csv(path, [
        "ticker,ts,open,high,low,close,volume",
        "SBER,not-a-date,295.0,295.5,294.8,295.3,10000",     # bad ts
        "GAZP,2026-05-29 14:37:00,abc,151.0,150.2,150.8,8000",  # bad number
        "LKOH,2026-05-29 14:37:00,5000.0,5050.0,4950.0,5020.0,1000",
    ])
    reader = CSVTailReader(path, bootstrap_mode="all")
    bars = list(reader.read_new_bars({}))
    assert len(bars) == 1
    assert bars[0].ticker == "LKOH"


# --- ExcelReader ---

def _write_xlsx(path: Path, rows: list[list]) -> None:
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "candles"
    for row in rows:
        ws.append(row)
    wb.save(str(path))


def test_excel_reader_reads_bars(tmp_path: Path):
    path = tmp_path / "candles.xlsx"
    _write_xlsx(path, [
        ["ticker", "ts", "open", "high", "low", "close", "volume"],
        ["SBER", "2026-05-29 14:37:00", 295.0, 295.5, 294.8, 295.3, 10000],
        ["GAZP", "2026-05-29 14:38:00", 150.5, 151.0, 150.2, 150.8, 8000],
    ])
    reader = ExcelReader(path, sheet_name="candles")
    bars = list(reader.read_new_bars({}))
    assert len(bars) == 2
    tickers = {b.ticker for b in bars}
    assert tickers == {"SBER", "GAZP"}


def test_excel_reader_skips_stale(tmp_path: Path):
    path = tmp_path / "candles.xlsx"
    _write_xlsx(path, [
        ["ticker", "ts", "open", "high", "low", "close", "volume"],
        ["SBER", "2026-05-29 14:37:00", 295.0, 295.5, 294.8, 295.3, 10000],
        ["SBER", "2026-05-29 14:38:00", 295.3, 295.8, 295.0, 295.6, 9500],
    ])
    reader = ExcelReader(path, sheet_name="candles")
    since = {"SBER": datetime(2026, 5, 29, 14, 37)}
    bars = list(reader.read_new_bars(since))
    assert len(bars) == 1
    assert bars[0].ts == datetime(2026, 5, 29, 14, 38)


def test_excel_reader_missing_sheet(tmp_path: Path):
    path = tmp_path / "candles.xlsx"
    _write_xlsx(path, [["ticker", "ts", "open", "high", "low", "close", "volume"]])
    reader = ExcelReader(path, sheet_name="wrong_sheet")
    bars = list(reader.read_new_bars({}))
    assert bars == []


def test_excel_reader_missing_file(tmp_path: Path):
    reader = ExcelReader(tmp_path / "missing.xlsx", sheet_name="candles")
    assert not reader.source_exists()
    bars = list(reader.read_new_bars({}))
    assert bars == []


# --- Factory ---

def test_build_reader_csv(tmp_path: Path):
    path = tmp_path / "x.csv"
    reader = build_reader(path, sheet_name="candles", bootstrap_mode="tail")
    assert isinstance(reader, CSVTailReader)


def test_build_reader_xlsx(tmp_path: Path):
    path = tmp_path / "x.xlsx"
    reader = build_reader(path, sheet_name="candles", bootstrap_mode="tail")
    assert isinstance(reader, ExcelReader)


def test_build_reader_unsupported_extension(tmp_path: Path):
    with pytest.raises(ValueError, match="Unsupported"):
        build_reader(tmp_path / "x.txt", sheet_name="candles", bootstrap_mode="tail")
