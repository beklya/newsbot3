"""CSVTailReader 'all'-режим: сверка+дозагрузка недостающих свечей после ребута.

Гейт под фичу «дозагрузка минутных свечей за весь рабочий день»: candle_dump.lua на
старте truncate'ит CSV и дампит всю сегодняшнюю сессию; quik_feed читает CSV С НАЧАЛА
(bootstrap_mode=all) и дедупит по last_ts из candles:1m → публикует ТОЛЬКО пропущенные.
Плюс детект усечения файла (Lua truncate под работающим quik_feed).
"""
from __future__ import annotations

from datetime import datetime

from src.services.quik_feed.readers import CSVTailReader

HEADER = "ticker,ts,open,high,low,close,volume\n"


def _row(ticker, ts):
    return f"{ticker},{ts},100.0,100.5,99.5,100.2,10\n"


def _write(path, lines):
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(HEADER)
        for ln in lines:
            f.write(ln)


def test_reconcile_publishes_only_missing(tmp_path):
    p = tmp_path / "candles.csv"
    # Lua задампил всю сессию SI 10:00..10:03
    _write(p, [_row("SI", "2026-06-15 10:00:00"), _row("SI", "2026-06-15 10:01:00"),
               _row("SI", "2026-06-15 10:02:00"), _row("SI", "2026-06-15 10:03:00")])

    r = CSVTailReader(p, bootstrap_mode="all")
    # candles:1m уже содержит до 10:01 (last_ts из Redis) → дозагрузить 10:02, 10:03
    since = {"SI": datetime(2026, 6, 15, 10, 1, 0)}
    got = [b.ts for b in r.read_new_bars(since)]
    assert got == [datetime(2026, 6, 15, 10, 2), datetime(2026, 6, 15, 10, 3)], got

    # хвост: дописан 10:04 → tail отдаёт только его
    with p.open("a", encoding="utf-8", newline="") as f:
        f.write(_row("SI", "2026-06-15 10:04:00"))
    since = {"SI": datetime(2026, 6, 15, 10, 3, 0)}
    got2 = [b.ts for b in r.read_new_bars(since)]
    assert got2 == [datetime(2026, 6, 15, 10, 4)], got2


def test_truncation_resets_offset(tmp_path):
    p = tmp_path / "candles.csv"
    _write(p, [_row("SI", "2026-06-15 10:00:00"), _row("SI", "2026-06-15 10:01:00")])
    r = CSVTailReader(p, bootstrap_mode="all")
    _ = list(r.read_new_bars({}))            # прочитали всё, offset = конец файла
    assert r._offset > 0

    # Lua перезапустился → truncate + новая сессия (файл стал короче)
    _write(p, [_row("SI", "2026-06-15 11:00:00")])
    got = [b.ts for b in r.read_new_bars({"SI": datetime(2026, 6, 15, 10, 1)})]
    # детект усечения → offset сброшен → перечитали, 11:00 > 10:01 → отдан
    assert got == [datetime(2026, 6, 15, 11, 0)], got
