"""Screener пропущенных свечей в _publish_bar: дозагрузка ТОЛЬКО недостающих.

Дедуп по МНОЖЕСТВУ (canonical,ts) за окно сверки, НЕ по max-ts → дыра в СЕРЕДИНЕ
дозагружается, а не игнорируется. Бары старше окна (reconcile_lookback_days) — skip.
Время относительное (floor = now − N дней), чтобы тест не зависел от календарной даты.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from src.services.quik_feed.config import QuikFeedSettings
from src.services.quik_feed.feeder import QuikFeeder
from src.services.quik_feed.metrics import QuikFeedMetrics
from src.services.quik_feed.readers import CandleBar, CandleReader

_MSK = timezone(timedelta(hours=3))


def _now():
    return datetime.now(_MSK).replace(tzinfo=None, second=0, microsecond=0)


def _iso(dt):
    return dt.isoformat(timespec="seconds")


class FakeReader(CandleReader):
    def __init__(self, bars):
        self.bars = bars

    def source_exists(self):
        return True

    def read_new_bars(self, since):
        for b in self.bars:                      # reader фильтрует по RAW тикеру
            last = since.get(b.ticker)
            if last is not None and b.ts <= last:
                continue
            yield b


class FakeRedis:
    def __init__(self, boot):
        self.boot = boot
        self.xadds = []

    async def xrevrange(self, stream, count=10000):
        return self.boot

    async def xadd(self, stream, fields=None, maxlen=None, approximate=None):
        self.xadds.append(fields)
        return b"1-1"


def _bar(t, ts):
    return CandleBar(t, ts, 100.0, 100.5, 99.5, 100.2, 10.0)


def _boot(*dts):
    # candles:1m уже содержит эти (canonical SI) бары
    return [(f"{i}-0".encode(), {b"ticker": b"SI", b"ts": _iso(d).encode()})
            for i, d in enumerate(dts)]


def _run(boot, bars):
    r = FakeRedis(boot)
    f = QuikFeeder(redis=r, reader=FakeReader(bars),
                   settings=QuikFeedSettings(), metrics=QuikFeedMetrics())

    async def go():
        await f.bootstrap_from_redis()
        await f.poll_once()
    asyncio.run(go())
    return [x["ts"] for x in r.xadds]


def test_no_republish_of_present_bars():
    base = _now() - timedelta(hours=2)
    t = [base + timedelta(minutes=i) for i in range(3)]
    # candles:1m имеет t0,t1; reader отдаёт t0,t1,t2 (raw) → опубликован только t2
    out = _run(_boot(t[0], t[1]), [_bar("Si", x) for x in t])
    assert out == [_iso(t[2])], out


def test_middle_gap_is_backfilled():
    base = _now() - timedelta(hours=2)
    t = [base + timedelta(minutes=i) for i in range(3)]
    # candles:1m имеет t0 и t2 (ДЫРА на t1); reader отдаёт t0,t1,t2 → дозагружен t1
    out = _run(_boot(t[0], t[2]), [_bar("Si", x) for x in t])
    assert out == [_iso(t[1])], out


def test_older_than_window_skipped():
    old = _now() - timedelta(days=10)            # вне окна сверки (4д) → skip
    recent = _now() - timedelta(minutes=30)
    out = _run([], [_bar("Si", old), _bar("Si", recent)])
    assert out == [_iso(recent)], out
