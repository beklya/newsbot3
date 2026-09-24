"""Shared CandleCache — in-memory minute-bar storage for 19 tickers.

Used by:
  - Predictor (5.1): feature_builder market context
  - Bridge   (5.3): PaperExecutor fill + position tracking

Load on startup: читает 19 prices_{TICKER}.csv в RAM, ключ по canonical-
имени тикера (через instruments registry). Каждый DataFrame индексирован
по `ts` (naive MSK по convention Phase 2).

Sprint 5.8 — Live updates через Redis stream `candles:1m` (производит
quik_feed service). Cold start читает historical CSVs (если есть),
поверх них накатываются live bars. Backfill window: prices_*.csv
покрывает 2022-01-03 → 2026-04-21 (Phase 2 end), live feed continues
оттуда.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.contracts.instruments import try_normalize_ticker
from src.infra.redis_retry import (
    ReconnectBackoff,
    is_connection_error,
    reset_pool,
)

log = logging.getLogger(__name__)


# Mapping canonical-name → CSV file prefix.
# Phase 2 переименовал YNDX→YDEX, GOLD→GLDRUB, Si→SI, MX→MIX в файловых
# именах. Поэтому даже canonical ticker (SI, MIX, YDEX, GLDRUB) требует
# explicit маппинг — CSV-name был выбран до canonical schema.
CSV_PREFIX_FOR_CANONICAL: Dict[str, str] = {
    "SBER": "SBER", "GAZP": "GAZP", "LKOH": "LKOH",
    "YDEX": "YDEX", "ROSN": "ROSN", "NVTK": "NVTK",
    "VTBR": "VTBR", "GMKN": "GMKN", "MGNT": "MGNT",
    "MTSS": "MTSS", "TATN": "TATN", "PLZL": "PLZL",
    "SI": "SI", "MIX": "MIX", "BR": "BR",
    "NG": "NG", "GLDRUB": "GLDRUB", "CNY": "CNY",
    "USDRUB": "USDRUB",
}


def _read_candles_csv(path: Path) -> Optional[pd.DataFrame]:
    """Reuse Phase 2 reader: detect separator, normalize columns, parse datetime."""
    with open(path, encoding="utf-8") as fp:
        first_line = fp.readline().strip()
    sep = ";" if (";" in first_line and "," not in first_line) else ","

    df = pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False)
    df.columns = [c.strip("<>").lower() for c in df.columns]

    if "datetime" in df.columns:
        df["ts"] = pd.to_datetime(df["datetime"], errors="coerce")
    elif "date" in df.columns and "time" in df.columns:
        df["ts"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str).str.zfill(6),
            format="%Y%m%d %H%M%S", errors="coerce",
        )
    else:
        return None

    df = df.dropna(subset=["ts", "close"])
    if len(df) == 0:
        return None

    if "vol" in df.columns:
        df = df.rename(columns={"vol": "volume"})
    if "volume" not in df.columns:
        df["volume"] = 0

    needed = ["ts", "open", "high", "low", "close", "volume"]
    available = [c for c in needed if c in df.columns]
    return df[available]


class CandleCache:
    """In-memory candle store. Single CSV per ticker."""

    def __init__(self, prices_dir: Path) -> None:
        self._prices_dir = prices_dir
        self._candles: Dict[str, pd.DataFrame] = {}  # key = canonical ticker

    def load_all(self) -> None:
        """Load 19 CSVs into RAM. Logs each ticker."""
        log.info("Loading candles from %s...", self._prices_dir)
        total_bars = 0
        for canonical, prefix in CSV_PREFIX_FOR_CANONICAL.items():
            path = self._prices_dir / f"prices_{prefix}.csv"
            if not path.exists():
                log.warning("  %s: missing CSV at %s", canonical, path.name)
                continue
            df = _read_candles_csv(path)
            if df is None or len(df) == 0:
                log.warning("  %s: empty/unreadable", canonical)
                continue
            df = df.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
            df = df.set_index("ts")
            self._candles[canonical] = df
            total_bars += len(df)
            log.info("  %s: %d bars [%s → %s]",
                     canonical, len(df), df.index[0].date(), df.index[-1].date())
        log.info("CandleCache loaded: %d tickers, %d total bars",
                 len(self._candles), total_bars)

    def get(self, ticker: str) -> Optional[pd.DataFrame]:
        """Look up by ticker (canonical or legacy via instruments registry)."""
        canonical = try_normalize_ticker(ticker) or ticker
        return self._candles.get(canonical)

    def has(self, ticker: str) -> bool:
        return self.get(ticker) is not None

    def ready_tickers(self) -> list[str]:
        return sorted(self._candles.keys())

    def last_bar_time(self, ticker: str) -> Optional[pd.Timestamp]:
        """Latest bar ts для тикера. None если нет данных. Используется
        Bridge.PositionTracker для wait_for_bar и feature_builder для
        staleness check."""
        df = self.get(ticker)
        if df is None or len(df) == 0:
            return None
        return df.index[-1]

    def add_bar(
        self,
        ticker: str,
        ts: datetime,
        o: float, h: float, l: float, c: float, v: float = 0.0,
    ) -> bool:
        """Append a single 1-min bar. Returns True если bar добавлен (новый),
        False если ts ≤ last_bar_time (повтор/устаревший).

        Безопасно вызывается из async (no awaits внутри). Используется
        Sprint 5.8 live feed subscriber.

        - Если ts > last_bar_time → fast append, sorted index сохраняется
        - Если ts уже есть в index → overwrite (idempotent на duplicate)
        - Если ts < last_bar_time → insert + re-sort (редкий case на retransmit)
        """
        canonical = try_normalize_ticker(ticker) or ticker
        ts_pd = pd.Timestamp(ts)
        new_row = pd.Series(
            {"open": float(o), "high": float(h), "low": float(l),
             "close": float(c), "volume": float(v)},
        )

        df = self._candles.get(canonical)
        if df is None:
            # Cold start: ticker без historical CSV — создаём DF на лету.
            self._candles[canonical] = pd.DataFrame(
                [new_row.values],
                columns=["open", "high", "low", "close", "volume"],
                index=pd.DatetimeIndex([ts_pd], name="ts"),
            )
            return True

        if len(df) == 0:
            df.loc[ts_pd] = new_row
            return True

        last_ts = df.index[-1]
        if ts_pd > last_ts:
            df.loc[ts_pd] = new_row
            return True
        if ts_pd == last_ts or ts_pd in df.index:
            df.loc[ts_pd] = new_row  # overwrite — idempotent on duplicate
            return False
        # ts_pd < last_ts (out-of-order) — append + re-sort
        df.loc[ts_pd] = new_row
        self._candles[canonical] = df.sort_index()
        return True

    async def subscribe_redis_stream(
        self,
        redis,
        stream: str,
        shutdown: asyncio.Event,
        block_ms: int = 5_000,
    ) -> None:
        """Async subscriber для Redis stream `candles:1m` (от quik_feed).

        Каждый message — один CandleBar. После XADD в quik_feed.feeder
        вызывает add_bar и продолжает читать. Не использует consumer group
        (это broadcast: и Predictor, и Bridge должны независимо update'ить
        свой in-process cache).

        Note: cursor хранится в self._stream_cursor — после restart resume
        с того места где остановились. Это даёт catch-up на пропущенные
        bars во время downtime сервиса.
        """
        cursor = "$"  # начинаем с новых сообщений (после bootstrap)
        log.info("candle_cache live subscriber started stream=%s", stream)
        # Sprint 6.2 — force pool reset + exp backoff on connection failures.
        # candles:1m is broadcast (XREAD, no consumer group), so on reconnect
        # we just resume from the last known cursor (no PEL semantics).
        backoff = ReconnectBackoff()
        while not shutdown.is_set():
            try:
                resp = await redis.xread(
                    streams={stream: cursor}, count=100, block=block_ms,
                )
                backoff.reset()
            except Exception as e:
                if is_connection_error(e):
                    log.warning(
                        "candle_cache xread connection error (attempt %d): %s",
                        backoff.attempts + 1, e,
                    )
                    await reset_pool(redis, where=f"candle_cache/{stream}")
                    await backoff.sleep(shutdown)
                    continue
                log.warning("candle_cache xread error: %s", e)
                await asyncio.sleep(1)
                continue
            if not resp:
                continue
            for _stream_name, msgs in resp:
                for msg_id, fields in msgs:
                    cursor = msg_id
                    try:
                        ticker = fields[b"ticker"].decode("utf-8")
                        ts = datetime.fromisoformat(fields[b"ts"].decode("utf-8"))
                        o = float(fields[b"open"])
                        h = float(fields[b"high"])
                        lo = float(fields[b"low"])
                        c = float(fields[b"close"])
                        v = float(fields.get(b"volume", b"0"))
                    except (KeyError, ValueError, UnicodeDecodeError) as e:
                        log.warning("candle_cache parse_error msg_id=%s err=%s", msg_id, e)
                        continue
                    self.add_bar(ticker, ts, o, h, lo, c, v)
        log.info("candle_cache live subscriber stopped")
