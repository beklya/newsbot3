"""Main loop quik_feed: poll source file → publish to candles:1m stream.

Algorithm:
  1. last_ts_per_ticker = {} (bootstrap from XREVRANGE on candles:1m to resume)
  2. while not shutdown:
     a. reader.read_new_bars(last_ts_per_ticker)
     b. for each new bar:
        - normalize ticker через instruments.try_normalize_ticker
        - skip if not in accepted_tickers
        - XADD к Redis `candles:1m`
        - update last_ts_per_ticker[bar.ticker]
        - track bar lag (now - bar.ts) для observability
     c. sleep poll_sec

Bar shape в Redis stream (fields):
  ticker (str canonical через instruments registry)
  ts     (ISO8601 naive MSK)
  open, high, low, close, volume (str-encoded float)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from redis.asyncio import Redis

from src.contracts.instruments import try_normalize_ticker

from .config import QuikFeedSettings
from .metrics import QuikFeedMetrics
from .readers import CandleBar, CandleReader

log = logging.getLogger(__name__)

# MSK = UTC+3 (без перехода на летнее время с 2014)
_MSK = timezone(timedelta(hours=3))


class QuikFeeder:
    """Poll loop: reader → Redis stream."""

    def __init__(
        self,
        *,
        redis: Redis,
        reader: CandleReader,
        settings: QuikFeedSettings,
        metrics: QuikFeedMetrics,
    ) -> None:
        self.redis = redis
        self.reader = reader
        self.settings = settings
        self.metrics = metrics
        self._accepted = frozenset(settings.accepted_tickers)
        self._last_ts: Dict[str, datetime] = {}
        self._seen: set[str] = set()                       # {canonical|ts_iso} за окно сверки
        self._reconcile_floor: Optional[datetime] = None    # бары старше → не сверяем

    async def bootstrap_from_redis(self) -> None:
        """Строим МНОЖЕСТВО уже опубликованных (canonical,ts) за окно сверки
        (reconcile_lookback_days) → screener в _publish_bar дозагружает ТОЛЬКО
        отсутствующие бары, включая дыры в СЕРЕДИНЕ (не только хвост). Плюс max-ts
        per-ticker для pre-фильтра reader'а. Корректный resume после restart.
        """
        now_msk = datetime.now(_MSK).replace(tzinfo=None)
        self._reconcile_floor = now_msk - timedelta(days=self.settings.reconcile_lookback_days)
        self._seen = set()
        try:
            entries = await self.redis.xrevrange(
                self.settings.candles_stream, count=self.settings.reconcile_scan_max,
            )
        except Exception as e:
            log.warning("bootstrap_xrevrange_failed err=%s", e)
            return
        for _msg_id, fields in entries:
            ticker = fields.get(b"ticker", b"").decode("utf-8", errors="ignore")
            ts_str = fields.get(b"ts", b"").decode("utf-8", errors="ignore")
            if not ticker or not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except ValueError:
                continue
            cur = self._last_ts.get(ticker)
            if cur is None or ts > cur:
                self._last_ts[ticker] = ts
            if ts >= self._reconcile_floor:                # множество для screener'а
                self._seen.add(f"{ticker}|{ts_str}")
        log.info(
            "bootstrap done: seen=%d (окно %dд от %s) tickers=%d",
            len(self._seen), self.settings.reconcile_lookback_days,
            self._reconcile_floor.date(), len(self._last_ts),
        )

    async def _publish_bar(self, bar: CandleBar) -> bool:
        """True если бар реально опубликован, False если отфильтрован (whitelist/барьер)."""
        # normalize ticker через registry; off-list → skip
        canonical = try_normalize_ticker(bar.ticker) or bar.ticker
        if canonical not in self._accepted and bar.ticker not in self._accepted:
            self.metrics.inc("bars_skipped_off_whitelist")
            return False
        # SCREENER пропущенных свечей: дозагружаем ТОЛЬКО отсутствующие бары по МНОЖЕСТВУ
        # (canonical,ts), а не по max-ts → дыра в СЕРЕДИНЕ дозагружается, а не игнорируется.
        # Старше окна сверки (reconcile_floor) → считаем опубликованным. Это же чинит
        # raw/canonical-рассинхрон (reader фильтрует по raw, bootstrap ключует по canonical).
        if self._reconcile_floor is not None and bar.ts < self._reconcile_floor:
            return False
        ts_iso = bar.ts.isoformat(timespec="seconds")
        key = f"{canonical}|{ts_iso}"
        if key in self._seen:
            self.metrics.inc("bars_dedup_skipped")
            return False
        # canonical preferred for downstream (CandleCache использует canonical keys)
        fields = {
            "ticker": canonical,
            "ts": ts_iso,
            "open": f"{bar.open:.6f}",
            "high": f"{bar.high:.6f}",
            "low": f"{bar.low:.6f}",
            "close": f"{bar.close:.6f}",
            "volume": f"{bar.volume:.2f}",
        }
        await self.redis.xadd(
            self.settings.candles_stream,
            fields=fields,
            maxlen=self.settings.candles_stream_maxlen,
            approximate=True,
        )
        if len(self._seen) < self.settings.reconcile_scan_max * 4:   # кап памяти
            self._seen.add(key)                 # screener: бар теперь опубликован
        self._last_ts[bar.ticker] = bar.ts     # raw-ключ — для pre-фильтра reader'а
        self._last_ts[canonical] = bar.ts       # canonical-ключ — консистентность с bootstrap
        self.metrics.inc("bars_published")
        # bar lag (now_msk - bar.ts) — наблюдаемая latency feed→Redis
        now_msk = datetime.now(_MSK).replace(tzinfo=None)
        lag_sec = max(0.0, (now_msk - bar.ts).total_seconds())
        self.metrics.record_bar_lag_sec(lag_sec)
        return True

    async def poll_once(self) -> int:
        """One poll cycle: read new bars, publish each. Returns n_published."""
        n_published = 0
        if not self.reader.source_exists():
            self.metrics.inc("polls_source_missing")
            return 0
        try:
            for bar in self.reader.read_new_bars(self._last_ts):
                if await self._publish_bar(bar):
                    n_published += 1
        except Exception as e:
            log.exception("poll_cycle_error err=%s", e)
            self.metrics.inc("errors.poll_cycle")
        self.metrics.inc("polls_total")
        return n_published

    async def run(self, shutdown: asyncio.Event) -> None:
        log.info(
            "feeder started source=%s poll_sec=%d stream=%s",
            self.settings.quik_feed_source_path.name,
            self.settings.quik_feed_poll_sec,
            self.settings.candles_stream,
        )
        await self.bootstrap_from_redis()
        while not shutdown.is_set():
            n = await self.poll_once()
            if n > 0:
                log.info("feed: published %d new bars", n)
            try:
                await asyncio.wait_for(
                    shutdown.wait(), timeout=self.settings.quik_feed_poll_sec,
                )
            except asyncio.TimeoutError:
                pass
        log.info("feeder stopped")
