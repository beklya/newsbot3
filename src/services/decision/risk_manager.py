"""RiskManager — Redis-backed state for trade gating.

Decision READS state перед каждым trade_signal decision.
Bridge (5.3) WRITES state на каждый open/close (PnL ownership: Bridge).
Это устраняет race condition между двумя сервисами.

State:
  risk:open_positions          — SET с ticker'ами текущих открытых позиций
  risk:cooldown:<ticker>       — STRING (EX cooldown_ticker_sec), cooldown гейт
  risk:daily_pnl:<YYYY-MM-DD>  — STRING (FLOAT, INCRBYFLOAT atomic), expires next day

Decision проверяет:
  - SCARD risk:open_positions  ≤ max_open_positions
  - EXISTS risk:cooldown:<ticker> == False
  - |daily_pnl| < INITIAL_EQUITY × daily_kill_pct
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from redis.asyncio import Redis

log = logging.getLogger(__name__)


class RiskManager:
    """Read-only view of risk state from Decision perspective.

    Bridge (Sprint 5.3) реализует counterpart с SADD/SREM/INCRBYFLOAT.
    """

    def __init__(
        self,
        redis: Redis,
        open_positions_key: str,
        daily_pnl_key_prefix: str,
        cooldown_key_prefix: str,
        max_open_positions: int,
        daily_kill_pct: float,
        initial_equity_rub: float,
    ) -> None:
        self.redis = redis
        self.open_positions_key = open_positions_key
        self.daily_pnl_key_prefix = daily_pnl_key_prefix
        self.cooldown_key_prefix = cooldown_key_prefix
        self.max_open_positions = max_open_positions
        self.daily_kill_pct = daily_kill_pct
        self.initial_equity_rub = initial_equity_rub

    async def open_positions_count(self) -> int:
        n = await self.redis.scard(self.open_positions_key)
        return int(n or 0)

    async def is_cooldown_active(self, ticker: str) -> bool:
        key = f"{self.cooldown_key_prefix}{ticker}"
        exists = await self.redis.exists(key)
        return bool(exists)

    def _daily_pnl_key(self, when: Optional[datetime] = None) -> str:
        d = (when or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
        return f"{self.daily_pnl_key_prefix}{d}"

    async def daily_pnl_rub(self, when: Optional[datetime] = None) -> float:
        key = self._daily_pnl_key(when)
        raw = await self.redis.get(key)
        if raw is None:
            return 0.0
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return float(raw)
        except ValueError:
            log.warning("daily_pnl_parse_error key=%s raw=%r", key, raw)
            return 0.0

    async def daily_pnl_pct(self, when: Optional[datetime] = None) -> float:
        pnl = await self.daily_pnl_rub(when)
        return pnl / self.initial_equity_rub if self.initial_equity_rub > 0 else 0.0

    async def is_daily_kill_triggered(self, when: Optional[datetime] = None) -> bool:
        pnl_pct = await self.daily_pnl_pct(when)
        return abs(pnl_pct) >= self.daily_kill_pct
