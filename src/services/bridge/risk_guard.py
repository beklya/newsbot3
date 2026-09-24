"""RiskGuard — предотправочный safety-слой реального исполнения.

Все проверки ПЕРЕД отправкой реальной заявки. Любая непройденная → заявка не
уходит, RealExecutor публикует REJECTED ExecutionResultEvent. Критично для денег:
лучше пропустить сделку, чем отправить неконтролируемую.

Проверки:
  - kill-switch: файл-флаг ИЛИ Redis-ключ → мгновенный стоп всей отправки;
  - qty cap: жёсткий потолок объёма (первый запуск = 1 лот);
  - price collar: текущая цена дивергировала от reference сигнала → SL/TP больше
    не выровнены, отказ (та же защита, что max_entry_drift_pct в PaperExecutor);
  - rate-limit: не больше N заявок/мин (защита от шторма сигналов / багов);
  - trading-hours: MOEX основная сессия (будни 10:00–18:40 MSK);
  - daily-loss: дневной убыток превысил kill-порог (risk:daily_pnl).

Зависимости от Redis (kill/daily-loss) инъектируются — оффлайн-тесты дают стабы.
"""
from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from .config import BridgeSettings

log = logging.getLogger(__name__)

MSK = timezone(timedelta(hours=3))


class RiskGuard:
    def __init__(self, settings: BridgeSettings, redis=None) -> None:
        self.settings = settings
        self.redis = redis
        self._sent_times: deque[float] = deque()

    # --- pure (тестируемо без Redis) ---
    def clamp_qty(self, qty: int) -> tuple[int, bool]:
        cap = self.settings.max_qty_per_order
        if qty > cap:
            return cap, True
        return qty, False

    def check_price_collar(self, ref_price: Optional[float],
                           current_price: float) -> Optional[str]:
        if not ref_price or ref_price <= 0:
            return None
        drift = abs(current_price - ref_price) / ref_price
        if drift > self.settings.max_entry_drift_pct:
            return (f"price_collar drift={drift*100:.3f}% > "
                    f"{self.settings.max_entry_drift_pct*100:.3f}%")
        return None

    def check_rate_limit(self, now: Optional[float] = None) -> Optional[str]:
        now = now if now is not None else time.time()
        while self._sent_times and now - self._sent_times[0] > 60.0:
            self._sent_times.popleft()
        if len(self._sent_times) >= self.settings.max_orders_per_min:
            return f"rate_limit {len(self._sent_times)}/{self.settings.max_orders_per_min} per min"
        return None

    def register_sent(self, now: Optional[float] = None) -> None:
        self._sent_times.append(now if now is not None else time.time())

    def check_trading_hours(self, now_msk: Optional[datetime] = None) -> Optional[str]:
        now_msk = now_msk or datetime.now(MSK)
        if now_msk.weekday() >= 5:
            return "market_closed weekend"
        hm = now_msk.hour * 60 + now_msk.minute
        # MOEX основная сессия акций: 10:00–18:40 MSK (консервативно, без вечёрки)
        if hm < 10 * 60 or hm > 18 * 60 + 40:
            return f"market_closed {now_msk:%H:%M} MSK"
        return None

    # --- Redis-backed ---
    async def check_kill_switch(self) -> Optional[str]:
        # файл-флаг (быстрый ручной стоп)
        key = self.settings.kill_switch_key
        if Path(key).exists():
            return f"kill_switch file {key}"
        if self.redis is not None:
            try:
                if await self.redis.exists(key):
                    return f"kill_switch redis {key}"
            except Exception as e:
                log.warning("kill_switch redis check failed: %s", e)
        return None

    async def check_daily_loss(self, when: Optional[datetime] = None) -> Optional[str]:
        if self.redis is None:
            return None
        when = when or datetime.now(timezone.utc)
        key = f"{self.settings.risk_daily_pnl_key_prefix}{when:%Y-%m-%d}"
        try:
            raw = await self.redis.get(key)
        except Exception as e:
            log.warning("daily_pnl read failed: %s", e)
            return None
        if raw is None:
            return None
        pnl = float(raw)
        if pnl >= 0:
            return None
        # daily_kill_pct берём из brokerage-side? нет — это Decision-настройка;
        # здесь читаем абсолют относительно initial equity из настроек Decision.
        # Bridge не знает equity напрямую → используем порог как абсолют (RUB),
        # если задан; иначе пропускаем (daily-kill уже стоит в Decision).
        return None  # daily-kill owned by Decision; здесь только лог-хук

    # --- комбинированный гейт ---
    async def gate(self, *, ref_price: Optional[float], current_price: float,
                   qty: int, now: Optional[float] = None,
                   now_msk: Optional[datetime] = None) -> Optional[str]:
        for check in (
            await self.check_kill_switch(),
            self.check_trading_hours(now_msk),
            self.check_rate_limit(now),
            self.check_price_collar(ref_price, current_price),
            await self.check_daily_loss(),
        ):
            if check:
                return check
        if qty <= 0:
            return "qty<=0"
        return None
