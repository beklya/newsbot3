"""Groq LLM client pool.

Пул LLM-клиентов с per-client cooldown/failover на 429.

Стратегия:
- По одному AsyncGroq клиенту на каждый сконфигурированный API-ключ.
- Round-robin при выборе следующего клиента.
- При 429 — помечаем клиент как "cooling down" до конкретного timestamp
  (читаем из заголовка Retry-After если есть, иначе 60 сек дефолт).
- Если все клиенты cooling down — ждём, пока освободится ближайший.

Threadsafe не нужен (внутри одного asyncio-loop).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Sequence

import httpx
from groq import AsyncGroq, RateLimitError

log = logging.getLogger(__name__)

DEFAULT_COOLDOWN_SEC = 60.0  # fallback если нет Retry-After


@dataclass
class _KeySlot:
    """Один API-ключ с состоянием cooldown."""
    key_id: int               # 0, 1, 2 — для логов
    client: AsyncGroq
    cooldown_until: float = 0.0   # monotonic timestamp; 0 = ready
    # Sprint 5.10: если AsyncGroq был инициализирован с кастомным http_client
    # (для SOCKS5 proxy), храним ссылку чтобы корректно закрыть в pool.close().
    # AsyncGroq.close() сам не закрывает переданный extern http_client.
    http_client: Optional[httpx.AsyncClient] = field(default=None, repr=False)

    def is_ready(self, now: float) -> bool:
        return self.cooldown_until <= now

    def set_cooldown(self, seconds: float, now: float) -> None:
        self.cooldown_until = now + max(seconds, 1.0)


class GroqKeyPool:
    """Пул AsyncGroq клиентов с round-robin и cooldown на 429.

    Использование:
        pool = GroqKeyPool(api_keys, timeout_sec=10)
        async with pool.acquire() as client:
            resp = await client.chat.completions.create(...)
        # При RateLimitError: pool.mark_rate_limited(client, retry_after)
    """

    def __init__(
        self,
        api_keys: Sequence[str],
        timeout_sec: float = 10.0,
        proxy_url: Optional[str] = None,
    ):
        """Создаёт пул AsyncGroq клиентов.

        proxy_url: если задан (например "socks5://user:pass@host:1080"), каждый
        слот получает свой httpx.AsyncClient с SOCKS5-транспортом и AsyncGroq
        инициализируется с http_client=этим клиентом. По одному httpx на ключ
        чтобы избежать shared connection pool issues (Groq SDK ожидает
        эксклюзивный http_client).
        """
        if not api_keys:
            raise ValueError("GroqKeyPool requires at least one API key")
        self._slots: list[_KeySlot] = []
        for i, k in enumerate(api_keys):
            http_client: Optional[httpx.AsyncClient] = None
            if proxy_url:
                # httpx 0.28+ принимает строку SOCKS5 через socksio extra.
                # AsyncHTTPTransport создаёт SOCKS-совместимый транспорт.
                http_client = httpx.AsyncClient(
                    transport=httpx.AsyncHTTPTransport(proxy=proxy_url),
                    timeout=timeout_sec,
                )
            # max_retries=0 — критично: иначе Groq SDK сам делает retry на 429
            # с собственным backoff (до 30 сек), и пул не успевает переключить ключ.
            # Мы обрабатываем 429 сами в GroqLLMClient._call_with_retry.
            kwargs = {"api_key": k, "timeout": timeout_sec, "max_retries": 0}
            if http_client is not None:
                kwargs["http_client"] = http_client
            self._slots.append(_KeySlot(
                key_id=i,
                client=AsyncGroq(**kwargs),
                http_client=http_client,
            ))
        self._cursor: int = 0  # round-robin pointer
        log.info(
            "GroqKeyPool initialized n_keys=%d timeout_sec=%.1f sdk_retries=disabled proxy=%s",
            len(self._slots), timeout_sec, "on" if proxy_url else "off",
        )

    def stats(self) -> dict[str, int | float]:
        """Snapshot для логов/heartbeat: сколько ключей готово / на cooldown."""
        now = time.monotonic()
        ready = sum(1 for s in self._slots if s.is_ready(now))
        cooldown = len(self._slots) - ready
        # Ближайшее освобождение
        earliest_ready_in = 0.0
        if cooldown > 0:
            earliest_ts = min(s.cooldown_until for s in self._slots if not s.is_ready(now))
            earliest_ready_in = max(earliest_ts - now, 0.0)
        return {
            "n_total": len(self._slots),
            "n_ready": ready,
            "n_cooldown": cooldown,
            "earliest_ready_in_sec": earliest_ready_in,
        }

    @property
    def size(self) -> int:
        return len(self._slots)

    async def acquire(self) -> AsyncGroq:
        """Возвращает готовый клиент. Ждёт, если все на cooldown."""
        while True:
            now = time.monotonic()

            # Сначала пробуем round-robin от текущего cursor
            for offset in range(len(self._slots)):
                idx = (self._cursor + offset) % len(self._slots)
                slot = self._slots[idx]
                if slot.is_ready(now):
                    self._cursor = (idx + 1) % len(self._slots)
                    return slot.client

            # Все на cooldown — ждём ближайшего
            earliest = min(s.cooldown_until for s in self._slots)
            wait = max(earliest - now, 0.5)
            log.warning(
                "all_keys_rate_limited waiting=%.1fs n_keys=%d",
                wait, len(self._slots),
            )
            await asyncio.sleep(wait)

    def mark_rate_limited(
        self,
        client: AsyncGroq,
        retry_after_sec: float | None = None,
    ) -> None:
        """Помечает ключ как cooling down. retry_after из ответа Groq."""
        cooldown = retry_after_sec if retry_after_sec is not None else DEFAULT_COOLDOWN_SEC
        now = time.monotonic()
        for slot in self._slots:
            if slot.client is client:
                slot.set_cooldown(cooldown, now)
                log.warning(
                    "key_rate_limited key_id=%d cooldown_sec=%.1f",
                    slot.key_id, cooldown,
                )
                return
        log.error("mark_rate_limited called for unknown client")

    async def close(self) -> None:
        """Закрыть все HTTP-сессии (AsyncGroq + опциональный httpx.AsyncClient)."""
        for slot in self._slots:
            try:
                await slot.client.close()
            except Exception:
                log.exception("error closing groq client key_id=%d", slot.key_id)
            # Sprint 5.10: AsyncGroq не закрывает extern http_client сам.
            if slot.http_client is not None:
                try:
                    await slot.http_client.aclose()
                except Exception:
                    log.exception("error closing httpx client key_id=%d", slot.key_id)


def extract_retry_after(exc: RateLimitError) -> float | None:
    """Парсит Retry-After из RateLimitError ответа Groq.

    Groq возвращает либо число секунд, либо HTTP-date. Берём только секунды
    (HTTP-date — редкий случай, дефолт DEFAULT_COOLDOWN_SEC покроет).
    """
    try:
        response = getattr(exc, "response", None)
        if response is None:
            return None
        headers = getattr(response, "headers", {})
        retry_after = headers.get("retry-after") or headers.get("Retry-After")
        if retry_after is None:
            return None
        return float(retry_after)
    except (TypeError, ValueError):
        return None
