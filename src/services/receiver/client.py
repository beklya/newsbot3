"""Telethon client wrapper for the receiver service.

Sprint 2 / Commit 5 — adds heartbeat publishing.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from redis.asyncio import Redis
from telethon import TelegramClient, events

from src.infra.idempotency import IdempotencyGuard
from src.infra.publisher import StreamPublisher
from src.infra.redis_factory import make_redis
from src.services.receiver.config import ReceiverSettings
from src.services.receiver.event_builder import build_raw_event
from src.infra.heartbeat import HeartbeatPublisher
from src.services.receiver.telegram_health import TelegramHealthMonitor

log = logging.getLogger(__name__)


class ReceiverClient:
    """Owns Telethon, Redis, Publisher, Guard, Heartbeat, and dispatch loop."""

    def __init__(self, settings: ReceiverSettings) -> None:
        self.settings = settings
        self.settings.session_path.parent.mkdir(parents=True, exist_ok=True)

        # Sprint 5.10: optional outbound SOCKS5 proxy для Telethon.
        # resolve_proxy_tuple() возвращает None если proxy_enabled=False,
        # тогда Telethon коннектится напрямую как раньше.
        proxy_tuple = self.settings.resolve_proxy_tuple()
        if proxy_tuple is not None:
            # Не логируем username/password, только тип+host+port для аудита.
            log.info(
                "telethon: using proxy type=%s host=%s port=%d rdns=%s",
                proxy_tuple[0], proxy_tuple[1], proxy_tuple[2], proxy_tuple[3],
            )

        self.client = TelegramClient(
            session=str(self.settings.session_path),
            api_id=self.settings.tg_api_id,
            api_hash=self.settings.tg_api_hash,
            proxy=proxy_tuple,
        )

        self._redis: Optional[Redis] = None
        self._publisher: Optional[StreamPublisher] = None
        self._guard: Optional[IdempotencyGuard] = None
        self._heartbeat: Optional[HeartbeatPublisher] = None

        self._entities: Dict[str, Any] = {}

        # Counters — exposed via _snapshot() for heartbeat
        self.published_count: int = 0
        self.deduped_count: int = 0
        self.error_count: int = 0
        self.empty_count: int = 0

        # Sprint 5.9 — Telethon MTProto health observer (hooks into Telethon
        # loggers, no monkey-patching of internals). Storm detection: 10+
        # reconnects in 60s. Exposed via heartbeat snapshot for Monitor.
        self._tg_health = TelegramHealthMonitor()

    # ------------------------------------------------------------------
    # Snapshot for heartbeat
    # ------------------------------------------------------------------

    def _snapshot(self) -> Dict[str, int]:
        snap: Dict[str, int] = {
            "published": self.published_count,
            "deduped": self.deduped_count,
            "empty": self.empty_count,
            "errors": self.error_count,
            "channels": len(self._entities),
        }
        snap.update(self._tg_health.snapshot())
        return snap

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        # Telegram
        await self.client.start(phone=self.settings.tg_phone)
        log.info("telethon: authorized as %s", self.settings.tg_phone)

        # Redis / Memurai
        self._redis = make_redis(self.settings.redis_url)
        try:
            pong = await self._redis.ping()
            log.info("redis: connected (%s) ping=%s", self.settings.redis_url, pong)
        except Exception as e:
            await self.client.disconnect()
            raise RuntimeError(
                f"Redis ping failed at {self.settings.redis_url}: {e}"
            ) from e

        self._publisher = StreamPublisher(
            redis=self._redis,
            stream=self.settings.raw_news_stream,
        )
        self._guard = IdempotencyGuard(
            redis=self._redis,
            ttl_seconds=self.settings.idempotency_ttl_sec,
        )

        # Channel entities
        for username in self.settings.channels:
            try:
                entity = await self.client.get_entity(username)
                self._entities[username] = entity
                title = getattr(entity, "title", username)
                log.info("channel resolved: @%s -> %s", username, title)
            except Exception as e:
                log.error("channel resolve FAILED: @%s: %s", username, e)

        if not self._entities:
            raise RuntimeError(
                "No channels resolved — aborting. Check credentials / channel names."
            )

        log.info(
            "channels ready: %d / %d",
            len(self._entities),
            len(self.settings.channels),
        )

        # Heartbeat — start AFTER everything is wired and channels resolved
        self._heartbeat = HeartbeatPublisher(
            redis=self._redis,
            stream=self.settings.heartbeat_stream,
            producer=self.settings.producer_name,
            interval_sec=self.settings.heartbeat_interval_sec,
            snapshot_fn=self._snapshot,
        )
        self._heartbeat.start()

    async def close(self) -> None:
        log.info(
            "shutdown counters: published=%d deduped=%d empty=%d errors=%d",
            self.published_count,
            self.deduped_count,
            self.empty_count,
            self.error_count,
        )

        # Stop heartbeat first so it doesn't race the Redis client closing
        if self._heartbeat is not None:
            try:
                await self._heartbeat.stop()
            except Exception as e:
                log.warning("heartbeat stop raised: %s", e)

        # Detach Telegram health observer.
        try:
            self._tg_health.close()
        except Exception as e:
            log.warning("tg_health close raised: %s", e)

        try:
            await self.client.disconnect()
        except Exception as e:
            log.warning("telethon disconnect raised: %s", e)

        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception as e:
                log.warning("redis aclose raised: %s", e)

    # ------------------------------------------------------------------
    # Backfill
    # ------------------------------------------------------------------

    async def backfill(self, hours: int) -> int:
        if hours <= 0:
            log.info("backfill: skipped (hours=%d)", hours)
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        log.info("backfill: cutoff=%s", cutoff.isoformat())

        total = 0
        for username, entity in self._entities.items():
            channel_count = 0
            async for msg in self.client.iter_messages(entity, reverse=False):
                if msg.date is None or msg.date < cutoff:
                    break
                await self._dispatch(msg, username, source="backfill")
                channel_count += 1
            log.info("backfill: @%s -> %d messages", username, channel_count)
            total += channel_count

        log.info("backfill: dispatched=%d", total)
        return total

    # ------------------------------------------------------------------
    # Live listening
    # ------------------------------------------------------------------

    async def listen(self) -> None:
        entities = list(self._entities.values())

        @self.client.on(events.NewMessage(chats=entities))
        async def _on_new(event):  # noqa: ANN001
            username = self._username_of(event.chat)
            await self._dispatch(event.message, username, source="live")

        if self.settings.handle_edited_messages:
            @self.client.on(events.MessageEdited(chats=entities))
            async def _on_edit(event):  # noqa: ANN001
                username = self._username_of(event.chat)
                await self._dispatch(event.message, username, source="edit")

        log.info(
            "listening on %d channels, edited=%s",
            len(entities),
            self.settings.handle_edited_messages,
        )
        await self.client.run_until_disconnected()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _username_of(chat: Any) -> str:
        return getattr(chat, "username", None) or "unknown"

    async def _dispatch(self, msg: Any, channel: str, source: str) -> None:
        """Build RawNewsEvent, dedup via guard, publish to Redis."""
        assert self._publisher is not None and self._guard is not None, (
            "client not connected; call connect() first"
        )

        t_start = time.perf_counter()

        try:
            event = build_raw_event(
                msg,
                channel_username=channel,
                max_text_length=self.settings.max_text_length,
            )
        except Exception as e:
            self.error_count += 1
            log.error(
                "[%s] @%s msg_id=%s build FAILED: %s",
                source, channel, getattr(msg, "id", "?"), e,
            )
            return

        if event is None:
            self.empty_count += 1
            return

        text_hash = event.payload.text_hash
        try:
            claimed = await self._guard.claim(
                scope=self.settings.idempotency_scope,
                key=text_hash,
            )
        except Exception as e:
            self.error_count += 1
            log.error(
                "[%s] @%s msg_id=%d guard FAILED: %s",
                source, channel, msg.id, e,
            )
            return

        if not claimed:
            self.deduped_count += 1
            log.info(
                "[%s] @%s msg_id=%d hash=%s deduped",
                source, channel, msg.id, text_hash[:12],
            )
            return

        try:
            redis_msg_id = await self._publisher.publish(event)
        except Exception as e:
            self.error_count += 1
            log.error(
                "[%s] @%s msg_id=%d publish FAILED: %s",
                source, channel, msg.id, e,
            )
            return

        latency_ms = (time.perf_counter() - t_start) * 1000.0
        self.published_count += 1
        snippet = event.payload.text.replace("\n", " ")[:60]
        xid_str = (
            redis_msg_id.decode() if isinstance(redis_msg_id, (bytes, bytearray))
            else str(redis_msg_id)
        )
        log.info(
            "[%s] @%s msg_id=%d hash=%s xid=%s lat=%.1fms | %s",
            source, channel, msg.id, text_hash[:12], xid_str, latency_ms, snippet,
        )
