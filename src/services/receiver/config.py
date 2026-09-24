"""Configuration for the Telegram receiver service.

All values can be overridden via environment variables (case-insensitive)
or via a `.env` file at the project root. See `.env.example` for the
authoritative list of variables.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import unquote, urlparse

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root resolved from this file's location, not cwd.
# This file lives at <root>/src/services/receiver/config.py, so
# parents[3] climbs out of (receiver -> services -> src -> root).
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"


class ReceiverSettings(BaseSettings):
    """Settings loaded from environment / .env file.

    Required (no defaults): TG_API_ID, TG_API_HASH, TG_PHONE.
    Everything else has sensible defaults for production.
    """

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Telegram credentials (https://my.telegram.org) ---
    tg_api_id: int = Field(..., description="Telegram API ID")
    tg_api_hash: str = Field(..., description="Telegram API hash")
    tg_phone: str = Field(..., description="Phone with country code, e.g. +7...")

    # --- Telegram session ---
    session_path: Path = Field(
        default=PROJECT_ROOT / "data" / "sessions" / "receiver.session",
        description="Persistent Telethon session file (gitignore!)",
    )

    # --- Channels (without @ prefix; receiver adds it when building event) ---
    channels: List[str] = Field(
        default=[
            "interfaxonline",
            "rian_ru",
            "tass_agency",
            "rbc_news",
        ],
        description="Telegram channel usernames",
    )

    # --- Redis / Memurai ---
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Memurai/Redis URL",
    )

    # --- Streams and scopes ---
    raw_news_stream: str = Field(default="news:raw")
    heartbeat_stream: str = Field(default="system:heartbeats")
    idempotency_scope: str = Field(default="news_text")
    idempotency_ttl_sec: int = Field(default=86400, description="24h dedup window")

    # --- Behavior ---
    heartbeat_interval_sec: int = Field(default=30)
    backfill_hours: int = Field(
        default=0,
        ge=0,
        le=168,
        description="On startup, fetch last N hours of history. 0 = realtime only.",
    )
    # Hard upper bound here matches RawNewsPayload.text max_length=10_000.
    # Don't bump this without also bumping the contract schema_version.
    max_text_length: int = Field(
        default=10_000,
        ge=100,
        le=10_000,
        description="Truncate message text. Hard cap from RawNewsPayload contract.",
    )
    handle_edited_messages: bool = Field(
        default=True,
        description="If True, edited messages produce a new RawNewsEvent",
    )

    # --- Producer identity ---
    producer_name: str = Field(default="receiver")
    producer_version: str = Field(default="2.0.0")

    # --- SOCKS5 proxy (Sprint 5.10) ---
    # Опциональный исходящий прокси для соединений Telethon.
    # Format: socks5://user:pass@host:port. proxy_enabled=False — поле игнорируется.
    proxy_enabled: bool = Field(
        default=False,
        description="If True, route Telethon connections via PROXY_URL (SOCKS5).",
    )
    proxy_url: str = Field(
        default="",
        description="SOCKS5 URL: socks5://user:pass@host:port. Empty unless proxy_enabled.",
    )

    def resolve_proxy_tuple(self) -> Optional[Tuple]:
        """Парсит proxy_url в Telethon-формат tuple.

        Telethon 1.x принимает tuple вида:
            (proxy_type, host, port, rdns_bool, username, password)
        где proxy_type это строка 'socks5'/'socks4'/'http' или соответствующая
        константа из socks (PySocks). Используем строки — Telethon их понимает
        и не требует import socks в этом модуле.

        Возвращает None если proxy_enabled=False или proxy_url пуст.
        """
        if not self.proxy_enabled or not self.proxy_url:
            return None
        parsed = urlparse(self.proxy_url)
        scheme = (parsed.scheme or "").lower()
        if scheme not in ("socks5", "socks5h", "socks4", "http"):
            raise ValueError(
                f"Unsupported proxy scheme {scheme!r}; expected socks5/socks5h/socks4/http"
            )
        if not parsed.hostname or not parsed.port:
            raise ValueError(
                f"PROXY_URL must contain host and port; got {self.proxy_url!r}"
            )
        # socks5h means "do DNS through proxy" — Telethon resolves remotely when
        # rdns=True, so map socks5h → socks5 with rdns=True. Plain socks5 also
        # supports rdns toggle; we default to True (safer: DNS through proxy too).
        proxy_type = "socks5" if scheme in ("socks5", "socks5h") else scheme
        rdns = True
        # urlparse не декодирует %xx в username/password — делаем сами.
        # Иначе пароль вида "p%40ss" (URL-encoded "p@ss") пойдёт в PySocks
        # буквально как "p%40ss" и auth провалится.
        username = unquote(parsed.username) if parsed.username else ""
        password = unquote(parsed.password) if parsed.password else ""
        return (
            proxy_type,
            parsed.hostname,
            parsed.port,
            rdns,
            username,
            password,
        )


def load_settings() -> ReceiverSettings:
    """Instantiate ReceiverSettings with a friendly error if .env is missing."""
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env file not found at: {ENV_FILE}\n"
            f"Quick fix from project root:\n"
            f"    copy .env.example .env\n"
        )
    return ReceiverSettings()  # type: ignore[call-arg]