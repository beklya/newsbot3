"""Build RawNewsEvent from a Telethon Message.

Pure module: no Redis, no Telegram I/O, no logging side effects.
The only inherent side effect is reading the wall clock for `received_at`.
"""

from __future__ import annotations

import hashlib
import unicodedata
from datetime import timezone
from typing import Any, Optional

from src.contracts.base import utcnow_iso
from src.contracts.raw_news import RawNewsEvent, RawNewsPayload

# Hard cap dictated by the RawNewsPayload contract.
# See src/contracts/raw_news.py: text: Field(..., max_length=10_000).
TEXT_HARD_LIMIT: int = 10_000


def _normalize_text(text: str) -> str:
    """Canonical form for hashing: strip outer whitespace and Unicode-NFC."""
    return unicodedata.normalize("NFC", text.strip())


def _text_sha256(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_raw_event(
    msg: Any,
    channel_username: str,
    max_text_length: int = TEXT_HARD_LIMIT,
) -> Optional[RawNewsEvent]:
    """Build a validated RawNewsEvent from a Telethon Message.

    Returns None when the message has no usable text (empty / whitespace)
    or no server-side timestamp.

    The text_hash is computed from the *full* normalized text BEFORE
    truncation. This way two reposts of the same long article — even
    if Telegram truncates them at different positions — collide on hash
    and the second one is deduplicated.
    """
    raw_text = msg.message or ""
    canonical = _normalize_text(raw_text)
    if not canonical:
        return None

    if msg.date is None:
        # Without a server-side timestamp the event is meaningless for
        # downstream sentiment analysis; skip.
        return None

    text_hash = _text_sha256(canonical)
    effective_limit = min(int(max_text_length), TEXT_HARD_LIMIT)
    text_for_payload = canonical[:effective_limit]

    tg_published_at = msg.date.astimezone(timezone.utc).isoformat(
        timespec="milliseconds"
    )

    payload = RawNewsPayload(
        channel=f"@{channel_username}",
        message_id=int(msg.id),
        text=text_for_payload,
        tg_published_at=tg_published_at,
        received_at=utcnow_iso(),
        text_hash=text_hash,
        has_media=bool(getattr(msg, "media", None)),
        is_reply=bool(getattr(msg, "is_reply", False)),
        is_forward=bool(getattr(msg, "fwd_from", None)),
    )

    return RawNewsEvent(payload=payload)
