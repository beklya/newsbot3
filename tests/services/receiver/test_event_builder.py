"""Unit tests for build_raw_event (pure logic, no I/O)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from src.services.receiver.event_builder import (
    TEXT_HARD_LIMIT,
    _normalize_text,
    _text_sha256,
    build_raw_event,
)


def _make_msg(
    text: str = "Test news headline.",
    msg_id: int = 1,
    date: datetime | None = None,
    media: object | None = None,
    is_reply: bool = False,
    fwd_from: object | None = None,
):
    """Mimic just enough of telethon's Message for build_raw_event."""
    if date is None:
        date = datetime(2026, 5, 7, 14, 0, 0, tzinfo=timezone.utc)
    return SimpleNamespace(
        message=text,
        id=msg_id,
        date=date,
        media=media,
        is_reply=is_reply,
        fwd_from=fwd_from,
    )


class BuildRawEventTests(unittest.TestCase):
    def test_returns_event_for_normal_message(self):
        event = build_raw_event(_make_msg("Hello, world!"), "rian_ru")
        self.assertIsNotNone(event)
        self.assertEqual(event.producer, "receiver")
        self.assertEqual(event.payload.channel, "@rian_ru")
        self.assertEqual(event.payload.message_id, 1)
        self.assertEqual(event.payload.text, "Hello, world!")
        self.assertEqual(len(event.payload.text_hash), 64)

    def test_returns_none_on_empty_text(self):
        self.assertIsNone(build_raw_event(_make_msg(""), "rian_ru"))
        self.assertIsNone(build_raw_event(_make_msg("   "), "rian_ru"))
        self.assertIsNone(build_raw_event(_make_msg("\n\n  \t"), "rian_ru"))

    def test_returns_none_when_date_missing(self):
        msg = _make_msg()
        msg.date = None
        self.assertIsNone(build_raw_event(msg, "rian_ru"))

    def test_text_hash_deterministic_across_channels(self):
        e1 = build_raw_event(_make_msg("Same text"), "rian_ru")
        e2 = build_raw_event(_make_msg("Same text", msg_id=999), "tass_agency")
        self.assertEqual(e1.payload.text_hash, e2.payload.text_hash)

    def test_text_hash_normalizes_whitespace(self):
        e1 = build_raw_event(_make_msg("Hello"), "rian_ru")
        e2 = build_raw_event(_make_msg("  Hello  "), "rian_ru")
        e3 = build_raw_event(_make_msg("\nHello\n"), "rian_ru")
        self.assertEqual(e1.payload.text_hash, e2.payload.text_hash)
        self.assertEqual(e1.payload.text_hash, e3.payload.text_hash)

    def test_text_truncated_to_hard_limit(self):
        long_text = "X" * 50_000
        event = build_raw_event(_make_msg(long_text), "rian_ru")
        self.assertEqual(len(event.payload.text), TEXT_HARD_LIMIT)

    def test_hash_is_pre_truncation(self):
        # Two long texts that share the first 10_000 chars but differ
        # past that point must still produce different hashes.
        long_a = ("X" * 10_000) + "AAAA"
        long_b = ("X" * 10_000) + "BBBB"
        e1 = build_raw_event(_make_msg(long_a), "rian_ru")
        e2 = build_raw_event(_make_msg(long_b), "rian_ru")
        self.assertNotEqual(e1.payload.text_hash, e2.payload.text_hash)

    def test_channel_prefixed_with_at(self):
        event = build_raw_event(_make_msg(), channel_username="interfaxonline")
        self.assertEqual(event.payload.channel, "@interfaxonline")

    def test_telethon_flags_passed_through(self):
        msg = _make_msg(media=object(), is_reply=True, fwd_from=object())
        event = build_raw_event(msg, "rian_ru")
        self.assertTrue(event.payload.has_media)
        self.assertTrue(event.payload.is_reply)
        self.assertTrue(event.payload.is_forward)

    def test_iso8601_utc_timestamps(self):
        event = build_raw_event(_make_msg(), "rian_ru")
        self.assertIn("T", event.payload.tg_published_at)
        self.assertTrue(event.payload.tg_published_at.endswith("+00:00"))
        self.assertIn("T", event.payload.received_at)
        # Both timestamps should have millisecond precision (.NNN+00:00)
        self.assertEqual(event.payload.tg_published_at[-10:-6], ".000")

    def test_event_id_is_unique_across_calls(self):
        e1 = build_raw_event(_make_msg("a"), "rian_ru")
        e2 = build_raw_event(_make_msg("b"), "rian_ru")
        self.assertNotEqual(e1.event_id, e2.event_id)


class HelperTests(unittest.TestCase):
    def test_normalize_strip_and_nfc(self):
        self.assertEqual(_normalize_text("  hello  "), "hello")
        # NFC: "é" decomposed (e + combining acute) -> precomposed
        self.assertEqual(_normalize_text("cafe\u0301"), "café")

    def test_sha256_hex_length(self):
        self.assertEqual(len(_text_sha256("hello")), 64)


if __name__ == "__main__":
    unittest.main()
