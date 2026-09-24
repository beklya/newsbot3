"""Tests for filter_tickers_by_whitelist."""
from __future__ import annotations

import logging

from src.services.enricher.llm_client import filter_tickers_by_whitelist

ALLOWED = {"SBER", "GAZP", "Si", "MX", "BR"}


def test_whitelist_keeps_valid_tickers():
    tickers = [
        {"ticker": "SBER", "direction": "long"},
        {"ticker": "GAZP", "direction": "short"},
    ]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert len(result) == 2
    assert result[0]["ticker"] == "SBER"


def test_whitelist_drops_invalid_tickers(caplog):
    caplog.set_level(logging.WARNING)
    tickers = [
        {"ticker": "SBER", "direction": "long"},
        {"ticker": "INVALID", "direction": "short"},
        {"ticker": "MX", "direction": "long"},
    ]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert len(result) == 2
    assert {t["ticker"] for t in result} == {"SBER", "MX"}
    assert any("INVALID" in rec.message for rec in caplog.records)


def test_whitelist_empty_input_returns_empty():
    assert filter_tickers_by_whitelist([], ALLOWED, "evt-1") == []


def test_whitelist_all_invalid_returns_empty(caplog):
    caplog.set_level(logging.WARNING)
    tickers = [
        {"ticker": "FOO", "direction": "long"},
        {"ticker": "BAR", "direction": "short"},
    ]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert result == []
    assert len(caplog.records) == 2


def test_whitelist_non_dict_entry_dropped(caplog):
    """Если LLM вернул не-dict (например, строку), дроп с warning."""
    caplog.set_level(logging.WARNING)
    tickers = [
        {"ticker": "SBER", "direction": "long"},
        "not_a_dict",
        ["also", "not", "a", "dict"],
    ]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert len(result) == 1
    assert result[0]["ticker"] == "SBER"


def test_whitelist_missing_ticker_key_dropped(caplog):
    """Dict без 'ticker' — это пустая строка после .get, не в whitelist → дроп."""
    caplog.set_level(logging.WARNING)
    tickers = [
        {"direction": "long", "confidence": 0.5},  # нет ticker
        {"ticker": "SBER", "direction": "long"},
    ]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert len(result) == 1
    assert result[0]["ticker"] == "SBER"


def test_whitelist_case_sensitive():
    """Whitelist case-sensitive — 'sber' != 'SBER'."""
    tickers = [{"ticker": "sber", "direction": "long"}]
    result = filter_tickers_by_whitelist(tickers, ALLOWED, "evt-1")
    assert result == []
