"""Tests for parse_llm_json — JSON extraction from arbitrary LLM output."""
from __future__ import annotations

from src.services.enricher.llm_client import parse_llm_json


def test_parse_clean_json():
    raw = '{"is_financial": true, "tickers": []}'
    assert parse_llm_json(raw) == {"is_financial": True, "tickers": []}


def test_parse_json_wrapped_in_markdown_fence():
    raw = '```json\n{"is_financial": true}\n```'
    assert parse_llm_json(raw) == {"is_financial": True}


def test_parse_json_wrapped_in_plain_fence():
    raw = '```\n{"is_financial": false}\n```'
    assert parse_llm_json(raw) == {"is_financial": False}


def test_parse_json_with_preamble():
    """LLM иногда вставляет 'Here is the JSON:' до объекта."""
    raw = 'Here is the analysis:\n{"is_financial": true, "summary": "X"}'
    parsed = parse_llm_json(raw)
    assert parsed is not None
    assert parsed["is_financial"] is True


def test_parse_nested_json():
    raw = '{"tickers": [{"ticker": "SBER", "direction": "long"}], "is_financial": true}'
    parsed = parse_llm_json(raw)
    assert parsed is not None
    assert parsed["tickers"][0]["ticker"] == "SBER"


def test_parse_invalid_json_returns_none():
    raw = "Not JSON at all — just plain text"
    assert parse_llm_json(raw) is None


def test_parse_empty_string_returns_none():
    assert parse_llm_json("") is None


def test_parse_truncated_json_returns_none():
    """Обрезанный ответ — не пытаемся починить, возвращаем None."""
    raw = '{"is_financial": true, "tickers": [{"ticker": "SB'
    assert parse_llm_json(raw) is None


def test_parse_json_with_trailing_garbage():
    """JSON-объект первым, потом мусор — должен извлечь объект."""
    raw = '{"is_financial": true}\nSome trailing comment'
    parsed = parse_llm_json(raw)
    assert parsed is not None
    assert parsed["is_financial"] is True


def test_parse_json_with_unicode():
    raw = '{"summary": "ЦБ повысил ставку", "is_financial": true}'
    parsed = parse_llm_json(raw)
    assert parsed is not None
    assert parsed["summary"] == "ЦБ повысил ставку"
