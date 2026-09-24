"""Tests for PromptBuilder."""
from __future__ import annotations

import pytest
from pathlib import Path

from src.services.enricher.prompt import PromptBuilder


def _write_prompt(path: Path, body: str) -> Path:
    """Helper: write a prompt file and return its path."""
    path.write_text(body, encoding="utf-8")
    return path


def test_prompt_builder_loads_valid_file(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSystem prompt here.\n\n"
        "## USER_TEMPLATE\nHeadline: {headline}\nText: {text}",
    )
    pb = PromptBuilder(f, version="1.0.0")
    rendered = pb.render(headline="Test news", text="Full text")
    assert "System prompt here." in rendered.system
    assert "Test news" in rendered.user
    assert "Full text" in rendered.user


def test_prompt_builder_missing_file_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        PromptBuilder(tmp_path / "no_such.md", version="1.0.0")


def test_prompt_builder_missing_system_section_raises(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## USER_TEMPLATE\nHeadline: {headline}\nText: {text}",
    )
    with pytest.raises(ValueError, match="SYSTEM"):
        PromptBuilder(f, version="1.0.0")


def test_prompt_builder_missing_user_template_section_raises(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSystem prompt.",
    )
    with pytest.raises(ValueError, match="USER_TEMPLATE"):
        PromptBuilder(f, version="1.0.0")


def test_prompt_builder_missing_placeholders_raises(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSystem.\n\n## USER_TEMPLATE\nText: {text}",  # missing {headline}
    )
    with pytest.raises(ValueError, match="headline"):
        PromptBuilder(f, version="1.0.0")


def test_prompt_builder_empty_system_section_raises(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\n\n## USER_TEMPLATE\nHeadline: {headline}\nText: {text}",
    )
    with pytest.raises(ValueError, match="empty"):
        PromptBuilder(f, version="1.0.0")


def test_prompt_render_truncates_long_input(tmp_path: Path):
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSys.\n\n## USER_TEMPLATE\nHeadline: {headline}\nText: {text}",
    )
    pb = PromptBuilder(f, version="1.0.0")
    long_text = "A" * 5_000
    rendered = pb.render(headline="H", text=long_text)
    # text trimmed to 3_000 chars
    assert rendered.user.count("A") == 3_000


def test_prompt_render_optional_channel_placeholder(tmp_path: Path):
    """Channel is optional in template — when absent, still renders."""
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSys.\n\n## USER_TEMPLATE\nHeadline: {headline}\nText: {text}\nChannel: {channel}",
    )
    pb = PromptBuilder(f, version="1.0.0")
    rendered = pb.render(headline="H", text="T", channel="@interfax")
    assert "@interfax" in rendered.user


def test_prompt_render_handles_braces_in_news_safely(tmp_path: Path):
    """News text containing { or } should NOT break rendering — used to crash on .format()."""
    f = _write_prompt(
        tmp_path / "v1.md",
        "## SYSTEM\nSys.\n\n## USER_TEMPLATE\nH: {headline}\nT: {text}",
    )
    pb = PromptBuilder(f, version="1.0.0")
    # Реальная новость может содержать {key:value} (JSON-фрагмент), формулы,
    # технические URL — рендер должен работать.
    rendered = pb.render(headline="H", text="Some {unknown} format text")
    assert "Some {unknown} format text" in rendered.user
    assert "H: H" in rendered.user
