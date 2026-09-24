"""Prompt loading and rendering for the Enricher.

Prompts live as Markdown files in `src/services/enricher/prompts/`.
The file format separates the system prompt (instructions, schema, rules,
few-shot examples) from the user template (where the actual news goes).

Format:
    <FILE>
    ## SYSTEM
    [system prompt content here]

    ## USER_TEMPLATE
    [user template with {headline}, {text}, {channel} placeholders]
    </FILE>

PromptBuilder reads the file once at startup, validates structure,
and renders the user template per news event.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

# Section markers in the prompt file.
SYSTEM_MARKER: Final[str] = "## SYSTEM"
USER_TEMPLATE_MARKER: Final[str] = "## USER_TEMPLATE"


@dataclass(frozen=True)
class RenderedPrompt:
    """Output of PromptBuilder.render() — ready to be sent to Groq."""

    system: str
    user: str


class PromptBuilder:
    """Loads and renders prompts from a versioned Markdown file."""

    def __init__(self, prompt_file: Path, version: str):
        self.prompt_file = prompt_file
        self.version = version
        self._system, self._user_template = self._load_and_parse()

    def _load_and_parse(self) -> tuple[str, str]:
        """Parse the prompt file into (system, user_template)."""
        if not self.prompt_file.exists():
            raise FileNotFoundError(f"Prompt file not found: {self.prompt_file}")

        text = self.prompt_file.read_text(encoding="utf-8")

        if SYSTEM_MARKER not in text:
            raise ValueError(
                f"Prompt file {self.prompt_file.name} missing '{SYSTEM_MARKER}' section"
            )
        if USER_TEMPLATE_MARKER not in text:
            raise ValueError(
                f"Prompt file {self.prompt_file.name} missing '{USER_TEMPLATE_MARKER}' section"
            )

        # Split: everything between SYSTEM_MARKER and USER_TEMPLATE_MARKER is system,
        # everything after USER_TEMPLATE_MARKER is user_template.
        sys_start = text.index(SYSTEM_MARKER) + len(SYSTEM_MARKER)
        usr_start = text.index(USER_TEMPLATE_MARKER)
        usr_end = len(text)

        system = text[sys_start:usr_start].strip()
        user_template = text[usr_start + len(USER_TEMPLATE_MARKER):usr_end].strip()

        # Validate required placeholders exist in user_template.
        required_placeholders = {"{headline}", "{text}"}
        missing = required_placeholders - set(re.findall(r"\{[a-z_]+\}", user_template))
        if missing:
            raise ValueError(
                f"USER_TEMPLATE in {self.prompt_file.name} missing placeholders: {missing}"
            )

        if not system:
            raise ValueError(f"SYSTEM section is empty in {self.prompt_file.name}")

        return system, user_template

    def render(self, headline: str, text: str, channel: str = "") -> RenderedPrompt:
        """Substitute news fields into the user template.

        Args:
            headline: New title (RawNewsPayload.text first line or message subject).
            text: Full news text (RawNewsPayload.text).
            channel: Source channel like '@interfaxonline' — optional in template.

        Returns:
            RenderedPrompt with system and user filled in.
        """
        # Trim — protect against multi-megabyte input
        headline_clean = (headline or "").strip()[:500]
        text_clean = (text or "").strip()[:3_000]
        channel_clean = (channel or "").strip()[:64]

        # Используем replace() вместо .format() — устойчиво к {...} в тексте новостей.
        # Реальные новости могут содержать JSON-фрагменты, URL с {}, и т.д.
        user = (
            self._user_template
            .replace("{headline}", headline_clean)
            .replace("{text}", text_clean)
            .replace("{channel}", channel_clean)
        )
        return RenderedPrompt(system=self._system, user=user)

    @property
    def system_preview(self) -> str:
        """First 200 chars of system prompt — for logging/debug."""
        return self._system[:200].replace("\n", " ")
