# src/contracts/enriched_news.py
"""
EnrichedNewsEvent v1.1.0
========================

Изменения vs 1.0.0:
- producer default: "analyzer" -> "enricher"
- Добавлено: prompt_version (required)
- Добавлено: is_financial (required) — разделяет "не финансовая" от "LLM не справился"
- Добавлено: expected_timeframe (required) — горизонт ожидаемой реакции
- Добавлено: urgency (default "medium") — приоритет в очереди Decision Service
- Добавлено: category (default "other") — для аналитики
- Добавлено: is_actionable (default False) — факт vs прогноз/репортаж
- llm_raw_response max_length: 5_000 -> 10_000

Migration:
- BREAKING: новые required-поля. Consumer'ы 1.0.0 не смогут читать 1.1.0.
- Старые сообщения в Redis Stream news:enriched (если есть) нужно дренировать.

Sprint 4 / Commit 4.1 — validator-based ticker normalization:
- TickerImpact.ticker теперь нормализуется через registry (instruments.py)
- Legacy имена (Si/MX/YNDX/GOLD) автоматически конвертятся в canonical (SI/MIX/YDEX/GLDRUB)
- Unknown ticker -> ValidationError -> событие уходит в DLQ как schema_violation
- Контракт остаётся v1.1.0, version bump НЕ нужен — это soft compatibility layer
"""
import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .base import MessageEnvelope
from .instruments import CANONICAL_TICKERS, try_normalize_ticker

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1.1.0"

Sentiment = Literal["positive", "negative", "neutral"]
Direction = Literal["long", "short", "neutral"]
ExpectedTimeframe = Literal["instant", "short", "medium", "slow"]
Urgency = Literal["high", "medium", "low"]
Category = Literal[
    "geopolitics",
    "macro",
    "cbr",
    "corporate",
    "commodity",
    "currency",
    "market",
    "other",
]


class TickerImpact(BaseModel):
    """Один тикер из LLM-анализа.

    sentiment vs direction:
        sentiment — как новость "звучит" для компании/инструмента
                    (positive = хорошая для бизнеса).
        direction — что делать с позицией (long = покупать).
        В 80% случаев они совпадают. Расходятся в кейсах sell-the-news
        (хорошая новость, но рынок ждал большего → direction=short).

    Ticker normalization (Sprint 4 / Commit 4.1):
        ticker нормализуется через instruments registry:
        - Legacy ("Si", "MX", "YNDX", "GOLD") -> canonical ("SI", "MIX", "YDEX", "GLDRUB")
        - Unknown ticker -> ValidationError
        - Canonical имена возвращаются без изменений (idempotent)
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    ticker: str = Field(
        ...,
        description=(
            "MOEX ticker из whitelist. Принимает canonical (SBER, SI, MIX, YDEX) "
            "и legacy Phase 2 имена (Si, MX, YNDX, GOLD) — последние нормализуются "
            "в canonical через registry."
        ),
    )
    direction: Direction = Field(..., description="Торговое направление (long/short/neutral)")
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Уверенность LLM в направлении, 0-1"
    )
    sentiment: Sentiment = Field(..., description="Тон новости для тикера (positive/negative/neutral)")
    impact_strength: float = Field(
        ..., ge=0.0, le=1.0, description="Ожидаемая сила реакции цены, 0-1"
    )
    rationale: str = Field(
        "", max_length=500, description="1-2 фразы: почему такой direction"
    )

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_ticker_field(cls, v: str) -> str:
        """
        Нормализует ticker через instruments registry.

        - Canonical имя ("SBER", "SI", ...) -> возвращается как есть
        - Legacy имя ("Si", "MX", "YNDX", "GOLD") -> canonical
        - Unknown -> ValueError -> Pydantic ValidationError -> DLQ schema_violation

        Lowercase / mixed case (например "sber") НЕ нормализуется — это считается
        unknown ticker. Так мы защищаемся от LLM-галлюцинаций со странным форматом.
        """
        if not isinstance(v, str):
            raise ValueError(f"ticker must be str, got {type(v).__name__}")

        normalized = try_normalize_ticker(v)
        if normalized is None:
            raise ValueError(
                f"Unknown ticker {v!r}. "
                f"Allowed canonical: {sorted(CANONICAL_TICKERS)}. "
                f"Legacy mappings: Si->SI, MX->MIX, YNDX->YDEX, GOLD->GLDRUB."
            )

        if normalized != v:
            log.debug("Normalized ticker %r -> %r", v, normalized)

        return normalized


class EnrichedNewsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    # === Backref на исходную новость ===
    raw_event_id: str = Field(..., description="event_id от RawNewsEvent")
    # Sprint 6: оригинальное время публикации новости из Telegram (UTC ISO).
    # Optional + default None для backward compatibility со старыми events
    # которые в стримах. Predictor использует это поле (если установлено) как
    # reference time для построения features — это нужно для honest historical
    # replay. Если None — fallback на envelope.produced_at (как было раньше).
    tg_published_at: str | None = Field(
        default=None,
        description="UTC ISO 8601 — Telegram server-side time. None = use envelope.produced_at.",
    )

    # === Метаданные LLM ===
    llm_provider: Literal["groq", "ollama"] = Field(..., description="Провайдер LLM")
    llm_model: str = Field(
        ..., description="Точное имя модели, например llama-3.1-8b-instant"
    )
    llm_latency_ms: float = Field(..., ge=0.0, description="Время ответа LLM, мс")
    llm_input_tokens: int = Field(..., ge=0, description="Токенов в промпте")
    llm_output_tokens: int = Field(..., ge=0, description="Токенов в ответе")

    # === Версионирование промпта (NEW v1.1.0) ===
    prompt_version: str = Field(
        ...,
        description="Semver промпта, например '1.0.0'. Для post-mortem и A/B тестов.",
        pattern=r"^\d+\.\d+\.\d+$",
    )

    # === Результаты классификации ===
    is_financial: bool = Field(
        ...,
        description=(
            "True — новость относится к финрынку. "
            "False — погода/спорт/локальные события, tickers=[] легитимен. "
            "True + tickers=[] → ошибка обработки, в DLQ."
        ),
    )
    tickers: list[TickerImpact] = Field(
        default_factory=list, max_length=20, description="Затронутые тикеры (после whitelist валидации)"
    )
    summary: str = Field(
        "", max_length=300, description="Общее резюме новости одной фразой"
    )

    # === Метаданные события (NEW v1.1.0) ===
    expected_timeframe: ExpectedTimeframe = Field(
        ...,
        description=(
            "Ожидаемый горизонт реакции рынка: "
            "instant=0-5m, short=5-30m, medium=30-120m, slow=2h+. "
            "ВНИМАНИЕ: предсказание LLM, требует калибровки на бэктесте."
        ),
    )
    urgency: Urgency = Field(
        "medium",
        description=(
            "Приоритет в очереди Decision Service. "
            "high — обработать вне очереди, low — можно отложить."
        ),
    )
    category: Category = Field(
        "other",
        description="Категория новости. Для аналитики performance по типам.",
    )
    is_actionable: bool = Field(
        False,
        description=(
            "True — новость про конкретное событие/решение (ставка ЦБ, дивиденды, M&A). "
            "False — репортаж, прогноз, мнение, слух, дайджест."
        ),
    )

    # === Сырой ответ LLM (для debug / DLQ analysis) ===
    llm_raw_response: str = Field(
        "",
        max_length=10_000,
        description="Полный ответ LLM до парсинга. Truncate явный, не assertion.",
    )


class EnrichedNewsEvent(MessageEnvelope):
    schema_version: str = SCHEMA_VERSION
    producer: str = "enricher"  # было "analyzer" в 1.0.0
    payload: EnrichedNewsPayload
