"""Configuration for the Enricher service (Sprint 3).

The Enricher consumes RawNewsEvent from `news:raw`, calls Groq LLM to
classify the news, builds an EnrichedNewsEvent, and publishes it to
`news:enriched`.

All values can be overridden via environment variables or the .env file
at the project root.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root resolved from this file's location, not cwd.
# <root>/src/services/enricher/config.py — parents[3] climbs out.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]
ENV_FILE: Path = PROJECT_ROOT / ".env"

# Where the prompt template lives.
PROMPTS_DIR: Path = Path(__file__).resolve().parent / "prompts"


class EnricherSettings(BaseSettings):
    """Settings loaded from environment / .env file.

    Required: GROQ_API_KEYS (or single GROQ_API_KEY for backward compat).
    """

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # --- Groq API ---
    # GROQ_API_KEYS — CSV-строка ключей в .env, например:
    #   GROQ_API_KEYS=gsk_xxx,gsk_yyy,gsk_zzz
    # Backward compat: если задан только GROQ_API_KEY — используем его как одиночный.
    #
    # ВНИМАНИЕ: тип str (а не List[str]) намеренно — pydantic-settings v2 пытается
    # JSON-декодировать списковые типы из .env, что ломает простой CSV-формат.
    # Финальный список получаем через resolve_api_keys().
    groq_api_keys: str = Field(
        default="",
        description="CSV-строка ключей из .env: gsk_a,gsk_b,gsk_c",
    )
    groq_api_key: str = Field(
        default="",
        description="Одиночный ключ; используется если groq_api_keys пуст",
    )
    groq_model: str = Field(
        default="llama-3.3-70b-versatile",
        description=(
            "Primary Groq model. Sprint 5.4: переключено с 8b на 70b "
            "(Sprint 4.10 result: 70b +20%% OOS Sharpe vs 8b)."
        ),
    )
    groq_fallback_model: str = Field(
        default="llama-3.1-8b-instant",
        description=(
            "Fallback model на 403 content-filter. Sprint 4.5 finding: "
            "tass/interfax 1.5%% даёт 403 на 8b, 70b пропускает — и наоборот."
        ),
    )
    groq_timeout_sec: float = Field(default=10.0, ge=1.0, le=60.0)
    groq_max_retries: int = Field(default=2, ge=0, le=5)
    groq_temperature: float = Field(default=0.1, ge=0.0, le=1.0)
    groq_max_output_tokens: int = Field(default=400, ge=100, le=2000)

    # --- Sprint 6.2: provider switch (Groq vs DeepInfra) ---
    # Sprint 6.1 finding (docs/SPRINT_6_1_DONE.md): v7 trained on DI 70B
    # distribution → switching prod enricher to DI matches train-serve and
    # gives +8.24 Sharpe swing on identical 1766 VPS events vs Groq.
    enricher_provider: str = Field(
        default="groq",
        description="LLM provider: 'groq' (legacy) or "
                    "'deepinfra' (Sprint 6.2 deployment).",
    )
    deepinfra_api_key: str = Field(
        default="",
        description="DeepInfra API key. Required when enricher_provider=deepinfra.",
    )
    deepinfra_model: str = Field(
        default="meta-llama/Llama-3.3-70B-Instruct",
        description="DI primary model — matches v7 training distribution.",
    )
    deepinfra_fallback_model: str = Field(
        default="meta-llama/Meta-Llama-3.1-8B-Instruct",
        description="DI fallback model on 403 content-filter.",
    )

    # --- Prompt versioning (NEW) ---
    prompt_version: str = Field(
        default="1.0.0",
        description="Semver промпта. Должен совпадать с имени файла в prompts/.",
        pattern=r"^\d+\.\d+\.\d+$",
    )
    prompt_file: Path = Field(
        default=PROMPTS_DIR / "v1_0_0.md",
        description="Путь к Markdown-файлу с промптом. Используется PromptBuilder.",
    )

    # --- Validation toggle (NEW) ---
    validate_whitelist: bool = Field(
        default=True,
        description=(
            "True (production) — тикеры вне whitelist дропаются, warning в лог. "
            "False (debug) — все тикеры пропускаются в pydantic, упадёт на TickerImpact."
        ),
    )

    # --- Redis / Memurai ---
    redis_url: str = Field(default="redis://localhost:6379")
    raw_news_stream: str = Field(default="news:raw")
    enriched_news_stream: str = Field(default="news:enriched")
    enriched_news_dlq_stream: str = Field(
        default="news:enriched:dlq",
        description="DLQ для событий, которые LLM не смог корректно обработать",
    )
    enriched_news_maxlen: int = Field(default=50_000, ge=1_000)
    enriched_news_dlq_maxlen: int = Field(default=10_000, ge=100)
    heartbeat_stream: str = Field(default="system:heartbeats")

    # --- Consumer group (for Redis Streams XREADGROUP) ---
    consumer_group: str = Field(default="enricher")
    consumer_name: str = Field(default="enricher-1", description="Unique per process instance")
    consumer_block_ms: int = Field(default=5000, description="XREADGROUP block timeout")
    consumer_batch_size: int = Field(default=10, ge=1, le=100)

    # --- Sprint 6.2: parallel enrichment workers ---
    # Single DI 70B request takes 5-7s on a healthy day, 20-30s with
    # transient timeouts (observed ~14% timeout rate on our DI quota).
    # Single-thread => effective ~3-15 sec/event => 5h drain on a 2k backlog.
    #
    # Each worker spawns its own StreamConsumer with a UNIQUE consumer_name
    # under the same group, so Redis Streams natively distributes messages
    # round-robin between them. They share one AsyncOpenAI client (its
    # connection pool handles N concurrent requests fine) and one
    # IdempotencyGuard / Publishers / Pipeline / Metrics.
    #
    # Tradeoff: N parallel DI calls => N× chance of hitting DI per-key
    # rate limit if the account tier is tight. Empirically DI Llama-3.3-70B
    # holds 10+ qps fine on a paid key, so concurrency=6 is conservative.
    enricher_concurrency: int = Field(
        default=1, ge=1, le=20,
        description="Number of parallel StreamConsumer workers reading news:raw.",
    )

    # --- Sprint 6.2 ext: PEL reclaim daemon ---
    # Recovers messages stuck in PEL when handlers raise (e.g. DI invalid_json
    # under load). Without this, PEL items rot until next service restart.
    # min_idle_ms MUST exceed longest legitimate handler latency to avoid
    # stealing in-flight work. DI 70B p95 latency ~36s plus internal retries
    # → 120_000ms is the conservative safety margin.
    pel_reclaim_min_idle_ms: int = Field(
        default=120_000, ge=10_000,
        description="Reclaim PEL items idle longer than this (ms).",
    )
    pel_reclaim_poll_interval_sec: int = Field(
        default=30, ge=5, le=600,
        description="Tick cadence (seconds) for the reclaim loop.",
    )
    pel_reclaim_max_deliveries: int = Field(
        default=4, ge=1, le=20,
        description=(
            "Max times a message may be (re)delivered before terminal-DLQ. "
            "Counts the initial delivery + N reclaim attempts. "
            "Default 4 = 1 initial + 3 retry attempts via reclaim."
        ),
    )

    # --- Idempotency ---
    idempotency_scope: str = Field(
        default="news_enriched",
        description="Scope for IdempotencyGuard — prevents re-enriching same raw event_id",
    )
    idempotency_ttl_sec: int = Field(default=86400, description="24h dedup window")

    # --- Heartbeat ---
    heartbeat_interval_sec: int = Field(default=30)

    # --- Producer identity ---
    producer_name: str = Field(
        default="enricher",
        description="Соответствует EnrichedNewsEvent.producer в контракте 1.1.0.",
    )

    # --- SOCKS5 proxy (Sprint 5.10) ---
    # Прокси для исходящих HTTP-запросов в Groq. Когда proxy_enabled=True,
    # каждый AsyncGroq в GroqKeyPool инициализируется с httpx.AsyncClient
    # через socks5-transport. Поля совпадают с ReceiverSettings — pydantic
    # настройки читают тот же .env, но каждый сервис парсит независимо.
    proxy_enabled: bool = Field(
        default=False,
        description="If True, route Groq HTTP calls via PROXY_URL (SOCKS5).",
    )
    proxy_url: str = Field(
        default="",
        description="SOCKS5 URL: socks5://user:pass@host:port. Empty unless proxy_enabled.",
    )

    # --- Decision cache side effect (Sprint 5.4) ---
    enrichment_cache_key_prefix: str = Field(
        default="enriched:",
        description="После publish enriched: SETEX enriched:<event_id> JSON для Decision lookup",
    )
    # Sprint 5.11.2: bumped upper bound 3600 -> 604800 (7 days). The 5-minute
    # default was fine in steady state, but any backlog (predictor restart,
    # network drop, ssh tunnel down) caused Decision to miss the cache for
    # everything older than 5 min -> all backlog signals dropped with
    # enrichment_missing. 7-day cap covers any realistic restart scenario.
    enrichment_cache_ttl_sec: int = Field(default=300, ge=10, le=604800)

    # --- Validation: whitelist of tickers the LLM is allowed to mention ---
    # 19 instruments from Phase 1/2 ALLOWED_TICKERS (ollama_analyzer.py).
    # Tickers outside this list are dropped at validate stage.
    allowed_tickers: List[str] = Field(
        default=[
            # Equities
            "SBER", "GAZP", "LKOH", "YNDX", "ROSN", "GMKN", "NVTK",
            "TATN", "MGNT", "MTSS", "PLZL", "VTBR",
            # Futures / commodities / currencies
            "Si", "MX", "BR", "NG", "GOLD", "CNY", "USDRUB",
        ],
    )

    @field_validator("prompt_file")
    @classmethod
    def _prompt_file_must_exist(cls, v: Path) -> Path:
        """Fail fast если файла промпта нет — лучше упасть на старте, чем в рантайме."""
        if not v.exists():
            raise ValueError(
                f"Prompt file not found: {v}\n"
                f"Expected location: {PROMPTS_DIR}/v<version>.md"
            )
        return v

    def resolve_api_keys(self) -> list[str]:
        """Собрать финальный список ключей: groq_api_keys (CSV) ⊕ legacy groq_api_key.

        Дубликаты удаляются, порядок сохраняется (первый встретился — первый в списке).
        """
        keys: list[str] = []
        seen: set[str] = set()
        # Парсим CSV-строку
        if self.groq_api_keys:
            for k in self.groq_api_keys.split(","):
                k = k.strip()
                if k and k not in seen:
                    keys.append(k)
                    seen.add(k)
        # Добавляем одиночный legacy-ключ, если он не дублирует
        if self.groq_api_key and self.groq_api_key not in seen:
            keys.append(self.groq_api_key)
            seen.add(self.groq_api_key)
        return keys


def load_settings() -> EnricherSettings:
    """Instantiate settings with a friendly error when .env or key is missing."""
    if not ENV_FILE.exists():
        raise FileNotFoundError(
            f"\n\n.env file not found at: {ENV_FILE}\n"
            f"Quick fix from project root:\n"
            f"    copy .env.example .env\n"
        )
    try:
        settings = EnricherSettings()  # type: ignore[call-arg]
    except Exception as e:
        raise

    # Sprint 6.2 — only require Groq keys when provider=groq. DI uses its
    # own DEEPINFRA_API_KEY checked in DeepInfraLLMClient.__init__.
    provider = (settings.enricher_provider or "groq").lower()
    if provider == "groq":
        keys = settings.resolve_api_keys()
        if not keys:
            raise RuntimeError(
                "\n\nNo Groq API keys found in .env\n"
                "Get an API key from https://console.groq.com/keys and add either:\n"
                "    GROQ_API_KEY=gsk_xxx              # single key\n"
                "    GROQ_API_KEYS=gsk_a,gsk_b         # CSV list\n"
            )
    elif provider == "deepinfra":
        if not settings.deepinfra_api_key:
            raise RuntimeError(
                "\n\nENRICHER_PROVIDER=deepinfra but DEEPINFRA_API_KEY not set in .env\n"
            )
    return settings
