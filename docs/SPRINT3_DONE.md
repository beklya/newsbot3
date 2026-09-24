# Sprint 3 — DONE

## Резюме

Phase 3 / Sprint 3 закрыт. Pipeline `news:raw → Groq LLM → news:enriched`
прошёл 19-часовой soak на реальном трафике с **95% success rate**, **4.9% error rate**,
**latency p95 ~1 сек после прогрева**.

## Soak-метрики

| Метрика | Цель Plan | Stretch | Результат | Статус |
|---------|-----------|---------|-----------|--------|
| events_in | ≥200 | ≥500 | 102 за 19h | ⚠️ Период короче плановых 24h |
| events_out / events_in | ≥90% | ≥95% | 95.1% (97/102) | ✅ |
| error_rate | <8% | <5% | 4.9% (5/102) | ✅ |
| dlq_rate | <5% | <3% | 3.9% (4/102) | ✅ |
| latency p50 | <3000 ms | <2000 ms | 798 ms (после прогрева) | ✅ stretch |
| latency p95 | <15000 ms | <8000 ms | 1156 ms (после прогрева) | ✅ stretch |
| pool full cooldown | <5% hb | 0 | 8.2% (63/766) | ⚠️ Чуть выше плана |
| pending at end | ≤10 | 0 | 1 | ✅ |

**Note**: low throughput (5.3 events/hour vs ожидавшиеся 15) объясняется
тихим периодом в новостях + отсутствием CBR channel (удалён в Sprint 2.5 backlog).

## Что построено в Sprint 3

### Архитектура

```
news:raw (Receiver Sprint 2)
   │
   ├─→ StreamConsumer
   │     ├─ Phase 1: PEL recovery через XPENDING+XCLAIM
   │     └─ Phase 2: чтение новых через xreadgroup(>)
   │
   ├─→ EnrichmentPipeline.process(raw_event):
   │     ├─ IdempotencyGuard (Redis SET NX EX, scope=news_enriched)
   │     ├─ GroqLLMClient.enrich() (через KeyPool)
   │     │     ├─ result.ok → publish news:enriched + ack
   │     │     ├─ error.retryable → raise → no ack → PEL
   │     │     └─ error.non-retryable → DLQ news:enriched:dlq + ack
   │     └─ metrics.record(latency, tokens, kind)
   │
   └─→ HeartbeatPublisher (every 30s) → system:heartbeats
```

### Контракт EnrichedNewsEvent v1.1.0

- `prompt_version` (required, semver)
- `is_financial`, `is_actionable` (флаги разной семантики)
- `expected_timeframe` (instant/short/medium/slow)
- `urgency`, `category`
- `tickers[]` с раздельными `direction` / `sentiment` / `confidence` / `impact_strength` / `rationale`
- Whitelist 19 тикеров (MOEX equities + futures + currencies)

### Промпт v1.0.0

5 few-shot примеров (instant CBR, sell-the-news Газпром, forecast, noise, multi-ticker).
3 строгих правила:
1. JSON-only ответ через `response_format={"type": "json_object"}`
2. Whitelist валидация: тикеры вне списка отбрасываются с warning
3. Все ratelimits headers возвращаются через `with_raw_response.create()`

### KeyPool

- Пул LLM-клиентов с per-client cooldown
- Failover на 429 (Groq SDK `max_retries=0`, ловим RateLimitError сами)
- Respect `Retry-After` header
- `pool.stats()` для heartbeat

### Тесты

**77 passed** in 8.4s:
- 18 контракт 1.1.0
- 8 key_pool (включая cooldown/round-robin)
- 9 llm_client parsing (markdown wrappers, truncation)
- 7 whitelist
- 7 metrics (counter + percentiles)
- 5 heartbeat (lifecycle + error resilience)
- 9 pipeline E2E (через fakeredis: idempotency, DLQ, retryable)
- 4 consumer PEL recovery ⭐ (новые для Commit 3.5)
- 9 prompt (loading, rendering, edge cases)

### Документация

- `SPRINT3_COMMIT3.md` — снапшот при закрытии Commit 3
- `SPRINT3_SOAK_PLAN.md` — план soak с критериями
- `SPRINT3_KNOWN_ISSUES.md` — backlog для Sprint 4
- `SPRINT3_DONE.md` — этот файл

## Качество LLM-классификации — наблюдения

### ✅ Что работает

- **Корректное разделение financial vs noise**: 80%+ новостей — это шум (война, ДТП, общая повестка). Промпт правильно ставит им `tickers=[]`, не натягивает.
- **Sell-the-news Газпром распознан**: `direction=short, sentiment=positive` при дивидендах.
- **Multi-ticker для системных новостей**:
  - QR-оплата за границей → SBER, MTSS, VTBR long
  - Банки в "белые списки" → SBER, VTBR long
- **Currency моды**: «рубль ослабевает к юаню» → CNY short + USDRUB short.

### ⚠️ Что требует доработки v1.0.1+ (Sprint 4)

- Курсы ЦБ (USDRUB/EUR) → DLQ как `empty_financial`. Нужно явное правило про курсовые объявления.
- Длинные текcты с эмодзи `🗣 ▪️` → Groq 400 errors. Нужна санитизация перед отправкой.
- Прогнозы аналитиков (БКС таргеты) иногда классифицируются как `is_actionable=true` — нужно усилить пример #3 в промпте.

## Известные issues для Sprint 4

См. `docs/SPRINT3_KNOWN_ISSUES.md`:

| # | Issue | Severity |
|---|-------|----------|
| 1 | Pending leak (ИСПРАВЛЕНО в C3.5) | ✅ Closed |
| 2 | Periodic XAUTOCLAIM (если несколько consumer'ов) | High |
| 3 | Graceful shutdown с дренажом in-flight | High |
| 4 | TPM выгорание на burst (8% full cooldown в soak) | Medium |
| 5 | EMPTY_FINANCIAL — курсы ЦБ нужно понимать | Medium |
| 6 | Sell-the-news temperature jitter (хочется temperature=0.0) | Medium |
| 7 | `tickers=[]` доминирует (80%+) — design choice | Low (мониторим) |
| 8 | `rationale` копируется из few-shot — нельзя как ML feature | Low |
| 9 | Windows SIGTERM via NSSM — проверить в Sprint 2.5 | Low |
| 10 | schema_version semver-проверка в Decision Service | Low |

## Готовность к Sprint 4 (Decision Service)

✅ **Pipeline stable** — 19 часов без падений.
✅ **Контракт устойчив** — никаких schema_violation за 100 событий
   (1 случай — старый тест с неполным prompt).
✅ **Observability налажена** — heartbeats / analyze_soak / redis_inspect / watch_enriched.
✅ **PEL recovery работает** — критическая защита от потери retryable.

**Можно строить Decision Service** который читает `news:enriched` и решает,
открывать ли позиции в QUIK.

## Файлы Sprint 3

### Source
- src/contracts/enriched_news.py (schema 1.1.0)
- src/services/enricher/ — config, prompt, llm_client, key_pool, metrics, heartbeat, pipeline, __main__
- src/services/enricher/prompts/v1_0_0.md
- src/infra/consumer.py (с PEL recovery)

### Scripts
- scripts/test_groq_prompt.py — manual prompt testing
- scripts/redis_inspect.py — inspect Redis state (groups/pending/last/enriched/dlq/summary/claim)
- scripts/watch_enriched.py — live tail enriched events with colors
- scripts/analyze_soak.py — soak metrics aggregator
- scripts/enable_aof.py — Memurai persistence configuration

### Tests — 77/77 passed
- tests/services/enricher/ — 9 файлов, см. выше

### Docs
- docs/SPRINT3_COMMIT3.md / SPRINT3_SOAK_PLAN.md / SPRINT3_KNOWN_ISSUES.md / SPRINT3_DONE.md
