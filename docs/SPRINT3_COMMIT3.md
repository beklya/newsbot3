# Sprint 3 / Commit 3 — DONE

## Что сделано

Pipeline + Heartbeat + Metrics + Entry point для сервиса Enricher.

### Новые файлы

| Файл | Назначение |
|------|-----------|
| `src/services/enricher/metrics.py` | In-memory счётчики + percentiles |
| `src/services/enricher/heartbeat.py` | Периодический heartbeat в `system:heartbeats` (аналог Sprint 2) |
| `src/services/enricher/pipeline.py` | `EnrichmentPipeline` — оркестрация Consumer → LLM → Publisher/DLQ |
| `src/services/enricher/__main__.py` | Entry point + graceful shutdown |
| `tests/services/enricher/test_pipeline.py` | E2E через fakeredis (9 тестов) |
| `tests/services/enricher/test_heartbeat.py` | Heartbeat lifecycle (5 тестов) |
| `tests/services/enricher/test_metrics.py` | Metrics (7 тестов) |

### Обновлённые файлы

| Файл | Что изменилось |
|------|----------------|
| `src/services/enricher/config.py` | + `enriched_news_dlq_stream`, `enriched_news_maxlen`, `enriched_news_dlq_maxlen` |
| `src/services/enricher/llm_client.py` | `EMPTY_FINANCIAL.retryable = False` (по решению owner — в DLQ) |
| `tests/services/enricher/conftest.py` | + fakeredis fixture, raw_event_factory, mock_llm_ok/err |

## Архитектура pipeline

```
news:raw (Receiver Sprint 2)
   │
   ├─→ StreamConsumer (xreadgroup) — group "enricher"
   │
   ├─→ pipeline.process(raw_event):
   │     ├─ IdempotencyGuard.claim(scope="news_enriched", key=event_id)
   │     │     └─ False → skip + ack (counter events_skipped_idem)
   │     │
   │     ├─ GroqLLMClient.enrich(raw_event)
   │     │     ├─ result.ok → publish(news:enriched) + ack
   │     │     │
   │     │     ├─ error.retryable=True → raise EnrichmentRetryable
   │     │     │     └─ Consumer не ack — message в pending для reclaim
   │     │     │
   │     │     └─ error.retryable=False → xadd(news:enriched:dlq) + ack
   │     │
   │     └─ metrics.record_latency / inc(...)
   │
   └─→ HeartbeatPublisher (every 30s) → system:heartbeats
```

## Соглашения

- `event_id` **наследуется** от RawNewsEvent → одно ID на всю цепочку (трассировка через `trace[]` в envelope).
- `trace` от Receiver сохраняется + Publisher добавляет шаг `enricher` через `add_trace()`.
- DLQ-сообщения **не валидируются** против EnrichedNewsPayload — это flat dict с полями для триажа.

## Метрики (snapshot для heartbeat)

```json
{
  "service": "enricher",
  "at": "2026-05-12T21:30:00.000Z",
  "uptime_sec": 1834,
  "events_in": 367,
  "events_out": 350,
  "events_skipped_idem": 4,
  "errors.invalid_json": 3,
  "errors.rate_limit": 14,
  "errors.schema_violation": 1,
  "errors.empty_financial": 0,
  "dlq_total": 4,
  "latency_p50_ms": 850,
  "latency_p95_ms": 2100,
  "latency_samples": 367,
  "llm_input_tokens": 1450000,
  "llm_output_tokens": 73000,
  "pool_n_cooldown": 0
}
```

## Тесты

```
73 passed in ~6 sec
├── test_contract_1_1_0    18 tests  (Sprint 3 Commit 2)
├── test_key_pool           7 tests
├── test_llm_client_parse   9 tests
├── test_metrics            7 tests  ← Commit 3
├── test_pipeline           9 tests  ← Commit 3 (E2E через fakeredis)
├── test_heartbeat          5 tests  ← Commit 3
├── test_prompt             9 tests
└── test_whitelist          7 tests
```

## DLQ формат записи

Хранится в `news:enriched:dlq`, плоский dict в Redis Stream:

| Поле | Описание |
|------|----------|
| `raw_event_id` | event_id исходной RawNewsEvent (cross-ref) |
| `channel` | `@interfaxonline` / `@rian_ru` / ... |
| `text_hash` | SHA-256 от текста для post-mortem дедупликации |
| `tg_published_at` | Когда новость пришла в Telegram |
| `raw_text_preview` | Первые 500 символов текста |
| `error_kind` | `schema_violation` / `empty_financial` / ... |
| `error_message` | Детальное сообщение (до 1000 символов) |
| `llm_raw_response` | Сырой ответ LLM (до 5000 символов) |
| `llm_latency_ms`, `llm_input_tokens`, `llm_output_tokens` | Telemetry |
| `prompt_version` | Версия промпта на момент ошибки |
| `occurred_at` | ISO timestamp ошибки |

## Что осталось до закрытия Sprint 3 — Commit 4 (Soak)

- Запустить Receiver (Sprint 2) + Enricher (Sprint 3) одновременно
- 24h soak — собрать минимум 200 событий, измерить:
  - end-to-end latency p50/p95 (от `tg_published_at` до записи в news:enriched)
  - error rate по kind
  - количество всплесков `pool_n_cooldown > 0`
  - DLQ rate, разобрать примеры
- `scripts/analyze_soak.py` — адаптировать из Sprint 2, добавить чтение `system:heartbeats` + `news:enriched:dlq`
