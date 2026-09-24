# Sprint 5 / Commit 5.4 — Monitor + Enricher 70b switch / DONE

**Закрыт:** 2026-05-26
**Длительность:** ~1.5ч (vs план 1.5d)
**Цель:** последний из 4 placeholder сервисов + production upgrade Enricher на 70b.

---

## Часть A: Enricher upgrade

### Default model 8b → 70b (Sprint 4.10 winner)

`src/services/enricher/config.py`:
- `groq_model` default: `llama-3.1-8b-instant` → **`llama-3.3-70b-versatile`**
- Добавлено `groq_fallback_model` = `llama-3.1-8b-instant` (для 403 fallback)
- Добавлены `enrichment_cache_key_prefix` (= `enriched:`) и `enrichment_cache_ttl_sec` (= 300)

**Production .env note:** код default = 70b, но `.env` всё ещё содержит `GROQ_MODEL=llama-3.1-8b-instant` от Sprint 3. Pre-flight checklist для 5.5 soak — обновить .env или удалить override.

### 8b fallback на 403 content filter

`src/services/enricher/llm_client.py:_call_with_retry`:
- На `APIStatusError` со `status=403` (content filter) — один retry со `groq_fallback_model`.
- Sprint 4.5 finding: tass/interfax 1.5%% даёт 403 на 8b. Sprint 5.4 reverse case: 70b мог отказать там где 8b пропустит — fallback покрывает оба направления.
- `fallback_consumed: bool` гарантирует ровно один fallback per call (не infinite loop).

### SETEX side effect для Decision cache (план Open Q2)

`src/services/enricher/pipeline.py:_publish_enriched`:
- После `publisher_main.publish(enriched_event)` — `redis.set(f"enriched:{event_id}", JSON, ex=300)`.
- Cache write — best effort: ошибка не блокирует pipeline (log.warning + continue).
- Это поддержка Decision (5.2): `EnrichmentCache.get(enriched_event_id)` → live read без отдельного consumer group.

### TPM/TPD note (не реализовано в 5.4)

План §5.4 упоминал обновление TPM/TPD window под 70b лимиты (TPM 12K vs 8b 6K, TPD 100K vs 8b 500K — 5× строже по дню). **Не реализовано:**
- Текущий `GroqKeyPool` использует **reactive** cooldown на 429 (не proactive TPM/TPD tracking).
- Это работало в Sprint 3 + 4.5/4.6 reenrichment soak'ах — production-validated approach.
- Proactive TPM/TPD window — отдельная engineering задача, backlog Sprint 6.
- Для 5.5 soak — Monitor зафиксирует rate-limit-related counters через heartbeat snapshot.

---

## Часть B: Monitor service

### Service: src/services/monitor/

```
__init__.py
__main__.py        — poll loop, signal handling, Heartbeat publisher
config.py          — MonitorSettings + thresholds
aggregator.py      — XRANGE incremental tail на system:heartbeats
alerts.py          — 3 stateless evaluator (missing_heartbeat / dlq_spike / pnl_kill)
metrics.py        — counters
pipeline.py        — tick() = aggregator + DLQ XLEN + pnl GET + evaluate + dedupe + log
```

### Архитектурные решения

1. **Stateless rules, дедупликация во время loop**
   Каждое из 3 правил — pure function over snapshot. MonitorPipeline дедуплицирует alert'ы по `(rule, context_key)` — повтор не чаще раз в 10 tick'ов (≈5 минут при poll 30s).

2. **HeartbeatAggregator — XRANGE incremental tail**
   Cursor — `_last_id`. На каждом `tick()`: `XRANGE stream (last_id..+` (exclusive). Накапливает per-service `last_seen_utc` + последнюю snapshot dict.
   Bootstrap — first `tick()` от `0-0` подхватывает всю недавнюю историю (Redis Stream maxlen=5000 у нас, ~30 минут heartbeats покрыто).

3. **Three alert rules**
   - **missing_heartbeat**: gap > threshold_sec (default 90s) → warn; gap > 3× threshold → crit
   - **dlq_rate_spike**: XLEN на DLQ stream sampled каждый tick, deque(maxlen=60). Сравнение latest vs sample window_sec назад. delta ≥ threshold → warn
   - **daily_pnl_kill**: |pnl| / initial_equity ≥ kill_pct (default 2%) → crit. Заметит kill triggered в Decision raisin'е (cascaded alert).

4. **Output: log only**
   Sprint 5.4 — `log.warning` / `log.error`. Telegram alerts через Receiver telethon session — Sprint 6 backlog. Monitor сам публикует heartbeat в `system:heartbeats` (self-monitoring).

5. **DLQ streams configurable**
   Default: `news:enriched:dlq` + `ml:predictions:dlq`. Bridge не имеет DLQ (paper-режим). Sprint 6 расширит при добавлении decision DLQ / bridge DLQ.

---

## Тесты (21 new — total 18 monitor + 3 enricher upgrades)

**`tests/services/enricher/test_5_4_upgrades.py`** (3):
- `test_default_groq_model_is_70b` — проверка class default
- `test_enrichment_cache_settings_defaults` — prefix + TTL
- `test_setex_cache_side_effect` — SETEX происходит после publish

**`tests/services/monitor/`**:
- `test_alerts.py` (9):
  - missing_heartbeat: never seen / recent (no alert) / stale (warn) / very stale (crit)
  - dlq_spike: no history / detected / below threshold
  - daily_pnl_kill: no alert / negative trigger / positive trigger
- `test_aggregator.py` (5):
  - empty stream / absorb entry / dedup last_id / update on new entry / skip malformed
- `test_pipeline.py` (4):
  - first tick → alerts на all tracked services missing
  - second tick → suppressed (dedup)
  - daily_pnl seeded → crit alert

**Total Sprint 5.4 tests:** 21 cases. **Полный suite:** 333 passed (312 baseline + 21 new) ✓

---

## Files created/modified

**New:**
```
src/services/monitor/{__init__.py, __main__.py, config.py,
                      aggregator.py, alerts.py, metrics.py, pipeline.py}
tests/services/monitor/{__init__.py, conftest.py,
                        test_alerts.py, test_aggregator.py, test_pipeline.py}
tests/services/enricher/test_5_4_upgrades.py
docs/SPRINT5_COMMIT_5_4_DONE.md
```

**Modified:**
- `src/services/enricher/config.py` — default model 70b + fallback + cache settings
- `src/services/enricher/llm_client.py` — 403 fallback model retry
- `src/services/enricher/pipeline.py` — SETEX side effect

---

## Pre-flight notes для 5.5

1. **Update `.env`**:
   - `GROQ_MODEL=llama-3.3-70b-versatile` (или удалить override чтобы code default)
   - `RISK_PER_TRADE_PCT=0.005` (0.5% paper-analog Phase 2)
2. **Все 6 сервисов готовы** (receiver, enricher, predictor, decision, bridge, monitor).
3. **Data dependencies**:
   - `data/models/predictor/v1/*.joblib` (16 файлов + feature_order.json) — есть
   - `D:\quik_sber\newsbot\prices\*.csv` (19 файлов) — есть
4. **Memurai** должен быть running + AOF on (`config/redis.conf`).
5. **No proactive TPM/TPD** в 70b — Monitor зафиксирует cooldown counter spike, если затронем лимиты.

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| Enricher default → 70b | реализовано | ✓ | ✓ |
| 8b fallback на 403 | реализовано + retry guard | ✓ | ✓ |
| SETEX enriched cache | реализовано + tested | ✓ | ✓ |
| Monitor service 7 модулей | реализовано | ✓ | ✓ |
| missing_heartbeat alert | tested | ✓ | ✓ |
| dlq_rate_spike alert | tested | ✓ | ✓ |
| daily_pnl_kill alert | tested | ✓ | ✓ |
| Alert dedup | tested | ✓ | ✓ |
| Pytest зелёный | 100% | 333/333 | ✓ |
| Proactive TPM/TPD | deferred Sprint 6 | — | ⚠ |

**Sprint 5 / Commit 5.4 — closed ✅**

Готовы к 5.5 (24h end-to-end integration soak).
