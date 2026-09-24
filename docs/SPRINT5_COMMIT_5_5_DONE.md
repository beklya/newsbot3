# Sprint 5 / Commit 5.5 — End-to-end integration / DONE (code-level)

**Закрыт (code-level):** 2026-05-26
**Длительность:** ~0.5ч (vs план 1.5d)
**Цель:** интеграционная проверка цепочки + launcher + pre-flight checklist.

**Note:** реальный 24h soak — операторская работа, не code-task. Этот commit
зафиксирован «code-level done» (все механизмы готовы). Soak run + acceptance
criteria fill — Sprint 6 pre-condition.

---

## Что сделано

### 1. Integration test через все 6 сервисов

`tests/integration/test_full_pipeline.py`:
- **test_full_pipeline_end_to_end**: RawNewsEvent → Enricher (mocked LLM) → news:enriched → Predictor (real Fold 13 models) → ml:predictions → Decision (gates) → trade:signals → Bridge (paper) → trade:executions OPEN + CLOSE
  - Проверяет event_id propagation (raw.event_id == enriched.event_id, fresh ULID на ml:predictions)
  - Проверяет SETEX `enriched:<id>` cache (Decision dependency)
  - Проверяет trace[] propagation через 4 hops (enricher → predictor → decision → bridge)
  - Проверяет OPEN/CLOSE pair с правильным signal_event_id linkage
- **test_off_whitelist_ticker_dropped_early**: SBER (не в 12-ticker whitelist Predictor) → silent skip, 0 predictions/signals/executions

**Результат:** 2/2 passed in 2.59s ✓. Полный suite 335 passed.

### 2. Pre-flight .env update

`.env` обновлён:
- `GROQ_MODEL=llama-3.3-70b-versatile` (Sprint 4.10 winner, было 8b)
- `RISK_PER_TRADE_PCT=0.005` (paper-аналог Phase 2)

Sanity check всех 6 сервисов (load settings):
```
receiver  : redis=redis://localhost:6379
enricher  : model=llama-3.3-70b-versatile cache_prefix=enriched:
predictor : models_dir=v1 whitelist=12
decision  : rr=2.0 risk_pct=0.005
bridge    : mode=paper
monitor   : tracked=['receiver','enricher','predictor','decision','bridge']
```

### 3. Launcher script

`scripts/launch_paper_soak.ps1` — PowerShell launcher для Windows:
- Запускает 6 сервисов в отдельных PowerShell окнах (можно килить независимо)
- Sequenced start с 5-sec задержкой (predictor должен дождаться enricher для XRANGE bootstrap)
- Pre-flight verification: models dir, .env, Memurai

---

## Soak Pre-flight Checklist (operator)

Перед запуском `scripts/launch_paper_soak.ps1`:

1. **Memurai running**:
   ```powershell
   redis-cli ping        # expect PONG
   # OR
   python scripts/redis_inspect.py summary
   ```

2. **Streams должны быть пустые** (или хотя бы accept new messages):
   - `news:raw`, `news:enriched`, `ml:predictions`, `trade:signals`, `trade:executions`
   - Из предыдущей сессии PEL может быть. Опционально clear: `redis-cli FLUSHDB` (потеряет state)

3. **.env проверен**:
   - `GROQ_MODEL=llama-3.3-70b-versatile`
   - `RISK_PER_TRADE_PCT=0.005`
   - `GROQ_API_KEYS=<key>`

4. **Models artifacts**:
   - `data/models/predictor/v1/` содержит 16 .joblib + feature_order.json
   - Если нет — `python scripts/train_predictor_fold13.py`

5. **Candles**:
   - `D:\quik_sber\newsbot\prices\prices_*.csv` × 19 файлов

6. **Console encoding**:
   - `chcp 65001 ; $env:PYTHONIOENCODING="utf-8"` уже в launcher; ручной запуск — повторить

### Run sequence

```powershell
cd D:\quik_sber\newsbot\newsbot3
.\scripts\launch_paper_soak.ps1
```

6 окон откроются. Каждое можно килить независимо (Ctrl+C). Service order:
1. receiver (Telethon connects to Telegram)
2. enricher (consumes news:raw, publishes news:enriched + SETEX cache)
3. predictor (bootstraps news_history XRANGE, then consumes)
4. decision (single consumer ml:predictions, GETs enriched: cache)
5. bridge (PaperExecutor + PositionTracker async tasks)
6. monitor (poll loop)

### EOD acceptance check

```powershell
python scripts\analyze_soak.py --hours 24
python scripts\redis_inspect.py summary
python scripts\redis_inspect.py len trade:executions
python scripts\redis_inspect.py last trade:executions 10
```

Acceptance (per plan §5.5, **integration-level**, не PnL validation):
- 0 PEL stuck сообщений на каждом сервисе
- DLQ rate < 2% совокупно
- ≥5 trade:executions FILLED (нижняя граница для observability)
- 0 false-positive alerts в monitor logs
- Trace[] complete на ≥95% sample 20 random fills
- Latency p50 < 90s, p95 < 180s (news:raw → first execution)

---

## Files created/modified

**New:**
- `tests/integration/{__init__.py, test_full_pipeline.py}`
- `scripts/launch_paper_soak.ps1`
- `docs/SPRINT5_COMMIT_5_5_DONE.md`
- `docs/SPRINT5_DONE.md` (master summary)

**Modified:**
- `.env` — GROQ_MODEL switched to 70b, added RISK_PER_TRADE_PCT=0.005

---

## What's NOT done (deferred to operator/Sprint 6)

1. **Реальный 24h soak run** — операторская работа (требует реального live state)
2. **Acceptance criteria fill** — после soak run
3. **`docs/SPRINT5_DONE.md` final numbers** (DLQ rate, latency p50/p95, fills count) — после soak
4. **Validation milestone** (100+ trades) — Sprint 6 backlog (per план §5.5 — это **integration**, не **validation**)

---

## DoD (code-level)

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| End-to-end integration test | passes through 6 services | 2/2 ✓ | ✓ |
| Pre-flight .env update | GROQ_MODEL=70b, RISK=0.005 | applied | ✓ |
| Launcher script | спавнит 6 сервисов | ps1 готов | ✓ |
| Pre-flight checklist | документирован | в этом DONE | ✓ |
| Smoke-load всех settings | работает на текущем .env | 6/6 ✓ | ✓ |
| Pytest suite | 100% pass | 335/335 | ✓ |
| 24h soak run | hands-off operator work | — | ⚠ deferred |

**Sprint 5 / Commit 5.5 — code-level closed ✅**  (soak run = operator pre-condition for Sprint 6)
