# Sprint 3 — Known Issues (для Sprint 4 backlog)

Зафиксированы проблемы, обнаруженные в Commit 3 / smoke runs / soak.
Не блокируют завершение Sprint 3, но требуют внимания в Sprint 4.

## ✅ ИСПРАВЛЕНО в Sprint 3

### #1 Pending leak — Consumer не подбирал свой PEL (CLOSED)
- **Симптом:** retryable failures (rate_limit, timeout) → сообщение остаётся в PEL → никогда не переобрабатывается.
- **Корень:** `StreamConsumer` (Sprint 1) читал только `>` (новые), игнорировал свой PEL.
- **Fix:** `StreamConsumer._recover_pel()` на старте делает `XPENDING` + `XCLAIM` для своего consumer_name. Тесты: `test_consumer_pel.py` (4 теста).
- **Закрыто:** Sprint 3 Commit 3.5 (после Commit 3, перед soak).

---

## ⚠️ Открытые — приоритет HIGH

### #2 Periodic XAUTOCLAIM для зависших consumer'ов
- **Сценарий:** Consumer падает между `_read()` и `_handle()` (или после `_handle()` без `xack`). Сообщение в PEL.
- **Текущее поведение:** при перезапуске того же `consumer_name` подберёт. Но **если запустят с новым именем** (или horizontal scale) — PEL застрянет.
- **Решение:** периодический XAUTOCLAIM в фоновой задаче consumer'а: каждые N минут забирать у себя сообщения idle > X.
- **План:** Sprint 4 Commit 1, в `consumer.py`.

### #3 Прочный graceful shutdown с дренажём in-flight
- **Сценарий:** Ctrl+C / SIGTERM приходит во время обработки event'а. Сейчас `shutdown.is_set()` проверяется только между итерациями.
- **Текущее:** in-flight task не дренируется до конца — process кладётся async closer'ом.
- **Риск:** при NSSM stop с коротким timeout — потеря недообработанных сообщений (но они в PEL, поднимутся при restart).
- **План:** Sprint 4 — Pipeline должен иметь явный sigterm-safe wrapper вокруг каждого `pipeline.process()`.

---

## ⚠️ Открытые — приоритет MEDIUM

### #4 TPM выгорание при backlog burst
- **Сценарий:** При накопленной очереди (рестарт после простоя, разгрузка после нерабочего часа) Enricher быстро упирается в TPM. 30-сек паузы.
- **Импакт:** Norm flow OK. Burst разгребается в 2-3x медленнее теоретического минимума.
- **Решения (по убыванию приоритета):**
  1. Throttle requests на pipeline-уровне: `min_interval_ms` между enrich-вызовами.
  2. Сократить промпт v1.0.0 → v1.0.1 на 30-40% (убрать примеры 4-5 из few-shot, оставив 3).
  3. Groq Developer plan (когда выйдут из waitlist).
- **План:** monitoring в soak → решение по результатам.

### #5 EMPTY_FINANCIAL — курсы валют без тикеров
- **Сценарий:** новость `Официальные курсы ЦБ РФ ... 74.62 руб/$1 и 87.88 руб/EUR1` → LLM ставит `is_financial=true, category=currency, tickers=[]` → DLQ.
- **Корень:** EUR не в whitelist; промпт не направляет LLM на USDRUB/CNY для таких новостей.
- **План:**
  1. **Sprint 4 (после soak):** собрать ВСЕ EMPTY_FINANCIAL из DLQ, классифицировать, обновить промпт v1.1.0.
  2. Решить: проставлять USDRUB/CNY для курсовых новостей, или менять `is_financial → false` (это репортаж).

### #6 Sell-the-news не всегда срабатывает (temperature jitter)
- **Сценарий:** на одном и том же тексте «Газпром одобрил дивиденды» LLM то возвращает `direction=short, sentiment=positive` (правильно), то `direction=long, sentiment=positive` (sell-the-news не применён).
- **Корень:** `temperature=0.1` — низкая, но не нулевая. llama-3.1-8b плохо генерализует sell-the-news pattern.
- **План:** Sprint 4:
  - Либо `temperature=0.0` (deterministic) — попробовать.
  - Либо ужесточить промпт: добавить **второй** sell-the-news example.
  - Либо переход на llama-3.3-70b-versatile (но там TPM 12K, и так почти не хватает).

---

## ⚠️ Открытые — приоритет LOW

### #7 `tickers=[]` доминирует на реальном трафике
- **Сценарий:** в smoke и soak большинство новостей дают пустой tickers list (`category=other / geopolitics`).
- **Корень:** реальный новостной поток шумный. LLM по дизайну v1.0.0 промпта не натягивает тикеры на косвенные связи.
- **Решение:** ничего — это design choice. Мониторим в soak% events с tickers >= 1. Если <5% — пересматриваем промпт.

### #8 `rationale` копируется из few-shot
- **Сценарий:** LLM иногда буквально цитирует rationale из примера v1_0_0.md.
- **Импакт:** делает rationale бесполезным как feature для ML Predictor (Sprint 5).
- **Решение:** Sprint 5 — НЕ использовать rationale в ML фичах. Только `direction/confidence/impact_strength`.

### #9 Windows SIGTERM ≠ Linux SIGTERM
- **Сценарий:** NSSM stop отправляет CTRL_BREAK_EVENT. Текущий код ловит SIGINT/SIGTERM через `signal.signal()`.
- **Импакт:** должно работать, не проверено на NSSM ещё.
- **План:** проверить в Sprint 2.5 (NSSM deployment).

### #10 schema_version валидация в EnrichedNewsEvent consumer'ах
- **Сценарий:** если в Sprint 4 Decision Service ожидает schema_version 1.1.0, а в production летит 1.2.0 → `extra='forbid'` упадёт.
- **План:** добавить semver-проверку с предупреждением (не failfail) в Decision Service consumer'е.
