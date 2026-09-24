> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 1 — Contracts & Infrastructure (DONE)

**Закрыт:** 7 мая 2026  
**Длительность:** ~6 часов чистой работы  
**Project root:** `D:\quik_sber\newsbot\newsbot3`

---

## Что сделано

### Контрактный слой

5 типов событий + общий envelope, все с pydantic 2 валидацией:

| Контракт | Файл | Назначение |
|----------|------|------------|
| `MessageEnvelope` | `src/contracts/base.py` | Общий envelope: event_id (ULID), schema_version, producer, produced_at, trace |
| `RawNewsEvent` | `src/contracts/raw_news.py` | После Receiver: канал, текст, hash, timestamps |
| `EnrichedNewsEvent` | `src/contracts/enriched_news.py` | После LLM: tickers с direction/confidence/sentiment |
| `MLPredictionEvent` | `src/contracts/ml_prediction.py` | После XGBoost: predicted MFE/MAE long+short по горизонтам |
| `TradeSignalEvent` | `src/contracts/trade_signal.py` | После Decision: EXECUTE/REJECT, entry/SL/TP, qty, R:R |
| `ExecutionResultEvent` | `src/contracts/execution_result.py` | После QUIK: trans_id, filled_price, статус, latency |

**Сквозной event_id chain:** `raw -> enriched.payload.raw_event_id -> prediction.payload.enriched_event_id -> signal.payload.prediction_event_id -> execution.payload.signal_event_id`

### Инфра-компоненты

| Компонент | Файл | Что делает |
|-----------|------|------------|
| `StreamPublisher` | `src/infra/publisher.py` | xadd JSON-payload в стрим с MAXLEN=50_000 + add_trace |
| `StreamConsumer` | `src/infra/consumer.py` | xreadgroup с consumer group, xack только при успехе handler'а |
| `IdempotencyGuard` | `src/infra/idempotency.py` | SET NX EX — атомарная блокировка дубликатов |

### Тесты

- 5 golden JSON sample в `tests/contracts/golden/` (связанная цепочка GAZP)
- `pytest tests/contracts/ -v` -> **6 passed in 0.65s**
- `python scripts/demo_pipeline.py` -> 3 теста зелёные (Publisher/IdempotencyGuard/Consumer.run)

---

## Версии стека

| Компонент | Версия | Примечание |
|-----------|--------|------------|
| Python | 3.14.4 | bleeding edge, всё работает |
| pydantic | 2.13.4 | + pydantic-settings 2.14 |
| redis-py | 5.3.1 | async через `redis.asyncio.Redis` |
| pytest | 9.0.3 | + pytest-asyncio 1.3 |
| python-ulid | 3.1.0 | для event_id |
| structlog | 25.5.0 | для логов (используется частично) |
| Memurai | 8.1.240 (Redis 8.2.5 compat) | Windows-сервис, AUTO_START |

ML-стек установлен заранее (для Sprint 4): pandas 3.0.2, xgboost 3.2.0, pyarrow 24.0.0, numpy 2.4.4, scipy 1.17.1.

Telethon 1.43.2 установлен (для Sprint 2).

---

## Структура проекта

```
newsbot3/
├── conftest.py                       # pytest sys.path setup
├── requirements.txt                  # зависимости (с кракозябрами в комментариях, ОК)
├── .gitignore                        # .venv, .env, *.session, logs, data
├── config/
│   └── redis.conf                    # эталон production-настроек
├── scripts/
│   ├── generate_golden_samples.py    # пересоздаёт golden JSON
│   ├── configure_memurai.py          # применяет CONFIG SET production
│   └── demo_pipeline.py              # end-to-end smoke-test
├── src/
│   ├── contracts/  (6 файлов)        # pydantic-схемы
│   ├── infra/      (3 файла)         # Publisher, Consumer, IdempotencyGuard
│   ├── receiver/   (skeleton)        # Sprint 2
│   ├── analyzer/   (skeleton)        # Sprint 3
│   ├── predictor/  (skeleton)        # Sprint 4
│   ├── decision/   (skeleton)        # Sprint 5
│   ├── bridge/     (skeleton)        # Sprint 6
│   └── monitor/    (skeleton)        # Sprint 7
├── tests/
│   └── contracts/
│       ├── test_schemas.py           # parametrize по 5 golden samples
│       └── golden/                   # 5 *_v1.json
├── logs/                             # пусто (Sprint 7)
└── data/                             # пусто (для SQLite/Parquet архива)
```

---

## Memurai Production Config

Применённые настройки (через `python scripts/configure_memurai.py`):

```
appendonly yes
appendfsync everysec
maxmemory 2gb
maxmemory-policy noeviction
save 300 1 60 10000
```

> ⚠️ **noeviction критичен.** При достижении 2GB лимита producer получит ошибку (мы алертнём), а не молчаливая потеря сообщений.

> ⚠️ **Memurai Developer ограничение:** обязательный рестарт каждые 10 дней. Митигация — еженедельная плановая перезагрузка торгового сервера.

**Расположение бинарника:** `D:\quik_sber\Memurai\` (нестандартный путь — НЕ переименовывать папку).

---

## Быстрая верификация (когда возвращаешься к проекту)

```cmd
cd /d D:\quik_sber\newsbot\newsbot3
.venv\Scripts\activate.bat

REM 1. Memurai жив
sc query Memurai
memurai-cli ping

REM 2. Контракты валидны
pytest tests\contracts\ -v

REM 3. Pipeline работает
python scripts\demo_pipeline.py
```

Все три зелёные = Sprint 1 в порядке, можно работать.

---

## Известные tech debt (поправить когда удобно)

- [ ] `requirements.txt` имеет кракозябры в кириллических комментариях (не ломает работу)
- [ ] Неиспользуемые импорты в `publisher.py`/`consumer.py` (`json`, `time`, `Type`)
- [ ] `test_event_id_propagation` использует переменную `raw` без проверки — слабая логика теста
- [ ] Memurai установлен в `D:\quik_sber\Memurai\`, а не в `C:\Program Files\Memurai\` — не критично, но нестандартно
- [ ] Memurai через portable distro изначально ставили — потом переустановили; на проде сделать чистый MSI install

---

## Sprint 2 — Telegram Receiver (next)

**Цель:** реализовать `src/receiver/main.py`, который:
1. Подключается к Telegram через Telethon на 5 каналов: `@interfaxonline`, `@rian_ru`, `@tass_agency`, `@rbc_news`, `@cbr_official`
2. На каждое новое сообщение создаёт `RawNewsEvent` и публикует в `news:raw` через `StreamPublisher`
3. Дедупликация по SHA-256 от текста через `IdempotencyGuard` (scope=`"news_text"`)
4. Persistent session в `data/sessions/receiver.session`
5. Heartbeat в `system:heartbeats` каждые 30 сек

**Точка контроля:** Sprint 2 закрыт, когда receiver сутки ловит реальные новости и в `news:raw` накопилось ~1000 сообщений с уникальными `text_hash`.
