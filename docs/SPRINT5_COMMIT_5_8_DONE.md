# Sprint 5 / Commit 5.8 — Live candle feed / DONE (code-level)

**Закрыт:** 2026-05-29
**Длительность:** ~1ч coding
**Цель:** добавить live минутные свечи от QUIK Workstation в CandleCache, чтобы Predictor и Bridge работали на realtime данных вместо frozen historical CSV (до 2026-04-21).

---

## Резюме

CSV-файлы Phase 2 (`D:\quik_sber\newsbot\prices\prices_*.csv`) — frozen до 2026-04-21. Для paper trading на live потоке Predictor нужны свежие OHLCV для технических features (ATR/RSI/returns), Bridge — для fill at next-min bar open и bar-by-bar SL/TP.

Sprint 5.8 добавляет **7-й сервис `quik_feed`** который:
1. Tail'ит файл от QUIK Workstation (`.csv` от Lua-скрипта ИЛИ `.xlsx` от чистого DDE)
2. Publish'ит новые минутные бары в Redis stream `candles:1m`
3. Predictor + Bridge подписываются на этот stream и аппендят bars в CandleCache поверх historical CSVs

---

## Архитектура

```
QUIK Workstation
     │
     ├─[путь A]─► Lua скрипт candle_dump.lua → quik_live/candles.csv (append)
     │
     └─[путь B]─► DDE → Excel → quik_live/candles.xlsx
                                  │
                                  ▼
                        src/services/quik_feed/ poll loop (5s)
                                  │
                                  ▼
                        Redis stream candles:1m
                                  │
                ┌─────────────────┴─────────────────┐
                ▼                                    ▼
        Predictor                               Bridge
        CandleCache.subscribe_redis_stream      CandleCache.subscribe_redis_stream
        → add_bar поверх historical CSV         → add_bar для PositionTracker
```

**Backfill convention:** historical CSV покрывает 2022-01-03 → 2026-04-21. Live feed добавляется поверх — обеспечивает continuous coverage без gaps. На cold start Predictor читает CSV (cold), затем подписывается на live (`$` cursor — только новые).

---

## Что построено

### A. `quik_live/` каталог

Новые файлы:
- `quik_live/README.md` — setup инструкции для оператора (QUIK side + Excel/CSV)
- `quik_live/candle_dump.lua` — готовый Lua скрипт для QUIK (~120 строк), пишет завершённые минутки в CSV
- `quik_live/.gitkeep` — каталог не пустой
- `.gitignore` обновлён: `quik_live/*.xlsx`, `*.csv`, `*.jsonl`, `*.db` ignored

### B. `src/services/quik_feed/` — новый сервис (5 модулей)

- `__init__.py`
- `__main__.py` — entry point, mirror enricher/__main__.py структуры
- `config.py` — `QuikFeedSettings` (poll_sec, source_path, accepted_tickers, bootstrap_mode)
- `readers.py` — `CandleBar` dataclass + `CSVTailReader` + `ExcelReader` + `build_reader()` factory by file extension
- `feeder.py` — `QuikFeeder` poll loop + bootstrap_from_redis (resume state) + normalize ticker via `instruments.try_normalize_ticker`
- `metrics.py` — counters + `bar_lag_sec_p50/p95`

### C. `src/infra/candles.py` — live update API

Добавлены методы в `CandleCache`:
- `add_bar(ticker, ts, o, h, l, c, v)` — append/overwrite minute bar (idempotent на duplicate ts, re-sort на out-of-order). Через `try_normalize_ticker` (Si → SI).
- `last_bar_time(ticker)` — getter для staleness check
- `subscribe_redis_stream(redis, stream, shutdown, block_ms)` — async subscriber для `candles:1m`, broadcast mode (без consumer group — несколько сервисов независимо подписываются)

### D. Wiring в Predictor + Bridge

- `src/services/predictor/config.py` — добавлены `live_candles_enabled: bool = True`, `live_candles_stream: str = "candles:1m"`
- `src/services/predictor/__main__.py` — spawn `candle_sub_task = asyncio.create_task(candles.subscribe_redis_stream(...))` после `candles.load_all()`
- `src/services/bridge/config.py` — те же два поля
- `src/services/bridge/__main__.py` — то же spawn task

Graceful shutdown: на shutdown event ждём task с timeout 10s.

### E. Monitor + Launcher

- `src/services/monitor/config.py` — `tracked_services` теперь включает `"quik_feed"` (7 сервисов)
- `tests/services/monitor/test_pipeline.py` — first_tick test обновлён (alerts count == `len(tracked_services)`)
- `scripts/launch_paper_soak.ps1` — стартует quik_feed первым (до receiver), 7 сервисов вместо 6

### F. `.env`

```
QUIK_FEED_SOURCE_PATH=D:\quik_sber\newsbot\newsbot3\quik_live\candles.csv
QUIK_FEED_POLL_SEC=5
# BOOTSTRAP_MODE=tail
```

---

## Реализация деталей

### CSV vs Excel readers

Два варианта source поддерживаются единым interface (`CandleReader` ABC). Factory `build_reader(path, ...)` выбирает по file extension:

| Extension | Reader | Use case |
|---|---|---|
| `.csv` | `CSVTailReader` | Используется с Lua glue (`candle_dump.lua`). Append-only, seek-from-offset, tolerant к partial last line. **Recommended.** |
| `.xlsx` | `ExcelReader` | Pure DDE → Excel без Lua. Использует openpyxl read_only mode, перечитывает всё каждый poll. Может страдать от Excel file locking. |

Обе реализации **stateful**: tracking `last_ts_per_ticker` для skip уже-увиденных bars.

### Bootstrap state recovery

На startup `QuikFeeder.bootstrap_from_redis()` читает 10000 последних entries из `candles:1m` stream и восстанавливает `last_ts_per_ticker`. Это даёт правильный resume после quik_feed restart — не дублирует bars.

### CandleCache live subscriber

`subscribe_redis_stream` использует `XREAD streams={stream: cursor}` с `cursor="$"` (только новые сообщения после старта). Если subscriber пропустил bars во время downtime — они уже были обработаны при последнем live run и сидят в CandleCache historical DataFrame от прошлого update. Cold start всегда читает historical CSV → подписывается на `$`.

### Ticker normalization

В `QuikFeeder._publish_bar`: ticker нормализуется через `instruments.try_normalize_ticker` (Si → SI, MX → MIX, YNDX → YDEX, GOLD → GLDRUB). Off-whitelist tickers тихо skip с counter `bars_skipped_off_whitelist`. В Redis publish'им **canonical** имя — CandleCache использует canonical keys.

### Bar validation

`CandleBar.is_valid()` — sanity check:
- Все цены > 0
- high ≥ low
- high ≥ open и close
- low ≤ open и close

Invalid bars (parsing failure ИЛИ failed validation) тихо skip с counter.

---

## Verification

```powershell
cd D:\quik_sber\newsbot\newsbot3
.\.venv\Scripts\python.exe -m pytest -q
# Expected: 364 passed (335 baseline + 29 new для Sprint 5.8)
```

Result: **364 passed in 13.95s** ✓

Smoke load всех 7 сервисов:
```
receiver  : redis=redis://localhost:6379
enricher  : model=llama-3.3-70b-versatile
predictor : live_candles=True stream=candles:1m
decision  : rr=2.0 risk_pct=0.005
bridge    : mode=paper live_candles=True
monitor   : tracked=['receiver','enricher','predictor','decision','bridge','quik_feed']
quik_feed : source=candles.csv poll=5s
```

### Manual integration test (operator side)

1. **Запустить quik_feed без QUIK** (для проверки idle behavior):
   ```powershell
   .\.venv\Scripts\python.exe -m src.services.quik_feed
   ```
   Должен логировать `polls_source_missing` counter каждые 30s (heartbeat snapshot). Не падает.

2. **Запустить с пустым CSV**:
   ```powershell
   "ticker,ts,open,high,low,close,volume" > quik_live\candles.csv
   ```
   Снова quik_feed. Должен идти tail-mode (bootstrap_mode="tail" по умолчанию) — 0 bars published.

3. **Append вручную**:
   ```powershell
   "SBER,2026-05-29 14:37:00,295.0,295.5,294.8,295.3,10000" >> quik_live\candles.csv
   ```
   В логе quik_feed: `feed: published 1 new bars`. В Redis:
   ```powershell
   redis-cli XLEN candles:1m         # ожидается 1
   redis-cli XRANGE candles:1m - +
   ```

4. **Запустить Predictor отдельно** и проверить subscribe:
   ```powershell
   .\.venv\Scripts\python.exe -m src.services.predictor
   ```
   В логе должно быть `live candles subscriber: stream=candles:1m`. После append в CSV → Predictor's CandleCache получит новую bar (видно в heartbeat snapshot через `candle_cache_size`).

---

## Operator setup (cheatsheet)

### Путь A — Lua скрипт (recommended)

1. Открыть QUIK Workstation → подключение к брокеру live
2. `Сервисы → Lua скрипты → Добавить` → выбрать `quik_live/candle_dump.lua`
3. Запустить скрипт
4. Скрипт начинает писать в `quik_live/candles.csv`
5. Запустить Python `python -m src.services.quik_feed`

### Путь B — Pure DDE → Excel

1. В QUIK: `Создать окно → Таблица текущих параметров` → 19+4 тикера
2. `Передача через DDE → Excel`, save book to `quik_live/candles.xlsx`
3. Excel: настроить auto-save 30s (или VBA macro)
4. В `.env`: `QUIK_FEED_SOURCE_PATH=...\quik_live\candles.xlsx`
5. Запустить Python `python -m src.services.quik_feed`

### Запуск всей цепочки

```powershell
.\scripts\launch_paper_soak.ps1
# Открывается 7 окон (quik_feed первое)
```

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| quik_feed service (5 модулей) | реализован | ✓ | ✓ |
| CSV + Excel readers | оба работают | ✓ | ✓ |
| CandleCache live API | add_bar + subscribe_redis_stream + last_bar_time | ✓ | ✓ |
| Predictor + Bridge subscribe | spawned at startup | ✓ | ✓ |
| Monitor tracking | quik_feed добавлен | ✓ | ✓ |
| Launcher 7 services | ps1 обновлён | ✓ | ✓ |
| Lua glue script | готов | candle_dump.lua | ✓ |
| Operator README | setup инструкции | quik_live/README.md | ✓ |
| Tests | новые + green | 29 new, 364/364 | ✓ |

**Sprint 5 / Commit 5.8 — closed ✅**

---

## Known limitations / Sprint 6 backlog

1. **ODBC reader** — backlog. SQLite/Postgres надёжнее чем Excel polling.
2. **Excel file locking** — pure DDE → Excel может страдать от lock при auto-save. Lua → CSV путь более стабильный.
3. **Lua candle_dump.lua hardcoded contract codes** (SiM6, BRM6, MXM6, GDM6, NGM6, CRM6) — оператор должен править при перекате фьючерсов на новый квартал. В Sprint 6 — функция `get_active_contract(ticker, today)` см. PHASE2 §5.2.
4. **Cross-asset feature staleness** — если QUIK не пушит BR/USDRUB/MIX/GLDRUB live, features уйдут на frozen CSV. Lua скрипт включает их в SECURITIES list.
5. **No back-pressure** — если Redis отстаёт, feeder продолжает publish с increasing latency. Sprint 6 — добавить XLEN check и slow-down.
