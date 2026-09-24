# quik_live/ — QUIK live data exchange

Этот каталог — обменник между QUIK Workstation (DDE/ODBC) и Python-сервисом
`src/services/quik_feed/`. QUIK пишет сюда минутные свечи, Python tail'ит и
публикует в Redis `candles:1m` stream.

Файлы каталога git-ignored (см. `.gitignore`).

## Архитектура

```
QUIK Workstation (брокерский терминал)
     │
     │   Минутные свечи 19 тикеров + 4 cross-asset (BR/USDRUB/MIX/GLDRUB)
     │
     ├──► DDE сервер ──► Excel: D:\quik_sber\newsbot\newsbot3\quik_live\candles.xlsx
     │                              │
     │                              │  (или ODBC → SQLite — альтернатива)
     │                              ▼
     │                       src/services/quik_feed/ (Python poll loop)
     │                              │
     │                              ▼
     │                       Redis stream: candles:1m
     │                              │
     │                              ▼
     │             ┌────────────────┴────────────────┐
     │             ▼                                  ▼
     │      Predictor CandleCache              Bridge CandleCache
     │      (features ATR/RSI/...)             (fill at next-min bar)
     │
     └──► (Sprint 6) trans2quik COM ──► Bridge real trade execution
```

---

## Setup путь A — DDE → Excel (выбранный)

### Шаг 1. Настройка QUIK Workstation

Открой QUIK Workstation. Для каждого из 23 тикеров (19 trade whitelist + 4
cross-asset для cross-asset features):

| Группа | Тикеры |
|---|---|
| **Trade whitelist (12)** | YNDX (= YDEX), GAZP, NG, BR, PLZL, GMKN, TATN, MGNT, VTBR, NVTK, ROSN, LKOH |
| **Phase 2 reference (7)** | SBER, MTSS, Si (= SI), MX (= MIX), CNY, USDRUB, GOLD (= GLDRUB) |
| **Cross-asset (для features уже включены выше)** | BR, USDRUB, MX(MIX), GOLD(GLDRUB) |

Заходим в `Создать окно → Графики → Минутный график → SBER (TQBR)`.
В свойствах графика: **Передавать данные DDE → Excel**.

Альтернативно, можно открыть **«Текущая таблица»** с колонками
`code, datetime, open, high, low, close, volume` и настроить
**DDE → Excel** с автоматическим обновлением.

### Шаг 2. Excel книга

Создай Excel книгу `quik_live\candles.xlsx`.

**Convention (ожидаемая Python-сервисом):**

Один лист `candles` со следующим layout:

| Колонка | Содержимое |
|---|---|
| A | `ticker` (SBER, GAZP, ..., в Phase 2 нотации — Si, MX, GOLD, YNDX) |
| B | `ts` (datetime МСК, "2026-05-29 14:37:00" или Excel datetime cell) |
| C | `open` (float) |
| D | `high` (float) |
| E | `low` (float) |
| F | `close` (float) |
| G | `volume` (float, может быть 0) |

Первая строка — header (`ticker`, `ts`, `open`, `high`, `low`, `close`, `volume`).

Каждый раз когда минута завершилась, новая строка добавляется в конец
(QUIK с помощью DDE link к соответствующей ячейке).

**Расширение через `quik_lua/candle_dump.lua`** (если QUIK DDE напрямую
не делает append):

В QUIK Lua скрипт `Сервисы → Lua скрипты → Загрузить → candle_dump.lua`.
Скрипт каждые 5 секунд читает завершённые минутки из `CreateDataSource(INTERVAL_M1)`
и пишет в конец листа Excel через DDE poke.

### Шаг 3. Auto-save в Excel

В Excel: `Файл → Параметры → Сохранение → Автосохранение каждые 1 мин`.
Это нужно, чтобы Python мог читать файл через `openpyxl` (читает только
сохранённое содержимое).

Альтернативно — VBA макрос с `ThisWorkbook.Save` каждую минуту.

### Шаг 4. Старт quik_feed сервиса

В `.env`:
```
QUIK_FEED_XLSX_PATH=D:\quik_sber\newsbot\newsbot3\quik_live\candles.xlsx
QUIK_FEED_POLL_SEC=5
```

Запуск:
```powershell
.\.venv\Scripts\python.exe -m src.services.quik_feed
```

Должен начать логировать `feed: new bar SBER 14:37 close=295.45` каждые
несколько секунд (при наличии новых строк).

---

## Setup путь B — ODBC (опционально, рекомендация для production)

QUIK поддерживает экспорт таблиц через ODBC. Это **надёжнее** чем DDE Excel:
- Нет file locking
- Atomic SQL transactions
- Полноценные SQL запросы

### Setup:

1. Установи ODBC driver для SQLite (или MS Access / Postgres).
2. В QUIK: `Действия → Вывод через ODBC` для нужной таблицы.
3. Указать DSN с путём к локальной .db файлу `quik_live\candles.db`.
4. Python: использовать `feeder_odbc.py` (TBD Sprint 5.9) вместо
   `feeder_excel.py`.

Текущий MVP — DDE Excel. ODBC backport — backlog.

---

## Troubleshooting

| Симптом | Причина | Лечение |
|---|---|---|
| `openpyxl PermissionError` | Excel удерживает file lock | Включить auto-save 30s интервал, либо CSV альтернативу |
| `feed: bars=0 за 5 минут` (нет новых строк) | DDE не пишет в Excel | Проверить QUIK → таблица передаёт DDE? Окно открыто? |
| Bars приходят, но `ticker` неизвестен в Predictor | Mismatch имён (Si vs SI) | Lua-скрипт нормализует имена через TICKER_PREFIXES — см. `candle_dump.lua` |
| `feed: stale data — ts=14:30` 15 минут назад | QUIK paused / disconnected | Перезапустить QUIK, проверить подключение к брокеру |
| Bridge все сделки идут в `missing_market_data` DLQ | candles stream не доходит | Проверить `redis-cli XLEN candles:1m`, `redis-cli XINFO STREAM candles:1m` |

---

## Ticker name convention

Phase 2 / Predictor / Bridge используют **legacy** имена (которые исторически
были в `prices_*.csv` от newsbot2):

| Legacy (используется в коде) | Canonical (`instruments.py`) | QUIK Code | QUIK Class |
|---|---|---|---|
| Si | SI | SiM6 (или текущий) | SPBFUT |
| MX | MIX | MXM6 | SPBFUT |
| YNDX | YDEX | YDEX | TQBR |
| GOLD | GLDRUB | GOLD-9.26 | SPBFUT |
| остальные 15 | те же | те же | TQBR/SPBFUT |

В Excel/DDE можно использовать **любую** из двух нотаций — `CandleCache.add_bar`
прогоняет через `try_normalize_ticker` (instruments.py registry).
