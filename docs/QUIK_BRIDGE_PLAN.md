# План: реальный QUIK-мост исполнения (Sprint 6 backlog #1)

## Контекст и scope

Строим **стратегия-агностичный слой реального исполнения** на MOEX через Сбер/QUIK.
Питается внешними сигналами (любой проект публикует `TradeSignalEvent` в
`trade:signals`), реальные деньги, первый запуск — **1 лот / символически** для
проверки механики. Мост НЕ содержит стратегии — ответственность за качество
сигнала на проекте-источнике (наш news-пайплайн −EV и сюда не подключается).

**Транспорт**: QLUA-скрипт внутри QUIK + `sendTransaction`, file-based обмен —
симметрично существующему [quik_live/candle_dump.lua](../quik_live/candle_dump.lua)
(polling, append в файл, читает Python). Это уже проверенный в среде паттерн.

**Встраивание**: `BridgeSettings.mode` уже есть (`paper|real`). При `real`
[\_\_main\_\_.py:84](../src/services/bridge/__main__.py) создаёт `RealExecutor`
вместо `PaperExecutor` — drop-in: те же контракты, стримы, идемпотентность,
heartbeat, recovery-ключи.

## Архитектура

```
внешний проект → trade:signals (TradeSignalEvent, существующий контракт)
   │
Bridge (mode=real):
   RealExecutor.open_position(signal)
     → QuikOrderClient.send(order)            # пишет orders.jsonl (append)
        → [QUIK] order_bridge.lua: читает строку, sendTransaction()
        → callbacks OnTransReply/OnOrder/OnTrade → пишет status.jsonl
     → reconcile реальный fill (price/qty) ← status.jsonl (poll)
     → publish OPEN ExecutionResultEvent (РЕАЛЬНЫЕ fill, не симуляция)
   RealExecutor ставит SL/TP как СЕРВЕРНЫЕ стоп-заявки QUIK (переживают
     обрыв Python/сети) → на их исполнении publish CLOSE + реальная комиссия
```

## Компоненты (по слоям)

### 1. QUIK Lua — `quik_live/order_bridge.lua` (новый, по образцу candle_dump.lua)
- `OnInit/OnStop/main` lifecycle, poll 1с (быстрее чем свечи — ордера срочные).
- Читает `quik_live/orders.jsonl` (append от Python), обрабатывает новые строки
  по `trans_id` (монотонный, защита от двойного исполнения при рестарте).
- `sendTransaction({ACTION, ACCOUNT, CLASSCODE, SECCODE, OPERATION=B/S,
  TYPE=M/L, QUANTITY, PRICE, TRANS_ID, ...})`. Для SL/TP — стоп-заявки
  (`STOPORDER`) на стороне QUIK.
- Callbacks `OnTransReply` (приём/отказ транзакции), `OnOrder` (статус заявки),
  `OnTrade` (реальная сделка с ценой+комиссией) → append в `status.jsonl`.
- Таблица `(class,code)` тикеров + актуальные фьючерсные коды (взять из
  candle_dump.lua, единый источник через комментарий-ссылку).

### 2. Python транспорт — `src/services/bridge/quik_order_client.py` (новый)
- `send_order(trans_id, ticker, side, qty, type, price)` → append в orders.jsonl.
- `poll_status() -> list[StatusEvent]` — читает новые строки status.jsonl
  (offset-трекинг, как readers.py в quik_feed).
- Маппинг тикер→(class,code) через `instruments.py` + futures-коды.

### 3. `src/services/bridge/real_executor.py` (новый, интерфейс PaperExecutor)
Те же методы, что у PaperExecutor (`open_position`, `build_open_payload`,
`build_close_payload`), но:
- `open_position`: отправляет реальную заявку, ждёт реальный fill (с таймаутом),
  возвращает `OpenPosition` с РЕАЛЬНОЙ ценой входа и комиссией из OnTrade.
- Вместо bar-by-bar симуляции — ставит серверные SL/TP стоп-заявки в QUIK.
- Реальная комиссия из OnTrade → в `ExecutionResultEvent` (не оценка).

### 4. Reconciliation & state — QUIK как source of truth
- На старте: запросить открытые позиции/заявки из QUIK (`getNumberOf`,
  `getItem` через Lua dump в `positions.jsonl`), сверить с
  `bridge:open_positions:*` в Redis. Расхождение → алерт, не торговать вслепую.
- Идемпотентность отправки: `trans_id` детерминирован из `event_id` сигнала —
  повторная доставка сигнала не создаёт второй ордер.

### 5. Safety-слой — `src/services/bridge/risk_guard.py` (новый, критично для денег)
- **Kill-switch**: файл/Redis-флаг `bridge:kill` → мгновенный стоп отправки.
- **1-лот cap** (config `max_qty_per_order`, дефолт 1) — жёсткий потолок объёма.
- **Price collar**: отказ от заявки если текущая цена дивергировала от
  `signal.entry_price` > `max_entry_drift_pct` (поле уже есть в config).
- **Max notional / orders-per-min** — троттлинг.
- **Trading-hours gate** (есть в Decision; продублировать здесь).
- **Daily-loss kill** (есть `risk:daily_pnl`; читать перед отправкой).
- Все срабатывания → REJECT ExecutionResultEvent + heartbeat-метрика + лог.

### 6. Эмпирические косты (связка с задачей #1)
OnTrade несёт реальную комиссию брокера+биржи. RealExecutor логирует её рядом с
notional → накапливаем реальную cost-таблицу, заменяющую хардкод в
`costs_sber.py`. Решает «нет выписки брокера» — соберём косты из живых fills.

### 7. Конфиг — расширить `BridgeSettings`
`quik_orders_path`, `quik_status_path`, `quik_account`, `max_qty_per_order=1`,
`order_fill_timeout_sec`, `kill_switch_key`. `mode="real"` активирует ветку.

## Ключевые проектные решения (рекомендации, можно скорректировать)

1. **Вход**: marketable-limit (лимит по текущей ± малый зазор) вместо чистого
   market — контроль цены входа, защита от проскальзывания на тонком стакане.
2. **SL/TP**: **серверные стоп-заявки QUIK**, не Python-мониторинг — переживают
   обрыв сети/падение Python (критично для реальных денег; orphan-позиция без
   стопа — главный риск).
3. **Транспорт**: file-based jsonl (как candle_dump). Альтернатива — TCP-сокет в
   Lua, но файл проще, надёжнее, уже работает в этой среде.

## Лестница верификации (от нулевого риска к деньгам)

1. **Loopback без QUIK**: pytest — QuikOrderClient пишет orders.jsonl, мок-Lua
   (Python-скрипт) отвечает в status.jsonl, RealExecutor reconcile'ит. Полный
   цикл OPEN→CLOSE на фейковых fill. Реальных денег ноль.
2. **QUIK DEMO-счёт** (если у Сбера есть демо/тренировочный режим): реальный
   QUIK, ненастоящие деньги — проверить sendTransaction, callbacks, стоп-заявки.
3. **1 лот реально, 1 тикер (SBER)**: ручной разовый сигнал → проверить:
   заявка дошла, fill reconcile'ится, ExecutionResultEvent корректен, SL/TP
   стоит на сервере, **реальная комиссия захвачена и сверена с оценкой 0.06%**.
4. **Gate на масштаб**: только после успеха п.3 — снять 1-лот cap (отдельное
   твоё решение, не автоматически).

## Файлы

| Файл | Статус | Назначение |
|---|---|---|
| `quik_live/order_bridge.lua` | новый | QLUA: orders→sendTransaction→status |
| `src/services/bridge/quik_order_client.py` | новый | file-транспорт Python↔Lua |
| `src/services/bridge/real_executor.py` | новый | реальное исполнение (drop-in) |
| `src/services/bridge/risk_guard.py` | новый | safety-слой |
| `src/services/bridge/config.py` | правка | quik_* поля, max_qty_per_order |
| `src/services/bridge/__main__.py` | правка | ветка mode=real → RealExecutor |
| `tests/services/bridge/test_real_executor.py` | новый | loopback-цикл, safety |

## Границы (важно для реальных денег)

- **Я пишу софт; реальные заявки выставляешь ТЫ**, запуская сервис на своей
  машине с QUIK. Я не отправляю сделки и не подключаюсь к твоему счёту.
- Загрузка `order_bridge.lua` в QUIK, настройка счёта/класса, проверка прав на
  торговлю — на твоей стороне (как с candle_dump.lua).
- Каждый переход по лестнице верификации (особенно п.3, первые реальные деньги) —
  только после твоего явного подтверждения.

## Порядок реализации

Слой 2+3 (транспорт+executor) + loopback-тест (п.1) → safety (5) → Lua-скрипт (1)
→ конфиг/wiring (7,4) → reconciliation (4) → твоя загрузка в QUIK → п.2/п.3.
Первый коммит — чистый offline-loopback, нулевой риск.
