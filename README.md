# newsbot3 — новостной бот для MOEX (замороженный исследовательский проект)

**English summary.** A Russian-language pipeline that reads MOEX-relevant Telegram news
channels, classifies each message with a hosted LLM (Llama 3.x via Groq / DeepInfra),
predicts short-term price moves with XGBoost and emits *paper* trade signals, all wired
through Redis Streams. The project is **frozen**: after honest validation we found **no
directional edge** in news for MOEX (LLM direction ≈ coin flip, 0 of 440 configurations
profitable after real broker costs). It is published as working infrastructure plus an
honest record of a negative result. Not investment advice.

---

## Статус

- Проект заморожен 15.06.2026. Код рабочий (655 тестов), но **стратегии прибыли не дают** —
  поэтому бот не торгует.
- Выложен как (1) готовый каркас потокового конвейера «новость → LLM → модель → сигнал»
  и (2) честный отчёт о том, почему это не сработало. Отрицательный результат тоже результат.

## Результаты (коротко и честно)

| Что проверяли | Результат |
|---|---|
| Направление от LLM (70B, 51 282 пары событие × тикер, ходы ≥1%) | совпадение со знаком хода **49.6–50.7%** — монетка |
| XGBoost на честных LLM-признаках, walk-forward с реальными костами брокера | **0 из 38** фолдов и **0 из 440** конфигураций в плюсе; валовая прибыль ≈ +57 ₽ против костов ≈ 1 119 ₽ на сделку |
| «Sell the news», жёсткие события + OCO | в выборке 45.3%, вне выборки 50.6%; OCO −0.03% net |
| Досье компаний / база знаний (с shuffle-контролем) | 42.5% и 44.9% против 50.6% и 51.2% у контроля — хуже случайного |
| Исходный бэктест Phase 2 (Sharpe 4.87) | **артефакт утечки будущего** — см. ниже |

**Главная ловушка, которую стоит знать всем, кто размечает новости LLM-ом.** Исторический
корпус Phase 2 был размечен промптом [`docs/legacy_prompt/ollama_analyzer.py`](docs/legacy_prompt/ollama_analyzer.py),
который передавал в LLM движения цены *после* новости. В результате sentiment совпадал со
знаком будущего хода в 57.4% / 55.1% / 53.1% случаев (15m / 60m / 1d) против ~50% у честной
разметки, а модель «видела будущее». Стандартная проверка `corr(confidence, |move|) ≈ 0.07`
такую утечку **не ловит**: она сидит в знаке sentiment и в выборе тикера, а не в величине
уверенности. Документы, чьи числа опирались на эту разметку, помечены баннером «Опровергнуто».

## Что внутри

```
src/contracts/     pydantic-контракты событий (MessageEnvelope с ULID и trace[]), реестр 19 инструментов MOEX
src/infra/         Redis Streams: publisher, consumer (PEL-recovery), идемпотентность, heartbeat, retry
src/services/
  receiver/        Telethon: 4 Telegram-канала → news:raw (дедуп по SHA-256 текста)
  enricher/        LLM-разметка (Groq/DeepInfra, failover на 429, фолбэк модели) → news:enriched, DLQ
  predictor/       67 признаков + 16 моделей XGBoost (MFE/MAE × long/short × 30m/60m) → ml:predictions
  decision/        R:R-логика + LLM direction filter + риск-гейты → trade:signals (EXECUTE/REJECT)
  bridge/          paper-исполнение: вход по следующему бару, SL/TP/time-stop побарово → trade:executions
                   (+ незавершённый real-режим для QUIK, см. «Дисклеймер»)
  monitor/         heartbeats всех сервисов, алерты
  quik_feed/       минутные свечи из QUIK (quik_live/candle_dump.lua) → candles:1m
sprint4/           оффлайн-калибровка: выборки, факториальный анализ, гибридные бэктесты выходов
scripts/           обучение, реэнричмент, walk-forward, replay, аналитика спринтов 4–9, утилиты Redis
docs/              отчёты по каждому спринту (SPRINT*_DONE.md) — полная история, включая провалы
data/models/       обученные модели XGBoost (8 версий) — только для воспроизводимости
```

Поток событий:

```
Telegram → receiver → news:raw → enricher → news:enriched → predictor → ml:predictions
         → decision → trade:signals → bridge (paper) → trade:executions
QUIK → quik_feed → candles:1m (broadcast для predictor и bridge)
все сервисы → system:heartbeats → monitor
```

`event_id` (ULID) наследуется по всей цепочке, каждый сервис дописывает шаг в `trace[]` —
удобно для сквозной отладки. Ретраи делает сам стрим: исключение в обработчике = нет ACK =
сообщение остаётся в PEL и переотдаётся.

## Модели в `data/models/predictor/`

Все версии обучены на признаках, где LLM-часть либо содержит утечку (v1 — Phase 2 legacy),
либо честная, но без предсказательной силы (v2+ на разметке 70B). Для торговли они **не
годятся**; включены, чтобы можно было воспроизвести отчёты. У `v7*` дополнительно был баг
слияния обучающей выборки (строки размножены ×2.8) — см. `docs/SPRINT_6_3_DONE.md`.
По умолчанию predictor грузит `v7_70b_v2`; другую версию задаёт `MODELS_DIR`.

Файлы `.joblib` — это pickle: загружай их только из источника, которому доверяешь.

## Быстрый старт

Требования: Python 3.14 (на нём всё тестировалось; 3.12+ скорее всего подойдёт), Redis 7+ или Memurai на Windows.

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt     # Linux/macOS: .venv/bin/pip
cp .env.example .env                              # заполни ключи
pytest                                            # 655 тестов, Redis не нужен (fakeredis)
```

Запуск сервисов (каждый в своём терминале, из корня проекта):

```bash
python -m src.services.receiver
python -m src.services.enricher
python -m src.services.predictor
python -m src.services.decision
python -m src.services.bridge
python -m src.services.monitor
python -m src.services.quik_feed       # нужен QUIK с quik_live/candle_dump.lua
```

## Данные — не включены

- Архив Telegram-новостей: тексты принадлежат СМИ (Интерфакс, РИА, ТАСС, РБК) —
  распространять их нельзя. Собрать свой архив можно через receiver (`--backfill-hours`).
- Минутные цены: у поставщиков свои лицензии. Predictor и bridge ожидают CSV
  `prices_<TICKER>.csv` в `data/prices/` (или в каталоге из `PRICES_DIR`) и без него не стартуют.
- Сервисы используют пути от корня проекта. А вот многие исследовательские скрипты,
  `quik_live/candle_dump.lua` (`CSV_PATH`) и часть документации содержат абсолютные пути вида
  `D:\quik_sber\newsbot\...` из исходной среды — их нужно заменить на свои.

## Чему научил проект

1. Считай реальные издержки брокера **до** всего остального — именно они убили большинство идей.
2. Проверяй утечку будущего не только по величине уверенности, но и по знаку и по выбору объекта.
3. Сравнивай одну и ту же конфигурацию одной и той же метрикой (Sharpe по сделкам ≠ по дням).
4. Тестируй типы на границе «исследовательский харнесс ↔ прод» — один int вместо str
   перевернул вывод целого спринта.
5. Смотри концентрацию прибыли: если 5 сделок дают 97% результата — это не стратегия.
6. Требуй честный out-of-sample и shuffle-контроль до того, как радоваться.

## Дисклеймер

Код предоставляется «как есть», без каких-либо гарантий. Это не инвестиционная рекомендация.
По умолчанию bridge работает в paper-режиме. В коде есть и **незавершённый** real-режим
(переменная `MODE=real` в настройках bridge: `real_executor.py`, `quik_order_client.py`, `risk_guard.py`). Он передаёт
заявки через файлы в `quik_live/`, но QUIK-скрипт, который их исполняет, в репозиторий
**не входит**, и на реальных деньгах этот режим не проверялся. Стратегия edge не имеет (см. выше),
поэтому любое использование с реальными деньгами — на твой собственный риск.

## Лицензия

[GPL-3.0](LICENSE). Производные работы, которые распространяются, должны распространяться
под той же лицензией с открытым кодом.
