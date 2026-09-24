> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 3 — Soak Plan

## Цель

Проверить устойчивость Enricher pipeline на реальном трафике в течение 24 часов
до публикации Sprint 3 как DONE.

## Setup

| Компонент | Версия / конфиг |
|-----------|-----------------|
| OS | Windows |
| Python | 3.14.4 venv |
| Redis | Memurai 8.1 на localhost:6379 |
| Receiver | Sprint 2 DONE — `python -m src.services.receiver` |
| Enricher | Sprint 3 Commit 3 — `python -m src.services.enricher` |
| LLM | Groq llama-3.1-8b-instant |
| Prompt | v1.0.0 |
| Channels | interfax, rian, rbc, tass |

## Pre-flight checklist

- [ ] Все 77 тестов проходят: `pytest tests/services/enricher/ -v`
- [ ] Memurai запущен (`sc query Memurai` → RUNNING)
- [ ] `.env` содержит Groq API key
- [ ] PEL очищен (no pending): `python scripts/redis_inspect.py pending news:raw enricher`
- [ ] news:enriched dlq свежий или принят к сведению
- [ ] Receiver запущен и подключился к Telegram

## Запуск

В разных терминалах:

```powershell
# Терминал 1 — Receiver (если ещё не запущен от Sprint 2)
python -m src.services.receiver

# Терминал 2 — Enricher
python -m src.services.enricher
```

Оба пишут логи в stdout. NSSM в Sprint 2.5 — потом.

## Метрики и снимки

Запускать каждые 4-6 часов:

```powershell
# Что сейчас в системе
python scripts/redis_inspect.py summary

# Анализ за период
python scripts/analyze_soak.py --hours 6
```

## Критерии успеха (24h soak)

| Метрика | Цель | Stretch | Файл с подтверждением |
|---------|------|---------|------------------------|
| **events_in** | ≥200 | ≥500 | analyze_soak.throughput |
| **events_out / events_in** | ≥90% | ≥95% | analyze_soak.throughput |
| **error_rate** | <8% | <5% | analyze_soak.error_rates |
| **dlq_rate** | <5% | <3% | analyze_soak.error_rates.dlq_delta |
| **latency p50** | <3000 ms | <2000 ms | analyze_soak.latency_trend |
| **latency p95** | <15000 ms | <8000 ms | analyze_soak.latency_trend |
| **pool full cooldown** | <5% heartbeats | 0 | analyze_soak.pool_cooldowns |
| **pending at end** | ≤10 | 0 | analyze_soak.pending |
| **Receiver heartbeats gaps >60s** | 0 | 0 | вручную (Sprint 2 metric) |
| **Sell-the-news правильно** | ≥1 раз | — | DLQ + sample of enriched |

## Что НЕ блокирует пометить DONE

Эти проблемы фиксируем как issues для Sprint 4, не блокируют:

- `category=other` доминирует (>70% событий) — это **признак шумного трафика**, не пайплайна
- `tickers=[]` в большинстве событий — это **дизайнерское решение** v1.0.0 промпта, итерируем потом
- Sell-the-news Газпром нашёлся не сразу — это **temperature jitter**, не баг

## Что блокирует

- Pipeline падает (uncaught exception в логах)
- Receiver или Enricher теряют связь с Redis на >30 секунд
- DLQ rate >5% — означает, что LLM возвращает мусор больше чем в 1/20 случаев
- pool full cooldown держится >15% heartbeats — лимиты Groq не хватают, нужен Dev Tier

## Что записываем при закрытии

`docs/SPRINT3_DONE.md` должен содержать:

1. **Шапка**: dates, hours, total events_in/out
2. **Performance summary**: throughput / latency / error rate
3. **DLQ analysis**: что попало и почему, есть ли паттерны
4. **Pool behavior**: cooldown episodes, корреляция с burst'ами
5. **Известные issues для Sprint 4**:
   - XAUTOCLAIM расписание (если pending растёт)
   - Промпт v1.1.0 (что докрутить по DLQ-выборке)
   - Throttling enrich (если burst'ы регулярные)
6. **Готовность к Sprint 4 (Decision Service)**: пайплайн стабилен → можно строить дальше

## После soak

Перед закрытием Sprint 3:

```powershell
# Финальный JSON-отчёт сохраняется в файл
python scripts/analyze_soak.py --hours 24 --json > docs/sprint3_soak_report.json

# Проверка accumulated pending
python scripts/redis_inspect.py pending news:raw enricher
```

Если pending > 10 — сделать XAUTOCLAIM и второй pass:

```powershell
python scripts/redis_inspect.py claim news:raw enricher 300000
# рестарт Enricher на 5 минут, чтобы он подобрал claimed
```
