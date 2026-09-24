> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 4 / Commit 4.0 — exits_comparison / DONE

**Закрыт:** май 2026
**Длительность:** ~6 часов чистой работы (включая 4 итерации sanity fix)
**Проект:** D:\quik_sber\newsbot\newsbot3\sprint4\exits\

---

## Цель

Retrospective сравнение 5 exit-стратегий на 3,300 сделках Phase 2 best combo
(horizon=60, rr=2.0, mx_specific) — БЕЗ переобучения моделей.

Найти best exit-схему для:
- Sprint 4 hybrid backtest (коммит 4.9)
- Phase 3 paper trading

---

## Финальный результат — ranking 5 стратегий

| # | Strategy | Sharpe | Total PnL | MaxDD | Win % | Profit Factor |
|---|----------|--------|-----------|-------|-------|---------------|
| 🥇 | **baseline_fixed_tp_sl** | **4.98** | **+2,069,907** | -59,304 | 59.5% | 1.449 |
| 🥈 | breakeven_after_1r | 4.14 | +1,674,134 | -99,125 | 55.2% | 1.366 |
| 🥉 | trailing_after_1r | 1.71 | +570,257 | -199,259 | 62.9% | 1.138 |
| ❌ | partial_50_50_at_levels | -1.78 | -559,607 | -667,663 | 57.0% | 0.877 |
| ❌ | time_based_partial | -1.66 | -559,915 | -670,866 | 52.3% | 0.881 |

**WINNER:** baseline (Phase 2 fixed TP/SL + time-stop)

---

## Per-year breakdown (baseline) — никакой деградации Sharpe

| Year | Sharpe | PnL | n_trades |
|------|--------|-----|----------|
| 2023 | 4.90 | +697k | ~1000 |
| 2024 | 4.94 | +666k | ~1000 |
| 2025 | **5.66** | +678k | ~1000 |
| 2026 | 2.03 | +29k | ~200 (3 мес, малая выборка) |

**Ключевой вывод:** edge Phase 2 **не деградировал** — 2025 даже сильнее 2023-2024.
По 2026 пока недостаточно данных (Sharpe 2.03 на ~200 сделках за 3 месяца).

---

## Гипотезы и их статус

| Гипотеза (из exits_analysis_phase2.txt) | Статус | Что показала практика |
|------------------------------------------|--------|----------------------|
| Trailing на новостных импульсах **вредит** | ✅ Подтверждена | Sharpe 4.98 → 1.71 (-66%) |
| Breakeven защищает прибыль, но **срезает на ретесте** | ✅ Подтверждена | Sharpe 4.14, PnL -19%, но MaxDD +67% |
| Partial 50/50 на 1R даёт **выше Sharpe** | ❌ Опровергнута | Cost удваивается → -127% PnL |
| Time-based partial (50% @ 10m) — лучшая гипотеза | ❌ Опровергнута | Та же проблема с cost |

### Почему partial-стратегии провалились

**Не баг кода** (проверено вручную: manual = code, погрешность 0).
**Это методологическая особенность Phase 2:**

1. **Узкое распределение TP/SL.** TP_FRACTION=0.7 → tp_dist/sl_dist ≈ 1.17.
   Полная TP-сделка даёт +1.17R, полная SL даёт -1R. Partial 50/50 на 1R
   фиксирует половину TP-сделок ниже их полного потенциала.

2. **Cost удваивается.** Каждый partial exit добавляет 0.5× cost_rub.
   На 3300 сделок × ~1000-2000₽ средний cost = +1.5-3M издержек.
   Это **полностью съедает** edge.

3. **Узкая разница «дошёл до 1R → откатился в SL»**. Из 1681 SL-сделок baseline
   только ~456 (27%) активировали partial по 1R уровню — остальные не дошли
   даже до 1R. На этих 456 partial спасает (+1R vs -1R), но это не покрывает
   потерь от срезания TP.

**Вывод:** partial-стратегии **не окупаются** при текущей структуре спреда/комиссии
и узких TP уровнях. Они **могут** работать на сигналах с TP ≥ 2-3R и низким cost.

### Почему trailing провалился

**Это новостной импульс**, а не trend continuation. Трейлинг подтягивает SL
**близко к цене**, и **нормальный pullback внутри импульса** срабатывает trailing.

Из exit distribution: **42.3% сделок** закрылись на `trailing` reason. Из них
средний exit, видимо, ~+0.5-0.7R вместо +1.17R на полном TP.

**Решение для Phase 3:** trailing использовать нельзя.
**Возможное исключение:** если в Sprint 5 найдём LLM-категорию с **trend
continuation** характером (e.g. дивидендные новости с медленным эффектом) —
trailing может быть полезен только на ней.

---

## Что сравнивалось — структура коммита

### Files:
- `instruments.py` — реестр 19 инструментов (canonical/legacy mapping + lot_size)
- `prices_cache.py` — CSV → parquet кэш, lookup для bar-by-bar симулятора
- `trades_loader.py` — загрузчик phase2_mfe_trades.parquet → list[Trade]
- `base.py` — interface ExitStrategy, dataclass Trade/ExitResult, utilities
- `baseline.py` — Strategy #1 (Phase 2 fixed TP/SL + time-stop)
- `breakeven.py` — Strategy #2 (SL → entry после 1R)
- `trailing.py` — Strategy #3 (SL подтягивается по 0.5×ATR после 1R)
- `partial_at_levels.py` — Strategy #4 (50% @ 1R, 50% продолжает)
- `time_based_partial.py` — Strategy #5 (50% @ 10m close, 50% продолжает)
- `metrics.py` — Sharpe, MaxDD, win_rate, profit_factor, per-ticker/per-year
- `run_comparison.py` — runner + Excel reporter
- `discovery.py` — initial профилирование (one-shot)
- `check_formulas.py`, `diagnose_*.py` — диагностические скрипты

### Reports:
- `data/discovery_report.json`
- `data/exits_comparison_<timestamp>.xlsx` (7 sheets)
- `data/cache/<TICKER>.parquet` × 19 файлов (~300 MB)

---

## Sanity check evolution — методологические находки

| Revision | Diff vs Phase 2 | Что изменено |
|----------|-----------------|--------------|
| REV1 | -85.08% | Initial — без lot_size |
| REV2 | -84.36% | + TP-first (ошибка, нужна SL-first) |
| REV3 | +3.33% | + `lot_size` множитель в PnL формуле |
| **REV4** | **-0.86%** | + Phase 2 window (включая entry-минуту и +1 бар после time-stop) |

**Финальная sanity:** -0.86% diff, exit_reason disagreement 0.8%.
Воспроизводимость Phase 2 baseline **подтверждена**.

---

## Ключевые методологические инсайты (для backlog)

### 1. Phase 2 vs production-honest window

Phase 2 включает entry-минуту в exit-проверку. На 1.6% сделок это даёт
"ретроспективный SL" — методологически **неверно** для live (мы только что
вошли — low минуты до нас не относится). Но Phase 2 baseline использует
эту конвенцию, мы воспроизвели точно.

**Для Phase 3 paper trading рекомендуется production mode:**
```python
strategy = BaselineFixedTpSl(
    entry_bar_inclusive=False,
    after_time_stop_bar_inclusive=False,
)
```

**Ожидаемая разница vs Phase 2 baseline:** ~+3.5% PnL (без look-ahead bias).
То есть **true edge на 3.5% выше** заявленного Phase 2 = Sharpe ≈ 5.15
(вместо 4.98).

### 2. SBER -146k стабильно убыточный на всех 5 стратегиях

SBER даёт убыток во всех вариантах — это **не exit problem**, это **entry problem**
mx_specific модели. Кандидат на **per-ticker excluded list** в Sprint 4.10:
LLM-фильтр может отсеять SBER-новости с low confidence.

### 3. PSEUDO-RUB issue (BR/NG/GLDRUB)

Phase 2 считает PnL для USD-фьючерсов в долларах, но называет _rub.
В коммите 4.0 этот баг сохранён для internal consistency.

**Для Phase 3:** правильный TICK_VALUE через QUIK API.
**Для Sprint 5 backtest:** post-hoc correction через USDRUB(t) множитель.

---

## Что использовать в Phase 3

**Exit strategy: `baseline_fixed_tp_sl`** (Phase 2 fixed TP + SL + time-stop)
- TP_FRACTION = 0.7
- SL_BUFFER = 1.2
- MIN_SL_DIST = 0.0005 (0.05%)
- MIN_TP_DIST = 0.001 (0.10%)
- horizon = 60 минут (best combo)
- exit-order: SL-first, then TP

**Конфигурация production:**
- entry_bar_inclusive = False
- after_time_stop_bar_inclusive = False

**Реалистичная Sharpe expectation для live:**
- Backtest 2025: 5.66
- 50% degradation factor: 2.8 - 3.5
- (учитывает slippage live > backtest, retrain делагирование, рыночные шоки)

---

## Backlog для будущих коммитов / Sprint 5

1. **Per-ticker excluded list** — exclude SBER из mx_specific torch, потому что
   стабильно убыточный во всех exit-стратегиях.
2. **Dynamic exit per LLM-category** — если LLM-категория "cbr/high" → baseline;
   "corporate dividends" → breakeven (trend continuation характер).
3. **Partial с reduced cost** — повторить эксперимент, если найдём способ
   уменьшить slippage/commission (e.g. limit-orders, дробление по разным брокерам).
4. **Trailing на subset** — экспериментально на "slow" expected_timeframe от LLM,
   где tend continuation возможен.

---

## DoD коммита 4.0 — статус

| Критерий | Целевое значение | Факт | Статус |
|----------|------------------|------|--------|
| Все 5 стратегий реализованы | ✅ | ✅ | OK |
| Bar-by-bar симуляция на 100% сделок | ✅ | 3300/3300 | OK |
| Кэш parquet, повторный прогон < 5 мин | < 5 мин | 30 сек | OK |
| Excel-отчёт с 6+ листами | ≥ 6 листов | 7 листов | OK |
| Best strategy выбрана с rationale | ✅ | baseline (Sharpe 4.98) | OK |
| Unit-тесты на каждую стратегию | ≥ 5 | 14 (3-4 на каждую) | OK |
| Sanity baseline воспроизводит Phase 2 | ±1% | -0.86% | OK |

**Sprint 4 / Commit 4.0 — закрыт ✅**

---

## Следующий шаг: Commit 4.1

`instrument registry deployment` — интегрировать instruments.py в основной проект
(перенести в `src/contracts/instruments.py`), добавить unit-тесты на mapping
(`normalize_ticker("Si")` → "SI", etc.), интегрировать в `RawNewsEvent`/
`EnrichedNewsEvent` validators.

После 4.1 → 4.2 (dataset discovery telegram_news.jsonl + TZ verification).
