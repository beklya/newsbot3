# Sprint 6.3 — DONE (отрицательный результат, обоснованный)

**Дата**: 2026-06-10
**Вопрос спринта**: существует ли конфигурация стратегии (rr_threshold, min_mfe_pct,
direction_filter_min_confidence, whitelist) с положительным Sharpe на walk-forward
Y6-периода (2025-01-01 → 2026-06-06) при **реальной тарификации Сбер «Самостоятельный»**?

**Ответ: НЕТ.** 0 рабочих конфигураций из 440 точек сетки (240 base + 200 extended).
Gross edge стратегии реален (+1.81M ₽ на 32k trades), но в **~20 раз меньше** честных
round-trip costs (+57₽ gross/trade против 1,119₽ cost/trade). Это статус всей
news-flow стратегии на 60-минутном горизонте, не отдельного конфига.

---

## 1. Контекст: фиктивный baseline Sprint 6.1/6.2

`scripts/replay_vps_window_backtest.py` имел захардкоженный `cost_rub = 2.0` на
round-trip. Реальный cost при production-sizing (risk 0.5% × 500k, leverage cap 10×,
notional 300–900k ₽) — **~1,200₽/trade** (стоки 0.19% RT, валюты 0.50%, фьючерсы 0.08%).

### Task 1 — фикс и точная сверка
Замена на `cost_rub = notional × ROUND_TRIP_COST_PCT[ticker]`
([scripts/costs_sber.py](../scripts/costs_sber.py)). Re-run на тех же 1766 events
2026-06-01..05 (DI-swapped):

| | legacy 2₽ | честные costs |
|---|---|---|
| Trades (selection идентичен) | 61 | 61 |
| Total PnL | **+12,566.81 ₽** | **−60,597.75 ₽** |
| Sharpe | +4.11 | −23.49 |
| Mean cost/trade | 2 ₽ | 1,201.42 ₽ |

Сверка: `−60,597.75 = +12,566.81 − 73,286.56 + 61×2` — бит-в-бит, вся разница = costs.
Направление совпадает с live Bridge 2026-06-09 (терял 200–1900₽/trade vs offline).

### Тарифная модель (источник: tariff_self.pdf, с 14.07.2025)

| Класс | Брокерка RT | MOEX | Slippage RT | **Итого RT** | Break-even pred_MFE (÷0.7) |
|---|---|---|---|---|---|
| Стоки TQBR | 0.120% | 0.02% | 0.05% | **0.19%** | 0.271% |
| Валюты CETS (USDRUB/CNY/GLDRUB) | 0.40% | ~0.003% | 0.10% | **0.50%** | 0.714% |
| Фьючерсы FORTS (SI/MIX/BR/NG) | 0.030% | small | 0.05% | **0.08%** | 0.114% |

---

## 2. Walk-forward инфраструктура (новая, переиспользуемая)

[scripts/walk_forward_y6_honest.py](../scripts/walk_forward_y6_honest.py):
- Train: rolling 12 мес из v2 корпуса (Phase2-DI + Y6, единая DI 70B дистрибуция),
  per-fold обучение 60m-моделей. Test: 2-недельные окна по Y6-периоду, шаг 2 нед → **38 фолдов**.
- Sizing: production `compute_size` (как live Decision). Costs: per-class от notional.
- B_filter LENIENT воспроизведён из feature-колонок (`dir_*` + `confidence`).
  **NB: harness Sprint 6.1 (`walk_forward_v7_all_folds.py`) фильтр НЕ применял вопреки docstring.**
- **Two-phase дизайн**: обе стороны каждой строки симулируются один раз →
  `outcomes.parquet`; любой sweep-конфиг — векторная выборка за секунды
  ([scripts/sweep_y6_grid.py](../scripts/sweep_y6_grid.py), 240 точек за ~18с).

## 3. 🔴 Найден баг данных: merge только по `id`

`train_predictor_fold13.merge_features_and_targets()` джойнит features×targets
**только по `id`**, выбрасывая `ticker` из targets. Phase 2 id были per-(event,ticker) —
там 1:1. **Y6 id — per-event**: событие с N тикерами кросс-джойнится → инфляция строк
×2.8 (484,616 → 170,526 outcome rows после фикса) и **чужие label'ы в train**
(MFE другого тикера как таргет).

- Файлы features/targets сами чистые (67,790/67,792 уникальных (id,ticker)).
- **v7_70b_v2 (прод-модели) обучены через этот баг**: fold13 train_n=226,125 из
  136,798 реальных строк (зафиксировано в SPRINT_6_1_DONE и не замечено).
- Фикс: `merge_features_targets_clean()` — строгий 1:1 по (id, canonical ticker).
- **Гипотеза «label noise маскировал edge» проверена и отвергнута**: per-fold модели
  на чистом мёрдже дали sweep ХУЖЕ (best угла −0.38 → −1.67 Sharpe).
- Артефакт первого прогона: «кластер VTBR 14.02.2025» (8 идентичных trades, +128k,
  весь «плюс» extended-сетки) — схлопнулся в 0 на чистых данных.

## 4. Результаты (чистый мёрдж, 38 фолдов)

### Baseline (прод-конфиг rr=1.0, min_mfe=0, conf=0.5, full 19)
| Метрика | Значение |
|---|---|
| Trades | 32,029 |
| Gross PnL | **+1,814,500 ₽** |
| Costs | **−35,824,819 ₽** |
| Net PnL | −34,010,319 ₽ |
| Sharpe | −23.58 (fold-mean −29.97) |
| Фолды в плюсе | **0/38** |

### Sweep
- **Base grid 240 точек** (rr 1.0–2.0 × mfe 0–0.3% × conf 0.45–0.60 × 3 universe):
  0 positive Sharpe. Best: rr=2.0/mfe≥0.3/conf≥0.55/stocks_only → **−1.67**, n=162, −62.5k ₽.
- **Extended grid 200 точек** (rr→5, mfe→1.5%): все «плюсы» — единичные сделки.
  Headline rr=2.0/mfe≥1.0: n=150, +18.6k, из них **один trade = +34.9k (187% total)**;
  без него −16.2k. rr≥5 → 0–2 trades.
- **Robust-гейт (n≥200, ≥70% фолдов в плюсе): 0 из 440.**

### Cost-floor анализ ([cost_floor_analysis.md](../data/walk_forward/y6_sweep/cost_floor_analysis.md))
| Класс | % строк выше break-even | net/trade | net/trade above-floor подвыборки |
|---|---|---|---|
| Валюты | **0.3–3%** | −4,884 ₽ | −560…−2,036 ₽ (всё равно минус) |
| Фьючерсы | 92.7% | −684 ₽ | −610 ₽ |
| Стоки | 58.4% | −1,578 ₽ | −1,134 ₽ |

Floor-условие необходимое, но не достаточное: экономика выходов
**TP +337₽ / time −1,664₽ / SL −3,616₽** — выигрыш маленький (TP-дистанция 0.7×mfe
сравнима с костами), проигрыш большой (SL-дистанция 1.2×mae) плюс ~1,100₽ кости всегда.
Геометрия Phase 2 §5.3 калибровалась при costs≈0.

### Task 4 — universe
**Валюты (USDRUB/CNY/GLDRUB) структурно мертвы** при тарифе 0.40%: выше floor 0.714%
только 0.3–3% предсказаний, above-floor подвыборка тоже убыточна. Решение: исключить
из любых будущих конфигураций. (Пока стратегия в целом отрицательна — вопрос moot,
зафиксировано на будущее.)

## 5. Гипотеза скорости (секундные свечи/тики) — проверена, отвергнута

[scripts/analyze_entry_jump.py](../scripts/analyze_entry_jump.py): jump = движение
от pre-news close к нашему entry (open следующей минуты), со знаком в сторону сделки.
- ALL rows: mean **+0.0000%**, median 0.0000% — систематического раннего движения НЕТ.
- Baseline-selected: +0.0014% (0.14 б.п. — шум).
- Corner: TP-победители jump **−0.068%** (входили на откате), SL-проигравшие +0.061% —
  ранний вход не спасает победителей, micro-momentum антикоррелирует с исходом.
- Latency-бюджет: enrich p50 **6.9s** / p90 16.9s / p99 36.4s + телеграм-лаг + пайплайн —
  гонка за первые секунды недоступна и, по данным, не нужна.

→ Секундные свечи / тиковая лента edge не разблокируют. Сбор тиков не запускаем.

## 6. Применённые изменения

1. `replay_vps_window_backtest.py` — честные costs (флаг `--legacy-flat-cost 2.0`
   для воспроизведения старых чисел).
2. `BridgeSettings.brokerage_rt_pct` → тариф Сбер: стоки 0.14 (0.12+0.02 MOEX),
   фьючерсы 0.03, валюты (вкл. GLDRUB/CNY) 0.40. Slippage оставлен per-ticker (Phase 2).
   Любой будущий paper soak теперь считает честно.
3. `.env`: откачены временные overrides (DAILY_KILL_PCT=0.99, COOLDOWN_TICKER_SEC=0,
   MAX_OPEN_POSITIONS=20).
4. `DecisionSettings` defaults НЕ менялись — «оптимального» конфига не существует.
5. Live-validation (Task 5 acceptance) не проводилась — валидировать нечего.

## 7. Sprint 6.4 — кандидаты (по убыванию перспективности)

1. **Horizon 240m+**: цели должны быть в разы больше cost floor; длинный горизонт даёт
   большие ходы при том же RT cost. Нужно дериватнуть targets 240m из прайсов
   (`derive_targets_for_y6_corpus.py` за образец) — harness готов, прогон ~15 мин.
2. **Event-tier селективность**: деньги сидят в редких больших событиях
   (единичные большие trades в extended-сетке — реальные движения, просто их мало).
   Гейты по urgency/impact_strength + торговля штучно, не потоком.
3. **Cost-aware target**: `max(0, mfe − cost_floor)` или классификатор
   «ход > 2× cost» вместо регрессии сырого MFE.
4. **v8 retrain на чистом мёрдже** — гигиена данных в любом случае (прод v7 обучен
   на label noise), но per-fold чистые модели edge на 60m НЕ улучшили — от retrain
   одного только чуда не ждать.
5. **Заморозка проекта** — честная опция: 17 месяцев данных говорят, что news-flow
   на минутных горизонтах не выживает при retail-тарифе.

## Артефакты

| Path | Что |
|---|---|
| `scripts/costs_sber.py` | Тарифная модель (shared) |
| `scripts/walk_forward_y6_honest.py` | Walk-forward harness (clean merge, prod sizing, B_filter, two-phase) |
| `scripts/sweep_y6_grid.py` | Векторный sweep (240 base + 200 extended точек) |
| `scripts/analyze_y6_cost_floor.py` | Task 4: pred_MFE vs cost floor |
| `scripts/analyze_entry_jump.py` | Гипотеза скорости (jump-анализ + latency) |
| `scripts/dump_y6_outliers.py` | Форензика outlier-кластеров |
| `scripts/check_y6_duplication.py` | Диагностика дупликации merge |
| `data/replay/sprint6_3_honest_costs_summary.json` + `_trades.csv` | Task 1 re-run |
| `data/walk_forward/y6_honest_costs_baseline/` | outcomes.parquet (170,526 rows) + summary + folds_meta |
| `data/walk_forward/y6_sweep/` | grid_results[_ext].csv, top10_report[_ext].md, cost_floor_analysis.md |
