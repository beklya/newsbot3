# Sprint 6.1 — LENIENT-revert validation + distribution-shift diagnostics

**Дата**: 2026-06-06
**Контекст**: после отката `src/services/decision/filter.py` к Sprint 4 LENIENT-семантике
(см. [docs/B_FILTER_ARCHITECTURE.md](B_FILTER_ARCHITECTURE.md), section "Filter semantics
regression"), проверили работают ли live-данные так же как walk-forward.

**Короткий вывод**: код корректен, но **live data в июне 2026 показывает distribution
shift настолько большой, что doc-цифры (Sharpe 5.03) не воспроизводятся** — реально
−16.8 Sharpe на 5-дневном live окне.

---

## Что валидировано

### C — walk-forward на extended prices

| Параметр | Значение |
|---|---|
| Скрипт | `scripts/walk_forward_b_filter.py --prod-filter` |
| Данные | Phase 2 trades + 70B v1.0.0 enrichment + **обновлённые** `prices_*.csv` (с Sprint 6.1 futures backfill 2026-04-21 → 06-05) |
| Mean Sharpe | **5.03** |
| Folds positive | 13/13 |
| Total trades | 3167 |
| Total PnL | +2 126 953 ₽ |

**Идентично прошлому прогону до B1.** Backfill BR/NG/SI/MIX через BRN6/NGM6/SiM6/MXU6
не сломал walk-forward, потому что fold 13 заканчивается 2026-04-03, до splice-jump
2026-04-21.

### Equivalence guard — production filter.py ≡ sprint4 reference

| Тест | Результат |
|---|---|
| `tests/services/decision/test_filter_walk_forward_equivalence.py` | 206 / 206 pass |
| `tests/services/decision/test_filter.py` | 9 / 9 pass |
| Full pytest | 406 pass, 1 skipped |

Прод-фильтр после revert byte-for-byte эквивалентен Sprint 4 winning DirectionFilter
на полном cartesian product edge-cases.

---

## VPS replay через LIVE Decision pipeline

### Окно

| | |
|---|---|
| Источник | VPS Redis `news:enriched` через SSH-туннель 6380 |
| Период | 2026-06-01 → 2026-06-05 (5 трейдинговых дней) |
| Events | 1766 (после фильтрации по news_time) |
| Скрипт | `scripts/replay_vps_window_backtest.py` |

### Три режима фильтра на одной выборке

| Mode | Trades | Total PnL | Per-trade | Win% | Sharpe |
|---|---:|---:|---:|---:|---:|
| **LENIENT** (post-revert prod) | 43 | −43 394 ₽ | −1 009 ₽ | 25.6% | −16.80 |
| **STRICT** (pre-revert broken prod) | 42 | −43 792 ₽ | −1 043 ₽ | 23.8% | −16.75 |
| **NO_FILTER** (XGBoost only, A1) | 259 | −191 846 ₽ | −741 ₽ | 29.3% | −18.67 |

### A1 finding — B_filter работает как anti-selector

- Total: B_filter спасает **148 452 ₽** через отказ от 216 trade-ов
- Per-trade: B_filter оставляет ХУДШИЕ trade-ы (−1009 ₽ vs −741 ₽ universe avg)
- **Проблема НЕ в фильтре**, а в Predictor — без фильтра win-rate всего 29%

---

## A2 — drill-down по тикерам

### Концентрация потерь LENIENT-43

| Ticker | Trades | PnL | Win% | Доля loss |
|---|---:|---:|---:|---:|
| **GAZP** | 13 | **−24 003** | 15.4% | **55%** |
| VTBR | 7 | −6 680 | 14.3% | 15% |
| LKOH | 5 | −3 805 | 40.0% | 9% |
| ROSN | 2 | −2 748 | 0% | 6% |
| NG | 1 | −2 502 | 0% | 6% |
| GMKN | 1 | −2 502 | 0% | 6% |
| NVTK | 1 | −2 501 | 0% | 6% |
| BR | 12 | −1 699 | 41.7% | 4% |
| MGNT | 1 | **+3 046** | 100% | — |

### Side imbalance

| Side | n | PnL | Win% |
|---|---:|---:|---:|
| BUY | 42 | −46 552 | 23.8% |
| SELL | 1 | +3 158 | 100% |

**42 из 43 trades = BUY.** Predictor систематически over-bullish на этом окне.
Это причина почему bull-regime новости с long-bias LLM endorsement-ов в итоге дают
loss: цены делают sell-the-news, а наши SL ловят полный SL drawdown.

### B_filter quality per ticker (avg PnL vs universe)

Положительное `picks_better_than_universe` = фильтр работает, отрицательное = anti-works.

| Ticker | LENIENT avg | NO_FILTER avg | Δ |
|---|---:|---:|---:|
| BR | −142 | −1 246 | **+1 104** ✅ |
| ROSN | −1 374 | −2 060 | **+686** ✅ |
| LKOH | −761 | −852 | +91 ✅ |
| GMKN | −2 502 | −2 502 | 0 |
| MGNT | +3 046 | +3 479 | −433 |
| VTBR | −954 | −197 | −757 ⚠️ |
| NVTK | −2 501 | −1 559 | −942 ⚠️ |
| GAZP | −1 846 | −66 | **−1 780** ⚠️ |
| NG | −2 502 | −118 | **−2 384** ⚠️ |

- **На BR/ROSN/LKOH B_filter селективен в правильную сторону**
- **На GAZP/NG/VTBR/NVTK фильтр anti-works** — оставляет именно худшие сделки

Это объясняет почему по сумме B_filter "спасает" 148k₽, но per-trade keeps хуже:
большой положительный эффект на BR/ROSN перевешивается катастрофическим на GAZP.

---

## A3 — calibration drift в LLM 70B

### Distribution sравнение Phase 2 (2022-2026Q1) vs VPS (June 2026)

| Метрика | Phase 2 | VPS | Δ |
|---|---:|---:|---|
| `confidence` median | 0.50 | **0.45** | shift down |
| `confidence ≥ 0.5` share | **51.4%** | **21.9%** | **−29.5pp** 🚨 |
| `confidence ≥ 0.7` share | 6.4% | 0.8% | −5.6pp |
| `confidence == 0.5` cluster | 11.4% | 4.0% | "soft endorsement" исчез |
| direction `long` | 27.1% | **61.8%** | **+34.7pp bullish** |
| direction `short` | 25.4% | 37.2% | +11.8pp |
| direction `neutral` | 0.4% | 1.0% | |

### Per-ticker LLM direction на VPS (long-bias)

Все основные тикеры показывают **65-75% long endorsement** — bull-regime в новостях:

| Ticker | n | long | short |
|---|---:|---:|---:|
| ROSN | 105 | 75% | 24% |
| BR | 76 | 72% | 26% |
| NG | 14 | 71% | 29% |
| LKOH | 91 | 71% | 25% |
| VTBR | 71 | 69% | 28% |
| SBER | 262 | 68% | 30% |
| GAZP | 291 | 51% | 49% ← **только GAZP сбалансирован, и он же главный destructor** |

---

## Какие выводы

1. **Walk-forward 5.03 Sharpe не предсказывает live** — это **distribution-shift между
   Phase 2 trades (2022-2026Q1) и June 2026 enrichment/market regime**, ровно та
   опасность, о которой явно предупреждает `docs/B_FILTER_ARCHITECTURE.md`.

2. **Calibration в 70B меняется со временем** — confidence-распределение сдвинулось
   за 4-6 месяцев на −29pp в зоне ≥0.5. `direction_filter_min_confidence=0.5`
   перешёл из "median-cutoff" в "top-22%-cutoff".

3. **Predictor over-bullish bias** — 42/43 LENIENT-trades = BUY. XGBoost на 70B-features
   систематически выбирает long side в bull-news regime. SELL-сетапы R:R-rejected.

4. **B_filter работает per-ticker неравномерно** — saves на BR/ROSN/LKOH, anti-works
   на GAZP/NG/VTBR. Возможно нужен per-ticker tuning или подгон min_confidence к
   текущей calibration.

5. **5 дней / 43 trades — недостаточный sample** для финального вердикта. Walk-forward
   доверяет на 2569 trades × 13 folds. Для honest replicate нужно 4-8 недель сбора.

6. **Sprint 6.1 code level done** — filter, tests, equivalence guard, futures backfill,
   walk-forward verification. **Paper-PnL validation milestone — НЕ пройден**. Перед
   prod-deploy решения с реальными деньгами нужно либо (a) собрать больше данных и
   повторить, либо (b) принять регрессионную возможность.

---

## Sprint 6.2 backlog (от этого валидационного раунда)

- [ ] Калибровочные drift-метрики в Monitor: alerts на conf-distribution shift > 10pp
- [ ] Per-ticker `direction_filter_min_confidence` (или per-ticker отдельные веса)
- [ ] Futures roll-aware stitching при contract expiry (Sprint 6.1 захардкодил
      BRN6/NGM6/SiM6/MXU6, авто-detection переехал в backlog)
- [ ] 4-8 недель paper-soak, потом повторить эту же валидацию
- [ ] Расследовать over-bullish bias Predictor v1 на 70B features — это main contributor
      к 42/43 BUY-skew

---

## Артефакты

| Путь | Что |
|---|---|
| `scripts/pull_enriched_from_vps.py` | XRANGE через tunnel → JSONL |
| `scripts/replay_vps_window_backtest.py` | live PredictorPipeline + DecisionPipeline + sprint4 paper-sim |
| `scripts/probe_moex_futures.py` | discovery FORTS контрактов |
| `scripts/backfill_prices.py` (extended) | + BRN6/NGM6/SiM6/MXU6 mapping |
| `tests/services/decision/test_filter_walk_forward_equivalence.py` | 206-case prod≡sprint4 guard |
| `data/replay/enriched_vps_window.jsonl` | 1766 events 2026-06-01 → 06-05 |
| `data/replay/vps_window_backtest_report_{b1,a1_no_filter,a4_strict}.json` | 3 режима |
| `data/replay/vps_window_trades_*.csv` | per-trade dumps |
| `data/reenrich_phase2/walk_forward/sprint6_1_prod_filter_post_b1/` | C-validate output |
