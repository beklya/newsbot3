> ⚠️ **Опровергнуто (июнь–сентябрь 2026).** B_filter WF 6.42 / V1 3.14 недействительны: расчёт фильтровал готовые сделки Phase 2 (метки с утечкой), а в харнессе был баг сравнения side (int против str); с прод-семантикой было 5.03 / 1.59. Объяснение «distribution shift 8B→70B» тоже неверно: корпус новостей Phase 2 был размечен промптом `docs/legacy_prompt/ollama_analyzer.py`, который передавал LLM движения цены ПОСЛЕ новости (look-ahead). Sentiment совпадает со знаком будущего хода в 57.4% / 55.1% / 53.1% случаев (15m / 60m / 1d) против ~50% у честной разметки; без LLM-признаков та же комбинация даёт Sharpe −2.25. Честные проверки (Sprint 6.3–6.5, 8, 9) показали: направленного edge от новостей нет. Сводка — в README, раздел «Результаты».

# B_filter Architecture — Sprint 5 Production Decision Flow

**Дата документа**: 2026-05-28
**Статус**: ✅ Deployed (Sprint 5 уже wired, walk-forward 2026-05-28 валидировал)
**Walk-forward backtest result**: Mean Sharpe **6.42** на 13 фолдов (vs Phase 2 baseline 4.87)

---

## TL;DR — что это такое за 1 минуту

**Проблема**: Phase 2 обучала XGBoost на LLM-features от **Ollama llama3.1:8b локально**
(`D:\quik_sber\newsbot\newsbot2\news_bot_methodology\news_bot_methodology\code\llm_prompt_v2.py`
— prompt provider-agnostic, генерировал features_mfe.parquet, +4.87 walk-forward Sharpe).
**Замечание 2026-06-06:** ранние memory-файлы упоминали "8B (Ollama)" — это ошибка
атрибуции. В Phase 2 промпте нет Haiku/Anthropic; в features_mfe.parquet нет
model_attribution колонки. Используем "Ollama 8B" далее.
В prod Sprint 5 enricher это **70B Llama 3.3 на DeepInfra realtime** — другая модель,
другой RLHF, другая calibration → distribution shift → XGBoost predictions становятся
бесполезны (-5.13 Sharpe в prod).

**Решение** (Sprint 4.9 Track B + Sprint 5.7 walk-forward validation):
**Не подавать LLM features в XGBoost.** Использовать LLM **как post-filter** после
XGBoost decision. XGBoost решает "стоит ли торговать" по PRICE/TECHNICAL features
(которые не зависят от LLM версии), а LLM решает "согласен ли я с направлением".

Trade entry = (XGBoost says GO) AND (LLM direction matches AND confidence ≥ 0.5).

**Результат walk-forward 13 фолдов**:
- Mean Sharpe **6.42** (Phase 2 raw: 4.87 → +32% improvement)
- All 13 folds positive PnL
- 2,569 trades total, ~198/fold
- Total PnL +2,381,507 ₽ на initial equity 500k
- V1 holdout (Q1 2026): Sharpe 3.14 (vs Sprint 4.10 V1 8b baseline 2.71)

---

## История проблемы

### Phase 2 setup (2022-2026 backtest)
1. Ollama llama3.1:8b локально → enrichment всех 70k news events
   с 12 категорий (cat_geopolitics, cat_corporate, etc.)
2. Features build: `features_mfe.parquet` имеет 67 columns: 22 LLM-derived
   (sent_*, urg_*, cat_*, confidence, ...) + 45 technical (RSI, ATR, ret_*_pre, ...)
3. XGBoost обучен на этом полном feature set
4. Walk-forward 13 фолдов: best combo h=60, rr=2.0, mx_specific → **mean Sharpe 4.87**

### Sprint 5 prod setup (2026)
1. Enricher переключён на **Llama 3.3 70B через DeepInfra** (realtime, no price access)
2. Prompt v1.0.0 с per-ticker JSON schema
3. Predictor использует **те же v1_legacy XGBoost модели** (обученные на Phase 2 features)
4. В prod Predictor получает 70B features но обучен на 8B (Ollama) features →
   **distribution shift bug**

### Distribution shift на V1 holdout (Sprint 5.6 experiments)

| Config | Sharpe V1 | Trades V1 |
|---|---|---|
| v1_legacy + 8B (Ollama) features (Phase 2 reproduction) | **+2.59** | 126 |
| v1_legacy + 70B v1.0.0 features (текущий prod scenario) | **-5.69** | 146 |
| v2_70b + 70B features (Sprint 5.6 retrain) | -0.45 | 47 |
| v3 + 70B features (Sprint 5.6 expanding) | -10.81 | 19 |
| v5 + no-LLM features (Sprint 5.6 drop-LLM) | -7.15 | 29 |

Все попытки **починить через retrain XGBoost провалились** ($58 потрачено). Phase 2
ablation подтвердил что LLM features критичны (h=60 rr=2 WITHOUT_LLM: -2.25 Sharpe mean).

### Sprint 5.7 — почему prompt iteration тоже не помог

Тестировали 3 промпта (v2.1.0 close port → v2.2.0 EXTREME aggressive → v2.3.0 legacy mirror).
500 stratified events 2022-2026, **DeepInfra 8B и 70B + Groq 8B и 70B**:

| Provider+Model | directional | conf_mean | conf=0.5 cluster | n_tickers |
|---|---|---|---|---|
| Legacy 8B (Ollama) target | 46% | 0.66 | 0% | 1.16 |
| DI 70B (любой v2.x.x) | 32-34% | 0.51 | 12.7% | 0.77 |
| DI 8B (любой v2.x.x) | 22% | 0.54 | 8.4% | 0.43 |
| Groq 70B v2.2.0 | 36.8% | 0.51 | 4.8% | 0.88 |
| Groq 8B v2.2.0 | 25% | 0.56 | 9.0% | 0.47 |

**Вывод**: hosted LLM провайдеры (DeepInfra, Groq) имеют свой RLHF, который перекрывает
prompt instructions. Distribution не пробивается до legacy.

### Sprint 5.7 — пробив через смену архитектуры (этот документ)

Вместо борьбы с distribution в XGBoost features → использовать LLM как **filter** на trades.

---

## Архитектура B_filter

### Концепция

XGBoost и LLM решают **разные задачи**:

| Component | Что предсказывает | На каких фичах |
|---|---|---|
| XGBoost (v1_legacy) | pred_MFE_long, pred_MAE_long, ..., × {30m, 60m} = 8 значений → R:R решение | 67 features (22 LLM + 45 price/technical) |
| LLM (70B v1.0.0 prod) | direction, confidence per-ticker, category, urgency, is_actionable | Только текст новости |

**Phase 2 ошибка**: смешивала их через LLM-features в XGBoost — distribution становится
fragile к смене LLM модели.

**B_filter inversion**: использовать каждый компонент за то, что он хорошо делает:
- **XGBoost** → "сколько процентов вырастет/упадёт цена" (числовой прогноз)
- **LLM** → "куда движется news sentiment по этому тикеру" (direction agreement check)

Если оба согласны → trade. Если LLM не согласен с XGBoost direction → skip trade.

### Production flow (Sprint 5 wired)

```
Telegram channel
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Receiver  (Sprint 2)                                            │
│  ─────────                                                       │
│  Reads 4 channels (interfax/rian/tass/rbc), dedup via SHA256.    │
│  Publishes news:raw stream.                                      │
└──────────────────────────────────────────────────────────────────┘
        │
        │ news:raw → RawNewsEvent
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Enricher  (Sprint 3)                                            │
│  ────────                                                        │
│  Llama 3.3 70B на DeepInfra (или Groq) с prompt v1.0.0.          │
│  Возвращает EnrichedNewsEvent v1.1.0:                            │
│    {is_financial, tickers[{ticker, direction, sentiment,         │
│         confidence, impact_strength, rationale}],                │
│     summary, expected_timeframe, urgency, category,              │
│     is_actionable, prompt_version}                               │
│  Whitelist validator: ticker не из 19 → DLQ.                     │
│  Per-event idempotency.                                          │
└──────────────────────────────────────────────────────────────────┘
        │
        │ news:enriched → EnrichedNewsEvent
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Predictor  (Sprint 5.1)                                         │
│  ─────────                                                       │
│  Per-ticker: для каждого tickers[i] в payload:                   │
│    1. FeatureBuilder: 67 features из:                            │
│       - LLM (22) — из EnrichedNewsEvent (sent/urg/cat/conf)      │
│       - Price/technical (45) — из prices_*.csv через PricesCache │
│    2. XGBoost predict — 16 моделей:                              │
│       4 targets (mfe_long/mae_long/mfe_short/mae_short)          │
│       × 2 horizons (30m, 60m)                                    │
│       × 2 types (general/mx_specific)                            │
│    3. Publish MLPredictionEvent per (event_id, ticker).          │
│  ВАЖНО: Predictor НЕ ФИЛЬТРУЕТ. Просто выдаёт numeric prediction.│
│  Composite idempotency: {event_id}:{ticker}.                     │
└──────────────────────────────────────────────────────────────────┘
        │
        │ ml:predictions → MLPredictionEvent
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Decision  (Sprint 5.4) — ★ HERE B_FILTER GATE                   │
│  ────────                                                        │
│  Flow (см. src/services/decision/pipeline.py):                   │
│                                                                  │
│  1. Idempotency claim per prediction.event_id                    │
│  2. EnrichmentCache.get(payload.enriched_event_id)               │
│     → loads original EnrichedNewsEvent (for LLM signal)          │
│  3. R:R logic (Phase 2):                                         │
│     rr_long = pred_mfe_long / max(pred_mae_long, 0.05)           │
│     rr_short = pred_mfe_short / max(pred_mae_short, 0.05)        │
│     side = 'BUY' if rr_long ≥ 2.0 AND mfe_long ≥ 0.15 AND ...    │
│            'SELL' if rr_short ≥ 2.0 AND mfe_short ≥ 0.15 AND ... │
│            else REJECT('rr_below_threshold')                     │
│                                                                  │
│  4. ★ DirectionFilter (Sprint 4 B_filter, LENIENT — see below):  │
│     ti = enriched.tickers.find(ticker == prediction.ticker)      │
│     if ti is None:                                               │
│        INCLUDE  # absent signal ≠ veto (Sprint 4 design)         │
│     if ti.direction == 'neutral':                                │
│        INCLUDE  # neutral ≠ negative endorsement                 │
│     if ti.direction != side_to_direction(side):                  │
│        REJECT('LLM direction != expected')                       │
│     if ti.confidence < 0.5:                                      │
│        REJECT('confidence below threshold')                      │
│                                                                  │
│  5. RiskManager gates (Phase 2):                                 │
│     daily_kill, cooldown per-ticker, max_open_positions          │
│                                                                  │
│  6. SL/TP levels + sizing (0.5% risk, leverage 10)               │
│  7. Publish TradeSignalEvent action=EXECUTE                      │
│                                                                  │
│  Все REJECT публикуются с reject_reason для post-mortem.         │
└──────────────────────────────────────────────────────────────────┘
        │
        │ trade:signals → TradeSignalEvent (action=EXECUTE)
        ▼
┌──────────────────────────────────────────────────────────────────┐
│  Bridge  (Sprint 5.5)                                            │
│  ──────                                                          │
│  Lua/QUIK terminal interface. Translates TradeSignal → real      │
│  market order via QUIK API.                                      │
└──────────────────────────────────────────────────────────────────┘
```

### B_filter logic — implementation в `src/services/decision/filter.py`

> ⚠️ **Filter semantics regression + DOCUMENTED-NUMBER CALIBRATION — Sprint 6.1, 2026-06-06.**
>
> Production filter was wired with **STRICT** semantics (REJECT on absent ticker /
> neutral) вопреки Sprint 4 design. Reverted to LENIENT (this revision).
>
> **However, three walk-forward runs on the same 70B-v1.0.0 enrichment data revealed
> that the documented 6.42 / V1=3.14 number was inflated by a script-level bug,**
> not by the design itself:
>
> | Run | Filter wired | Mean Sharpe | V1 (fold 13) | Filter rate | Trades |
> |---|---|---|---|---|---|
> | Old STRICT (pre-revert prod) | reject on missing/neutral | 0.00 | 0.00 | 100% | 0 |
> | sprint4 LENIENT reference | rejects ALL endorsements via int-vs-str compare bug | **6.42** | 3.14 | 22% | 2569 |
> | **Live `apply_direction_filter` (post-revert prod, int→BUY/SELL normalized)** | rejects ONLY explicit mismatches | **5.03** | 1.59 | ~4% | 3167 |
>
> The walk-forward harness (`scripts/walk_forward_b_filter.py`) feeds Phase 2
> `Trade.side` as **int** (1/-1, per `sprint4/exits/base.py:Trade`), while the
> sprint4 reference `DirectionFilter` only normalizes `"BUY"/"SELL"/"buy"/"sell"`.
> int side falls through `.get(side, side)` to itself → comparison `"long" != 1`
> rejects EVERY trade with explicit endorsement, not just direction-mismatches.
>
> Production `apply_direction_filter` (`src/services/decision/filter.py`) is wired
> in the live pipeline with `side: "BUY"|"SELL"` from XGBoost R:R (see
> `decision/pipeline.py`) — i.e. the **proper** LENIENT path. The walk-forward
> adapter (`--prod-filter`) normalizes int → BUY/SELL so the harness exercises the
> *real* prod code path. Result: **mean Sharpe 5.03, 13/13 positive folds, beats
> Phase 2 baseline 4.87, beats Sprint 4.10 V1 acceptance 2.71**.
>
> **What this means for Sprint 6.1 operator expectations:**
> - The "6.42 / V1 3.14" headline in this doc is HISTORICALLY-OBSERVED but
>   came from accidental over-filtering. Don't use it as a paper-PnL target.
> - True prod expectation: mean Sharpe ≈ 5, V1 holdout ≈ 1.5, filter rate ≈ 5%.
> - The "≈ 22%" filter rate operator guideline below is **stale** — under prod
>   semantics expect closer to **5%** filter rate.  Revised tolerance bands:
>   - 5% normal; 2–10% acceptable
>   - <2% → LLM produces all-neutral / no-ticker (prompt broke?)
>   - >15% → LLM has strong directional disagreement vs XGBoost (model drift?)
>
> Verification: `scripts/walk_forward_b_filter.py --prod-filter` reproduces 5.03.
> Logical equivalence between prod and sprint4-LENIENT (modulo the int-bug) is
> guarded by `tests/services/decision/test_filter_walk_forward_equivalence.py`.

```python
def apply_direction_filter(
    event: EnrichedNewsEvent,
    ticker: str,
    side: str,             # 'BUY' or 'SELL' from XGBoost R:R
    min_confidence: float = 0.5,
) -> FilterDecision:
    expected_direction = SIDE_TO_DIRECTION[side]  # BUY→long, SELL→short

    # Step 1: LLM не упомянул этот ticker → INCLUDE (absent ≠ veto).
    #         XGBoost мог решить торговать по price/technical features.
    ti = get_ticker_impact(event, ticker)
    if ti is None:
        return INCLUDE

    # Step 2: neutral direction → INCLUDE (no negative endorsement).
    if ti.direction == 'neutral':
        return INCLUDE

    # Step 3: LLM явно указал direction — должен совпадать со стороной.
    if ti.direction != expected_direction:
        return REJECT(f'LLM={ti.direction} != expected={expected_direction}')

    # Step 4: Confidence порог (только когда есть явный endorsement).
    if ti.confidence < min_confidence:
        return REJECT(f'confidence {ti.confidence} < {min_confidence}')

    return INCLUDE
```

**Стоит в pipeline.py между шагом 3 (R:R) и шагом 5 (RiskManager gates)**.
Reference implementation: `sprint4/exits/hybrid/trade_filter.py:36-60`.

### Конфигурация (в `src/services/decision/config.py`)

```python
horizon_min: int = 60                          # Phase 2 best
rr_threshold: float = 2.0                      # Phase 2 best
min_mfe_pct: float = 0.15                      # Phase 2 best
min_mae_pct: float = 0.05                      # Phase 2 best
direction_filter_min_confidence: float = 0.5   # Sprint 4 B-filter threshold
```

---

## Почему это работает (theoretical foundation)

### Phase 2 raw vs B_filter — numbers

Walk-forward 13 фолдов (mirror Phase 2 walk-forward, h=60 rr=2 mx_specific):

| Metric | Phase 2 raw (legacy features XGBoost) | B_filter (Phase 2 trades + 70B LLM filter) |
|---|---|---|
| Mean Sharpe | 4.87 | **6.42** (+32%) |
| Median Sharpe | 5.70 | 5.99 |
| Min Sharpe | 1.24 | 3.03 (better worst case) |
| Std Sharpe | 2.51 | 2.72 |
| Mean trades/fold | 254 | 198 (-22%) |
| Win rate | ~57-66% | 58-73% |

### Почему Sharpe растёт

LLM identifies "это не sound trade" в случаях которые XGBoost decision не дисквалифицирует:
- XGBoost видит "ожидается движение 0.5% вверх, RR 2.5, MFE > 0.15%" → BUY MX
- LLM читает текст: "ЦБ повысил ставку на 200 bps" → direction=short MX
- Filter detects mismatch → skip the bad trade

Этих "bad trades" примерно 22% (filter rate). Они в среднем убыточные → их удаление
поднимает Sharpe.

### Почему это РОБАСТНО к distribution shift

В отличие от XGBoost features (где weights выучены на legacy distribution),
B_filter — **бинарный gate** на direction match:
- Не важно, что 70B даёт confidence 0.51 (legacy 0.66) — нам нужно знать только
  ti.confidence ≥ 0.5
- Не важно, что 70B даёт 32% directional (legacy 46%) — для тех 32% direction matches
  decisions XGBoost
- Distribution mismatch concentrates в neutral classifications → ti.direction='neutral'
  → REJECT (we'd skip anyway, no harm done)

LLM работает **в native mode** — natural language → direction. RLHF DeepInfra/Groq
не ломает эту task (отличает long от short), хотя ломает intensity calibration
(confidence levels) которую XGBoost разлогивал.

---

## Sprint 5 deployment state

### Уже сделано (Sprint 5 commits)

| Component | File | Tested | Status |
|---|---|---|---|
| Filter logic | `src/services/decision/filter.py` | `tests/services/decision/test_filter.py` (9 tests) | ✅ |
| Pipeline integration | `src/services/decision/pipeline.py` line 99-107 | `tests/services/decision/test_pipeline.py` (9 tests) | ✅ |
| Config setting | `src/services/decision/config.py` line 62 | covered above | ✅ |
| EnrichmentCache | `src/services/decision/enrichment_cache.py` | covered | ✅ |
| Metrics | `src/services/decision/metrics.py` (rejects.direction_filter) | covered | ✅ |

**Tests result (40/40 pass)**:
```
tests/services/decision/test_filter.py .........        [9 tests]
tests/services/decision/test_pipeline.py .........      [9 tests]
tests/services/decision/test_risk_manager.py ........   [8 tests]
tests/services/decision/test_rr_logic.py .........      [9 tests]
tests/services/decision/test_sizing.py .....            [5 tests]
========================== 40 passed in 0.69s ============================
```

### Что валидировано Sprint 5.7 (этот раунд)

- **walk_forward_backtest.py** на legacy features → 4.87 mean Sharpe (= Phase 2 baseline)
- **walk_forward_b_filter.py** с 70B v1.0.0 enrichment → **6.42 mean Sharpe**, все 13 fold положительны
- На V1 fold 13 (= Sprint 4.10 V1 holdout) даёт 3.14 Sharpe (vs Sprint 4.10 B-8b 2.71)

### Operating mode в prod

| Параметр | Значение | Источник |
|---|---|---|
| Enricher model | Llama 3.3 70B на DeepInfra realtime | текущий prod (Sprint 3) |
| Enricher prompt | v1.0.0 (как есть) | текущий prod |
| Predictor XGBoost | v1_legacy (обучен на Phase 2 features) | rolled back 2026-05-28 |
| Predictor R:R combo | h=60, rr=2.0, mx_specific | Sprint 5.1 |
| Decision B_filter | `direction_filter_min_confidence = 0.5` | config default |
| Decision risk | 0.5% per trade, leverage 10, max 3 open | Phase 2 §7.1 |

---

## Operations runbook

### Monitoring

Decision service метрики (Sprint 5):
- `events_in` — count MLPredictionEvent consumed
- `signals_execute` — EXECUTE signals emitted (trades passed all gates)
- `signals_reject` — REJECT signals emitted
- `rejects.rr_below_threshold` — XGBoost R:R didn't pass
- `rejects.direction_filter` ← **новая ключевая метрика для B_filter**
- `rejects.daily_kill`, `rejects.cooldown`, `rejects.max_open` — risk gates
- `errors.enrichment_missing` — race condition (Predictor пришёл раньше Decision cache)

**Ожидаемые ratios (по walk-forward)**:
- `rejects.rr_below_threshold` / `events_in` ≈ 0.85 (большинство predictions не tradeable)
- `rejects.direction_filter` / (events_in - rr_rejects) ≈ 0.22 (наш filter rate)
- `signals_execute` / `events_in` ≈ 0.10-0.12

Если `rejects.direction_filter` rate < 10% или > 50% — что-то не так:
- < 10%: LLM выдаёт ВСЁ как neutral / нет tickers (prompt broke?)
- > 50%: LLM сильно расходится с XGBoost (model drift? prompt drift?)

### Когда вмешиваться

| Симптом | Возможная причина | Действие |
|---|---|---|
| Sharpe paper trades < +2 за 100 trades | Possibly distribution drift | Run walk_forward_b_filter.py на свежих trades, сравнить |
| `rejects.direction_filter` ≫ 30% | LLM prompt change или 70B model swap | Проверить enrichment quality на live news |
| `errors.enrichment_missing` > 1% | Race condition Predictor faster than Enricher | Increase EnrichmentCache TTL |
| Decision dies on `apply_direction_filter` exception | Schema mismatch — EnrichedNewsEvent изменился | Verify contracts version |

### Validation runs (повторяемые)

Для регулярной проверки (e.g. еженедельно):

```powershell
# 1. Re-run walk-forward с свежим snapshot
.\.venv\Scripts\python.exe scripts\walk_forward_b_filter.py `
    --label b_filter_weekly_$(Get-Date -Format yyyyMMdd)

# 2. Compare mean Sharpe vs baseline 6.42
# 3. Если deviation > 1.0 Sharpe — investigate
```

---

## Failure modes & mitigations

### Failure 1: LLM провайдер upgrades model
- Symptom: enrichment distribution changes (директивность, confidence распределение)
- Detection: `rejects.direction_filter` rate changes значительно
- Mitigation: re-run walk_forward_b_filter.py, если Sharpe просел — pin model version
  или switch provider

### Failure 2: Prompt drift in enricher
- Symptom: tickers[] empty rate растёт, neutral rate растёт
- Detection: `errors.empty_tickers` metric
- Mitigation: revert prompt to v1.0.0 если изменили

### Failure 3: XGBoost predictions degrade
- Symptom: `rejects.rr_below_threshold` rate > 95% (XGBoost не находит сетапов)
- Detection: signals_execute падает к нулю
- Mitigation: проверить feature_builder.py — может изменилась prices data structure

### Failure 4: Distribution shift проявится в нечто другое (новый бул-рынок)
- Symptom: Win rate < 50% на 100+ trades
- Detection: post-mortem analytics на trade:signals stream
- Mitigation: возможно нужно retraining XGBoost на свежих данных + B_filter

---

## Why we didn't need Sprint 5.6 work (retrospectively)

Sprint 5.6 (re-enrich + retrain XGBoost on 70B features) пытался **починить distribution**.
Это была неверная фокусировка — distribution shift невозможно полностью починить
через retraining, потому что:
1. 70B даёт фундаментально другую calibration (RLHF effect)
2. Никакой prompt не пробивает это (Sprint 5.7 sample tests подтвердили)
3. Phase 2 features 12 categories — частично результат price_moves leakage в legacy
   prompt (хотя финальный llm_prompt_v2.py "clean" — pearsonr 0.082)

**Правильный фикс** — change architecture:
- Использовать LLM в native режиме (direction filter)
- Использовать XGBoost на стабильных PRICE/TECHNICAL features
- Distribution shift в LLM features становится **non-issue** потому что мы не подаём
  их в XGBoost decision

В hindsight, Sprint 4.9 B_filter был оптимальным решением сразу. Sprint 5.6/5.7
$58 потратили на тщетные попытки fix XGBoost features. Sprint 5.7 walk-forward
B_filter validation = +$0 (использовали существующий 70B enrichment).

---

## Files map

### Production code (deployed Sprint 5)
- `src/services/decision/filter.py` — apply_direction_filter
- `src/services/decision/pipeline.py` — wires filter in
- `src/services/decision/config.py` — direction_filter_min_confidence
- `tests/services/decision/test_filter.py` — 9 unit tests
- `tests/services/decision/test_pipeline.py` — integration tests with filter

### Backtest validation (Sprint 5.7 этот раунд)
- `scripts/walk_forward_backtest.py` — XGBoost-based walk-forward (Sharpe 4.87 на legacy)
- `scripts/walk_forward_b_filter.py` — B_filter walk-forward (Sharpe 6.42)
- `data/reenrich_phase2/walk_forward/legacy_baseline/` — Phase 2 reproduction
- `data/reenrich_phase2/walk_forward/b_filter_70b_v1_0_0/` — B_filter validation

### Phase 2 reference data (newsbot2)
- `D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet`
  — 3,300 raw trades h=60 rr=2 mx_specific
- `D:\quik_sber\newsbot\newsbot2\news_bot_methodology\news_bot_methodology\code\llm_prompt_v2.py`
  — Phase 2 prompt that generated features_mfe.parquet (8B Ollama)

### Enrichment data
- `data/reenrich_phase2/full_70k_70b.parquet` — current 70B enrichment всех Phase 2 events
- `data/reenrich_phase2/checkpoints/checkpoint_llama_3_3_70b_versatile_v1_0_0.jsonl` — raw checkpoint

### Models
- `data/models/predictor/v1/*.joblib` — v1_legacy XGBoost (Phase 2 features compatible)
- `data/models/predictor/v1_70b_rolling/` — Sprint 5.6 retrain backup (deprecated)
- `data/models/predictor/v3/` — Sprint 5.6 expanding backup (deprecated)
- `data/models/predictor/v5/` — Sprint 5.6 no-LLM backup (deprecated)

---

## Glossary

- **Phase 2 baseline** — backtest result newsbot2 без realtime LLM: Sharpe 4.87
  walk-forward 13 фолдов
- **B_filter / B_direction_filter** — Sprint 4.9 strategy: LLM как post-filter
  на XGBoost trades (направление match + confidence threshold)
- **Distribution shift** — несоответствие feature distribution между train (Ollama 8B)
  и inference (DeepInfra 70B)
- **R:R logic** — Phase 2 decision: r_long = mfe_long/mae_long, side based on max
- **mx_specific** — отдельная XGBoost модель для ticker=MX (Phase 2 MX делал 50%
  trades, выделение даёт boost)
- **Walk-forward** — train на expanding window, test on next 3 months, slide forward
  3 months — 13 folds total на Phase 2 data 2022-2026
- **Anchor match** — связывание Phase 2 trade с конкретным news event через
  timestamp window [-60s, ts_open]
- **EnrichedNewsEvent.tickers** — per-ticker список с {ticker, direction,
  sentiment, confidence, impact_strength, rationale}
- **MLPredictionEvent** — Predictor output per (event_id, ticker): 8 numeric
  predictions + horizon
- **TradeSignalEvent** — Decision output: action=EXECUTE с levels или action=REJECT
  с reason
