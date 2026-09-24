# Sprint 5 / Commit 5.1 — Predictor service + Fold 13 training / DONE

**Закрыт:** 2026-05-26
**Длительность:** ~3ч (vs план 3d — фактическое training было 14s вместо ожидаемых ~30 мин)
**Цель:** XGBoost MFE/MAE sidecar — real-time predictions per news:enriched event, publishes MLPredictionEvent.

---

## Pre-step: model training

`scripts/train_predictor_fold13.py` — урезанный backtest_mfe.py:
- Walk-forward как в Phase 2 (12mo train + 3mo test, step 3mo, purge 30min) → 13 фолдов
- Используем только Fold 13 (latest, test [2026-01-03 → 2026-04-03], train_n=67,455 MX_n=29,437)
- Тренируем 16 моделей: 2 horizons × 4 targets × 2 model_types
- XGBoost hyperparams строго по PHASE2.md §2.3 (`max_depth=4, n_estimators=150, lr=0.05, subsample=0.8, colsample_bytree=0.7`)
- Output: `data/models/predictor/v1/{target}_{horizon}_{model_type}.joblib` + `feature_order.json`

**Training run:** 14 секунд (бенчмарк намного лучше плана). Все 16 моделей сохранены, verify clean ✓.

**Sanity predictions** (на первых 5 test rows): MFE/MAE 0.1-0.4%, что согласуется с PHASE2.md §2.1 (median MFE_long_60m = 0.232%).

**Dependency fix:** добавил `scikit-learn>=1.5` в requirements.txt — XGBoost 3.x sklearn API требует его для XGBRegressor (поймал на первой попытке training).

---

## Service: src/services/predictor/

```
__init__.py
__main__.py          — entry, mirror enricher/__main__.py
config.py            — PredictorSettings (paths, whitelist, model_version, etc.)
candle_cache.py      — 19 CSVs из D:\quik_sber\newsbot\prices\ → RAM
news_history.py      — XRANGE bootstrap из news:enriched (24h), per-ticker deque
feature_builder.py   — port features_mfe.py extract_features() → 67-dim
model_loader.py      — joblib load 16 моделей + feature_order.json
inference.py         — vector → MLPredictionPerHorizon × 2 horizons
metrics.py           — counters + rolling latency p50/p95
pipeline.py          — orchestrator
```

---

## Критические дизайн-решения (зафиксированы в коде)

### 1. event_id — fresh ULID на каждый MLPredictionEvent
По плану 5.1 §1. Связь с upstream через `payload.enriched_event_id`. Это позволяет публиковать N MLPredictionEvent на одну news (по одному per ticker) без коллизий event_id в stream'е.

### 2. Idempotency composite key
`IdempotencyGuard.claim(scope="ml_prediction", key=f"{enriched_event_id}:{ticker}")`. Гарантирует что повторный consume одной news не блокирует обработку второго ticker'а (был бы баг если key = enriched_event_id).

### 3. Whitelist — single source of truth в Predictor
12 тикеров (YNDX, GAZP, NG, BR, PLZL, GMKN, TATN, MGNT, VTBR, NVTK, ROSN, LKOH). SBER/MX/Si/USDRUB/CNY/MTSS/GOLD silently dropped с инкрементом counter. Decision Service (5.2) не дублирует фильтр.

### 4. Schema adapters EnrichedNews → Phase 2 features
- **Sentiment**: `positive→bullish, negative→bearish, neutral→neutral`
- **Category**: 8 значений новой схемы vs 12 Phase 2 — 7 общих. "market" → все cat_* = 0 (документированный schema drift)
- **Text features**: считаются от `payload.summary` (300 char limit) вместо full_text. Sprint 6 backlog — lookup raw_event_id для полного текста
- **price_driven / causal**: отсутствуют в новой схеме → fixed defaults (0.0, is_actionable как proxy для causal)

### 5. news_history через XRANGE bootstrap
План Open Q1 resolved: in-memory `dict[ticker, deque]` (~50k events/day легко влезает в RAM). На startup делаем XRANGE с min=now-24h, на каждое новое event — append.

### 6. Timezone handling
Phase 2 свечи в naive MSK, EnrichedNewsEvent.produced_at в UTC. В feature_builder делаем explicit conversion: tz-aware UTC → +3h → tz-naive (matches Phase 2 candle index).

### 7. Routing general vs mx_specific
Per inference.py: `ticker == "MIX" → mx_specific, else → general`. Если mx_specific недоступен — fallback на general с warning.

### 8. DLQ flat dict (не MessageEnvelope)
Matches enricher pattern. `missing_market_data` — единственный известный non-retryable error.

### 9. Retryable errors via no-ack
Inference exception → raise PredictionRetryable → StreamConsumer no-acks → PEL replay. Соответствует enricher EnrichmentRetryable паттерну.

---

## Тесты (27 cases)

**`tests/services/predictor/`:**
- `test_feature_builder.py` (11 cases):
  - 67 features присутствуют
  - sentiment positive→bullish mapping
  - category one-hot (corporate, market edge case)
  - urgency one-hot
  - confidence passthrough (0..1 без деления)
  - missing candles → neutral defaults, нет NaN
  - news_history 24h window
  - text features из summary
  - vectorize ordering + missing keys
- `test_inference.py` (4 cases):
  - 2 horizons возвращаются
  - MFE/MAE clipped ≥ 0
  - rr_long = mfe/max(mae, 0.05)
  - routing MIX → mx_specific (verified by different output на одинаковом vector)
- `test_model_loader.py` (3 cases):
  - Real bundle load: 16 моделей + 67 features + fingerprint
  - Missing dir fail fast
  - Partial dir fail fast
- `test_news_history.py` (4 cases):
  - append separates tickers
  - 24h lookback window
  - bootstrap from fakeredis XRANGE
  - stats counter
- `test_pipeline.py` (5 cases):
  - whitelist ticker → publishes
  - off-whitelist → silent skip + counter
  - non-financial → skip + counter
  - multi-ticker (whitelist) → N events; missing candles → DLQ
  - composite idempotency: replay одной news → 1 event, не 2

**Total:** 27 passed in 1.92s ✓

**Полный suite:** 259 passed (232 baseline + 27 predictor) ✓

---

## Files changed

**New:**
- `scripts/train_predictor_fold13.py`
- `src/services/predictor/__init__.py`
- `src/services/predictor/__main__.py`
- `src/services/predictor/config.py`
- `src/services/predictor/candle_cache.py`
- `src/services/predictor/news_history.py`
- `src/services/predictor/feature_builder.py`
- `src/services/predictor/model_loader.py`
- `src/services/predictor/inference.py`
- `src/services/predictor/metrics.py`
- `src/services/predictor/pipeline.py`
- `tests/services/predictor/{__init__,conftest,test_*}.py` (5 файлов)
- `data/models/predictor/v1/{target}_{horizon}_{model_type}.joblib` × 16 + `feature_order.json` (gitignored)
- `data/train_predictor_fold13.log` (gitignored)

**Modified:**
- `requirements.txt` — добавил `scikit-learn>=1.5`

---

## Known limitations / deferred

1. **Text features из summary, не full_text** — known drift, low feature weight (5/67 = 7.4%). Sprint 6 backlog: lookup `raw:{raw_event_id}` для полного текста.
2. **Category one-hot drift** — "market" категория из новой схемы → все cat_* = 0. 5 Phase 2 категорий (sanctions/earnings/dividends/ma/regulation) никогда не активируются в новой enrichment.
3. **price_driven / causal** — нет в новой схеме. price_driven=0.0, causal=is_actionable.
4. **Candle cache static snapshot** — на startup load CSV, без live refresh. Sprint 6 — QUIK candle stream.
5. **No live training** — Fold 13 фиксированный. Monthly retrain — Sprint 6 backlog.
6. **No replay validation** — план 5.5 включит «replay 100 events → match Phase 2 backtest ±2%» как acceptance criterion. В 5.1 верифицировано только unit/integration через synthetic data.

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| 16 моделей обучены | 16 joblib + feature_order | 16 + json | ✓ |
| Service skeleton (10 модулей) | 10 файлов | 10 | ✓ |
| feature_builder schema adaptation | sentiment + category + text mapping | реализовано + tested | ✓ |
| Composite idempotency key | per (news, ticker) | реализовано + tested | ✓ |
| Fresh ULID event_id | unique per prediction | реализовано + tested | ✓ |
| news_history XRANGE bootstrap | per-ticker 24h | реализовано + tested | ✓ |
| Pytest зелёный | 100% pass | 259/259 | ✓ |
| Smoke imports работают | run() callable | ✓ | ✓ |

**Sprint 5 / Commit 5.1 — closed ✅**

Готовы к 5.2 (Decision service).
