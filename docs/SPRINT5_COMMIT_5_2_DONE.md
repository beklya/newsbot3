# Sprint 5 / Commit 5.2 — Decision service / DONE

**Закрыт:** 2026-05-26
**Длительность:** ~2ч (vs план 2.5d)
**Цель:** ml:predictions → trade:signals. LLM B-filter + Phase 2 R:R + RiskManager.

---

## Service: src/services/decision/

```
__init__.py
__main__.py            — entry point
config.py              — DecisionSettings + Phase 2 § 7.1 winning params
enrichment_cache.py    — Redis GET enriched:<id> (writer = Enricher 5.4)
filter.py              — DirectionFilter (port sprint4 trade_filter.py B-rule)
rr_logic.py            — evaluate_rr() + compute_levels() per Phase 2 § 2.3
sizing.py              — n_lots с floor=1 + leverage cap + expected_pnl
risk_manager.py        — Redis-backed read-only view (gates)
metrics.py             — counters
pipeline.py            — orchestrator
```

---

## Критические дизайн-решения

### 1. Один consumer, Redis cache (open Q2 resolved)
Decision подписан только на `ml:predictions`. На каждое событие — `redis.get(enriched:<enriched_event_id>)`. Cache miss → silent skip + counter `errors.enrichment_missing`. Это убирает dual-consumer-group complexity и thread-safe cache.

Enricher (5.4) добавит SETEX side effect параллельно publish — это integration dependency, документировано.

### 2. side ↔ direction mapping в filter.py
TradeSignal использует BUY/SELL, EnrichedNews — long/short. Маппинг `SIDE_TO_DIRECTION = {"BUY":"long","SELL":"short"}`. Filter работает в LLM-нотации (direction), TradeSignal output — execution-нотация (side).

### 3. Per-ticker direction match
`get_ticker_impact(event, ticker)` ищет TickerImpact для конкретного `prediction.ticker`. Sprint 4.1 нормализация уже привела ticker'ы в canonical (SI/MIX/YDEX/GLDRUB), exact match достаточно. Если ticker не упомянут → REJECT с "not in LLM tickers[]".

### 4. PnL ownership (open Q3 resolved)
Bridge (5.3) — единственный writer в `risk:daily_pnl:<date>` и `risk:open_positions`. Decision — read-only через RiskManager (`SCARD`, `EXISTS`, `GET`). Никаких race conditions между Decision и Bridge.

### 5. REJECT publishing
Все REJECT-причины публикуются в `trade:signals` с `action=REJECT`. Полей entry/sl/tp/quantity нет (контракт v1.0.1 allows None). Это критично для post-mortem analytics — Monitor (5.4) сможет считать REJECT rates по reason'ам.

### 6. size_lots floor ≥ 1
PHASE2 §5.3 paper-trade-off: на risk 0.5% + tight SL на дорогих фьючерсах вычисленный n_lots=0 → floor 1 с warning. Известный override риска — SL hit на 1 лоте может дать |PnL| > requested_risk_rub. Не блокирует soak (5.5).

### 7. Phase 2 R:R logic точно по PHASE2.md §2.3
- `rr_long = pred_mfe_long / max(pred_mae_long, MIN_MAE_PCT=0.05)`
- `rr_short = pred_mfe_short / max(pred_mae_short, 0.05)`
- Pick BUY if rr_long ≥ 2.0 AND mfe_long ≥ 0.15% AND rr_long ≥ rr_short
- Pick SELL if rr_short ≥ 2.0 AND mfe_short ≥ 0.15% AND rr_short > rr_long
- Else skip с reject_reason

### 8. compute_levels formula
- TP = entry × (1 ± TP_FRACTION × pred_mfe/100)
- SL = entry × (1 ∓ SL_BUFFER × pred_mae/100)
- Floors: sl ≥ 0.05%, tp ≥ 0.10% (PHASE2 §2.3)
- entry_price = last_close из MLPredictionPayload; Bridge скорректирует на fill

### 9. Whitelist — НЕ в Decision
Predictor (5.1) уже отфильтровал off-whitelist tickers. Decision доверяет: SBER/MX/Si etc. не доходят. План §5.2 говорит «whitelist owned by Predictor».

---

## Тесты (40 cases)

**`tests/services/decision/`:**
- `test_filter.py` (9): side↔direction mapping, match/mismatch, low conf, neutral, missing ticker
- `test_rr_logic.py` (9): pick long/short, RR threshold, MFE threshold, horizon switch, level math (BUY/SELL/floors)
- `test_sizing.py` (5): normal, leverage cap, floor=1, expected_pnl, zero SL fallback
- `test_risk_manager.py` (8): open_positions, cooldown, daily_pnl, daily_kill (positive + negative)
- `test_pipeline.py` (9): execute happy path, all 5 reject paths (direction, confidence, RR, max_open, cooldown, daily_kill), cache miss skip, idempotency dedup

**Total:** 40 passed in 0.26s ✓

**Полный suite:** 299 passed (259 baseline + 40 decision) ✓

---

## Files created

```
src/services/decision/
  __init__.py
  __main__.py
  config.py
  enrichment_cache.py
  filter.py
  rr_logic.py
  sizing.py
  risk_manager.py
  metrics.py
  pipeline.py

tests/services/decision/
  __init__.py
  conftest.py
  test_filter.py
  test_rr_logic.py
  test_sizing.py
  test_risk_manager.py
  test_pipeline.py

docs/SPRINT5_COMMIT_5_2_DONE.md
```

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| EnrichmentCache via Redis | реализовано | ✓ | ✓ |
| DirectionFilter port (B-rule) | реализовано + 9 tests | ✓ | ✓ |
| Phase 2 R:R logic (60m default) | реализовано + 9 tests | ✓ | ✓ |
| Sizing с floor=1 + leverage cap | реализовано + 5 tests | ✓ | ✓ |
| RiskManager (open/cooldown/kill) | реализовано + 8 tests | ✓ | ✓ |
| REJECT events публикуются | контракт v1.0.1 поддержка | ✓ | ✓ |
| Idempotency dedup | replay одного prediction → 1 signal | ✓ | ✓ |
| Pytest зелёный | 100% | 299/299 | ✓ |

**Sprint 5 / Commit 5.2 — closed ✅**

Готовы к 5.3 (Bridge service, Paper mode).
