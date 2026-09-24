# Sprint 5 — Phase 3 Paper Trading End-to-End / DONE (code-level)

**Закрыт (code-level):** 2026-05-26
**Длительность:** ~1 день фактическое coding (vs план ~11 дней)
**Цель:** собрать end-to-end цепочку до paper-fills с risk 0.5%, валидировать integration. Real QUIK + paper PnL validation deferred Sprint 6.

---

## Резюме

Все 6 сервисов цепочки **`Telegram → Receiver → news:raw → Enricher (70b) → news:enriched → Predictor (XGBoost MFE) → ml:predictions → Decision (B-filter + R:R + RiskMgr) → trade:signals → Bridge (PaperExecutor) → trade:executions → Monitor`** реализованы и интеграционно проверены.

Sprint 4 winning config валиден в коде:
- LLM B_direction_filter (skip if direction != side OR conf < 0.5)
- Phase 2 R:R (TP=0.7×pred_MFE, SL=1.2×pred_MAE, RR≥2.0, MIN_MFE_PCT=0.15)
- 12-ticker whitelist (YNDX/GAZP/NG/BR/PLZL/GMKN/TATN/MGNT/VTBR/NVTK/ROSN/LKOH)
- SBER excluded (через whitelist в Predictor, single source of truth)

Что **не сделано** в Sprint 5 (intentional):
- Real QUIK Lua bridge (Sprint 6)
- Реальный 24h soak run (operator pre-condition для Sprint 6)
- Paper PnL validation milestone — нужны 100+ trades, ≈2-3 недели paper soak (Sprint 6)

---

## Status snapshot

| Commit | Status | Key output |
|--------|--------|-----------|
| **5.0** Housekeeping + contracts v1.0.1 + heartbeat refactor | ✓ closed | 232 tests, cleanup мусора |
| **5.1** Predictor + Fold 13 training | ✓ closed | 16 models, 27 tests, 67 features adapted |
| **5.2** Decision service | ✓ closed | B-filter + Risk + R:R, 40 tests |
| **5.3** Bridge (Paper mode) | ✓ closed | PaperExecutor + PositionTracker, 13 tests |
| **5.4** Monitor + Enricher 70b switch | ✓ closed | 21 new tests, 70b default, SETEX cache, 8b fallback |
| **5.5** End-to-end integration | ✓ code-level closed | 2 integration tests, launcher, .env pre-flight |

**Total:** 335 passed (217 baseline + 118 new для Sprint 5) ✓

---

## Архитектура (после Sprint 5)

```
Telegram (Telethon)
   │
   ▼
news:raw ── RawNewsEvent v1.0.0
   │
   │  Enricher (70b primary + 8b fallback на 403, prompt v1.0.0)
   │  + side effect: SETEX enriched:<event_id> JSON TTL=300s
   ▼
news:enriched ── EnrichedNewsEvent v1.1.0
   │
   ├──► Predictor (XGBoost Fold 13, 16 models, 67 features)
   │       Per-ticker iteration, composite idempotency key
   │       Whitelist (12) — single source of truth
   │       ▼
   │    ml:predictions ── MLPredictionEvent v1.0.0 (fresh ULID)
   │       │
   │       ▼
   │    Decision (single consumer)
   │       • GET enriched:<id> cache (miss → skip)
   │       • DirectionFilter per-ticker (Sprint 4.10 B-rule)
   │       • Phase 2 R:R + sizing + RiskManager gates
   │       • REJECT events публикуются для analytics
   │       ▼
   │    trade:signals ── TradeSignalEvent v1.0.1 (REJECT-friendly)
   │       │
   │       ▼
   │    Bridge (PaperExecutor + PositionTracker)
   │       • Fill at next-min bar open + slippage
   │       • PositionTracker asyncio.Task per signal
   │       • bar-by-bar SL/TP/time-stop=60min
   │       • OPEN event на fill, CLOSE event на exit
   │       • PnL writeback INCRBYFLOAT risk:daily_pnl
   │       • Cooldown SETEX ticker
   │       ▼
   │    trade:executions ── ExecutionResultEvent v1.0.1 (exit fields)
   │
system:heartbeats ◄── все 6 сервисов (30s interval)
   │
   ▼
Monitor (poll 30s): aggregator + 3 alert rules + dedupe + log
```

---

## Ключевые архитектурные решения

| Решение | Обоснование |
|---------|-------------|
| **Single Predictor whitelist** | План §5.1: Decision не дублирует filter — single source of truth. SBER/MX/Si/USDRUB/CNY/MTSS/GOLD silently dropped в Predictor |
| **Composite idempotency (predictor)** | `{enriched_event_id}:{ticker}` — иначе 2-й ticker дропнется при N-ticker news |
| **Fresh ULID для MLPredictionEvent** | Уникальность event_id в stream. Связь через `payload.enriched_event_id` backref |
| **Redis SETEX cache (5.4 → 5.2)** | Решает dual-consumer-group complexity. Enricher pisht, Decision читает. TTL 300s = окно для predictor+decision pipeline |
| **PnL ownership: Bridge writes, Decision reads** | Устраняет race condition. Atomic INCRBYFLOAT для daily_pnl |
| **OPEN + CLOSE events (1:N сделки)** | Сохраняет 1:1 trade_signal→execution. Контракт v1.0.1 поддерживает exit_*=None для OPEN |
| **REJECT events публикуются** | Post-mortem analytics критичен. Контракт v1.0.1 делает execute-поля Optional |
| **PaperExecutor production-honest window** | `index > entry_ts` (без entry-bar look-ahead). Соответствует sprint4 BaselineFixedTpSl WINDOW_PROD |
| **asyncio.Task per position** | Не блокирует consumer, конкурентные positions tracked независимо |
| **Restart recovery в Redis** | `bridge:open_positions:<id>` JSON → tracker resume на startup |
| **HeartbeatPublisher в src/infra/** | DRY: 6 сервисов используют один модуль |
| **CandleCache в src/infra/** | Shared между Predictor (features) и Bridge (fills/tracking) |

---

## Что построено в каждом коммите (детали в COMMIT_5_*.md)

### 5.0 Housekeeping (1ч)
- Удалены 6 placeholder dirs (`src/decision/`, etc.) + 4 archive файла
- TradeSignal v1.0.0 → v1.0.1: optional execute-поля + EXECUTE model_validator
- ExecutionResult v1.0.0 → v1.0.1: exit fields (realized_pnl, exit_reason, exit_price, exit_time, duration_sec)
- HeartbeatPublisher → `src/infra/heartbeat.py` (был 2 копии)
- 15 новых contract tests, 5 golden samples regenerated

### 5.1 Predictor + training (3ч)
- `scripts/train_predictor_fold13.py` — 16 моделей за 14s (vs план ~30 мин)
- 10 service модулей: candle_cache, news_history, feature_builder, model_loader, inference, metrics, pipeline, main
- Phase 2 → новая схема adapter: sentiment positive→bullish, category 8→12 с защитой, sentiment 0→1, text features от summary (proxy)
- 27 tests, fingerprint = 2c52ebe8590b3fe5

### 5.2 Decision (2ч)
- 8 service модулей: enrichment_cache, filter, rr_logic, sizing, risk_manager, metrics, pipeline, main
- B_direction_filter portирован из sprint4/exits/hybrid/trade_filter.py
- Phase 2 §7.1 winning params как DecisionSettings defaults
- side ↔ direction mapping {BUY↔long, SELL↔short}
- size_lots floor ≥ 1 + leverage cap 10×
- 40 tests включая все 5 reject paths (direction, confidence, RR, max_open, cooldown, daily_kill)

### 5.3 Bridge Paper (2ч)
- 7 модулей: paper_executor, position_tracker (asyncio.Task per signal), pnl writeback, pipeline, main
- Reused sprint4/exits/baseline.py production-honest window logic
- Slippage half на entry + half на exit (PHASE2 §2.3 round-trip)
- Brokerage cost на close
- Restart recovery через `bridge:open_positions:*` keys
- 13 tests

### 5.4 Monitor + Enricher upgrade (1.5ч)
- Enricher: default model 8b→70b, добавлен groq_fallback_model, 403 fallback retry (один раз)
- Enricher pipeline: SETEX `enriched:<id>` side effect для Decision cache
- Monitor: 7 модулей (aggregator, alerts, pipeline, etc.)
- 3 alert rules: missing_heartbeat (warn/crit по gap), dlq_rate_spike, daily_pnl_kill
- Alert dedup (`_ALERT_SUPPRESS_TICKS=10`)
- 21 new tests (3 enricher upgrades + 18 monitor)
- **Не реализовано:** proactive TPM/TPD tracking — backlog Sprint 6 (reactive cooldown остаётся)

### 5.5 Integration (0.5ч code-level)
- `tests/integration/test_full_pipeline.py` — end-to-end through 6 services
- `.env` updated: GROQ_MODEL=70b, RISK_PER_TRADE_PCT=0.005
- `scripts/launch_paper_soak.ps1` для Windows
- 2 integration tests
- **24h soak run** — operator pre-condition для Sprint 6

---

## Files inventory

### Production code (src/)
```
src/contracts/
  trade_signal.py        v1.0.1 (Optional + EXECUTE validator)
  execution_result.py    v1.0.1 (exit fields)
  enriched_news.py, ml_prediction.py, raw_news.py, instruments.py, base.py  (unchanged)

src/infra/
  candles.py             ← new (moved from predictor)
  heartbeat.py           ← new (DRY from receiver+enricher)
  consumer.py, publisher.py, idempotency.py  (unchanged)

src/services/
  receiver/, enricher/   ← Sprint 2-3 + Sprint 5.4 enricher upgrades
  predictor/             ← new 10 модулей (5.1)
  decision/              ← new 9 модулей (5.2)
  bridge/                ← new 7 модулей (5.3)
  monitor/               ← new 7 модулей (5.4)
```

### Scripts
```
scripts/
  train_predictor_fold13.py     (Sprint 5.1)
  launch_paper_soak.ps1         (Sprint 5.5)
  redis_inspect.py, analyze_soak.py, watch_enriched.py  (existing observability)
```

### Tests
```
tests/contracts/test_{trade_signal,execution_result}_v1_0_1.py  (15 cases, Sprint 5.0)
tests/services/predictor/                                       (27 cases, Sprint 5.1)
tests/services/decision/                                        (40 cases, Sprint 5.2)
tests/services/bridge/                                          (13 cases, Sprint 5.3)
tests/services/monitor/                                         (18 cases, Sprint 5.4)
tests/services/enricher/test_5_4_upgrades.py                    (3 cases, Sprint 5.4)
tests/integration/test_full_pipeline.py                         (2 cases, Sprint 5.5)
```

### Data (gitignored)
```
data/models/predictor/v1/
  feature_order.json
  {mfe_long,mae_long,mfe_short,mae_short}_{30m,60m}_{general,mx_specific}.joblib  × 16

data/train_predictor_fold13.log
```

### Documentation
```
docs/SPRINT5_COMMIT_5_{0,1,2,3,4,5}_DONE.md  ← per-commit details
docs/SPRINT5_DONE.md                          ← this file
```

---

## Что НЕ работает / known limitations

1. **Schema drift в Phase 2 features**:
   - Category one-hot: новая схема 8 значений vs Phase 2 12 → "market" + 5 phase2-only категорий не активируются
   - Text features из summary (300 char) vs full_text Phase 2 (~252 median) → distribution shift, low feature weight (5/67 = 7.4%)
   - price_driven константа 0.0 (нет в новой схеме), causal=is_actionable как proxy
   - Sprint 6 fix: lookup raw text через `raw:{raw_event_id}` cache (нужен Receiver side effect)

2. **Candle cache static snapshot**:
   - В paper mode 24h soak: текущее время превысит last bar в CSV. PaperExecutor fallback на last_close. PnL близок к 0 на такие сделки.
   - Sprint 6: live QUIK candle stream.

3. **Proactive TPM/TPD не реализован**:
   - GroqKeyPool reactive cooldown на 429 (validated Sprint 3+4). Proactive TPM/TPD window — backlog Sprint 6.
   - Monitor зафиксирует rate-limit counter spikes через heartbeat snapshot.

4. **Без real QUIK Lua bridge**:
   - PaperExecutor сам моделирует fills. Live trading — Sprint 6.

5. **Slippage hardcoded**:
   - Не зависит от volatility/volume. Sprint 6: dynamic slippage от ATR / spread.

6. **Sprint 5.5 integration test использует mocked LLM**:
   - Real Groq calls в smoke flow не верифицированы. Soak run покажет.

---

## Backlog для Sprint 6

1. **24h paper soak run** (operator) + acceptance criteria fill
2. **Validation milestone**: 100-200 paper trades за ~2-3 недели → Sharpe check vs Sprint 4 V1 holdout 2.71
3. **Real QUIK Lua bridge** (trans2quik, TICK_VALUE polling, contract codes rolling)
4. **Live candle stream** (QUIK API)
5. **Telegram alerts** (через Receiver session)
6. **Proactive TPM/TPD** tracking в Enricher
7. **Raw text lookup** для feature_builder (`raw:{raw_event_id}` SETEX в Receiver)
8. **Multi-fold ensemble** если degradation в soak
9. **Prompt v1.0.1** (курсы ЦБ EMPTY_FINANCIAL fix)
10. **TICK_VALUE periodic refresh через QUIK API**
11. **Side BUY/SELL ↔ direction long/short унификация в контрактах** (сейчас bridge в filter.py)

---

## Verification commands

```powershell
cd D:\quik_sber\newsbot\newsbot3
.venv\Scripts\activate.bat
$env:PYTHONIOENCODING="utf-8"

# 1. Full pytest suite
pytest -q
# Expected: 335 passed

# 2. Train predictor (one-time, ~14 секунд)
python scripts/train_predictor_fold13.py

# 3. Smoke-load всех 6 сервисов
python -c "from src.services.receiver.config import load_settings as r; from src.services.enricher.config import load_settings as e; from src.services.predictor.config import load_settings as p; from src.services.decision.config import load_settings as d; from src.services.bridge.config import load_settings as b; from src.services.monitor.config import load_settings as m; r(); e(); p(); d(); b(); m(); print('ok')"

# 4. Launch 24h soak (operator hands-off)
.\scripts\launch_paper_soak.ps1

# 5. EOD observability
python scripts\redis_inspect.py summary
python scripts\redis_inspect.py len trade:executions
python scripts\analyze_soak.py --hours 24
```

---

## DoD (code-level)

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| Все 6 сервисов реализованы | receiver, enricher, predictor, decision, bridge, monitor | ✓ | ✓ |
| Sprint 4.10 winning config в коде | B-filter + 70b + Phase 2 R:R | ✓ | ✓ |
| Contracts v1.0.1 (REJECT + exit) | TradeSignal + ExecutionResult | ✓ | ✓ |
| Composite idempotency Predictor | per (news, ticker) | ✓ | ✓ |
| Bridge OPEN+CLOSE events | контракт + tests | ✓ | ✓ |
| RiskManager (open/cooldown/kill) | Redis-backed | ✓ | ✓ |
| Restart recovery Bridge | persisted state | ✓ | ✓ |
| Monitor 3 alert rules | реализовано + tested | ✓ | ✓ |
| End-to-end integration test | mocked LLM + real models | ✓ | ✓ |
| .env pre-flight для soak | GROQ_MODEL=70b + RISK=0.005 | ✓ | ✓ |
| Launcher script | Windows PowerShell | ✓ | ✓ |
| Pytest suite | 100% | 335/335 | ✓ |
| Soak run (24h) | hands-off operator | — | ⚠ Sprint 6 |
| Paper PnL validation | 100+ trades | — | ⚠ Sprint 6 |

**Sprint 5 — code-level closed ✅**

Готовы к operator soak run → Sprint 6 (real QUIK + validation milestone).
