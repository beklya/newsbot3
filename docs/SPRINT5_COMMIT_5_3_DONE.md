# Sprint 5 / Commit 5.3 — Bridge service (Paper mode) / DONE

**Закрыт:** 2026-05-26
**Длительность:** ~2ч (vs план 2d)
**Цель:** trade:signals → trade:executions с **realized PnL + exit fields** (контракт v1.0.1). Real QUIK Lua bridge deferred Sprint 6.

---

## Service: src/services/bridge/

```
__init__.py
__main__.py            — entry point + restart recovery hook
config.py              — BridgeSettings (paper/real toggle, costs, lot sizes)
paper_executor.py      — fill simulation + bar-by-bar SL/TP/time detection
position_tracker.py    — asyncio.Task per open position, PnL writeback
metrics.py             — counters + fill latency p50/p95
pipeline.py            — trade:signals → OPEN publish → spawn tracker
```

---

## Архитектурные решения

### 1. Два события на сделку (контракт v1.0.1)
- **OPEN**: `status=FILLED, filled_*` заполнены, `exit_*=None`. Publish сразу после fill simulation.
- **CLOSE**: тот же `signal_event_id`, новый `event_id`, `exit_*` заполнены, `realized_pnl_rub` финальный. Publish внутри PositionTracker._track_loop.
- Альтернатива «один event» нарушает 1:1 trade_signal→execution semantics — отвергнута.

### 2. PnL ownership (plan §5.3, open Q3)
Bridge — **единственный writer** в:
- `risk:open_positions` (SADD на open, SREM на close)
- `risk:daily_pnl:<YYYY-MM-DD>` (INCRBYFLOAT atomic, expire 36ч)
- `risk:cooldown:<ticker>` (SETEX cooldown_ticker_sec=60 после close)

Decision — read-only через RiskManager. Никаких race conditions.

### 3. PaperExecutor.open_position() — next-min bar
- `next_min = (signal.produced_at + 60s).floor("min")` в MSK
- `entry_bar = bars[next_min]` или fallback на last available close если timestamp в будущем (paper static snapshot)
- `filled_price = entry_bar.open × (1 + half_slippage × side_sign)` — half slippage на entry, half на exit (PHASE2 §2.3 round-trip 0.04% → 0.02% half для liquid акций)

### 4. check_exit() — Phase 2 bar-by-bar (SL-first)
Точная имитация `sprint4/exits/baseline.py` production mode:
- `window = bars[(index > entry_ts) & (index <= min(now, end_ts))]` — production-honest, без entry-bar lookback (исключает look-ahead bias на entry-минуте)
- SL-first проверка: для BUY `low ≤ sl → sl`; `high ≥ tp → tp`. Phase 2 convention.
- Time-stop: `now ≥ entry_ts + horizon_min → exit at last bar close`
- Exit price также применяет half slippage в направлении exit

### 5. asyncio.Task per position
- Каждая OpenPosition получает свою долгоживущую Task в PositionTracker
- Poll каждые `tracker_poll_interval_sec=5s` (тесты — 0.05s)
- Не блокирует consumer — параллельно может приходить много signals
- Persisted state в Redis (см. §6) → шаг shutdown не теряет позиции

### 6. Restart recovery
- На open: `SET bridge:open_positions:<signal_event_id>` JSON{ticker, side, entry, sl, tp, qty, entry_ts_iso, ...}
- На startup `__main__.py` вызывает `tracker.recover_from_redis()` — SCAN всех ключей, спавнит новый tracker per entry
- На close: `DEL bridge:open_positions:<signal_event_id>`
- Test: pre-seed key + recover + observe close — PASS

### 7. REJECT signals игнорируются
Bridge увеличивает counter `rejects_seen`, не публикует ничего. REJECT уже в trade:signals для Monitor analytics — не нужно дубля.

### 8. CandleCache моderн в src/infra/candles.py
Refactor: предиктор и бридж используют один `CandleCache` (был в `src/services/predictor/candle_cache.py`, теперь в `src/infra/candles.py`). Старый путь — thin shim re-export, тесты предиктора без правок.

### 9. Cost model
- **Slippage**: half-applied entry + half-applied exit (round-trip equiv to PHASE2 0.04% liquid acks)
- **Brokerage**: full round-trip applied в conv_rub (PHASE2 §2.3: equities 0.08%, futures 0.03%, USDRUB 0.40%)
- Эти cost'ы — paper-mode только. Sprint 6 QUIK заменит на реальный fill ack.

---

## Тесты (13 cases)

**`tests/services/bridge/`:**
- `test_paper_executor.py` (7):
  - open uses next-min bar + slippage BUY/SELL
  - check_exit TP buy / SL buy / time / alive
  - OpenPosition JSON round-trip (для Redis recovery)
- `test_pipeline.py` (6):
  - EXECUTE → OPEN + CLOSE published
  - REJECT → no publish, only counter
  - PnL writeback в `risk:daily_pnl:<today>` после close
  - Cooldown SETEX'нут после close
  - Idempotency — replay не открывает 2-ю позицию
  - **Restart recovery** — pre-seeded `bridge:open_positions:*` → tracker resume + close

**Total:** 13 passed in 0.53s ✓

**Полный suite:** 312 passed (299 baseline + 13 bridge) ✓

---

## Files created/modified

**New:**
```
src/services/bridge/{__init__.py, __main__.py, config.py,
                     paper_executor.py, position_tracker.py,
                     metrics.py, pipeline.py}
tests/services/bridge/{__init__.py, conftest.py,
                       test_paper_executor.py, test_pipeline.py}
src/infra/candles.py  (moved from src/services/predictor/candle_cache.py)
docs/SPRINT5_COMMIT_5_3_DONE.md
```

**Modified:**
- `src/services/predictor/candle_cache.py` — thin shim, re-exports from src/infra/candles
- `src/services/bridge/paper_executor.py` — replaced datetime.utcnow() deprecation

---

## Known limitations / deferred

1. **Static candle snapshot** — в paper mode на 24h soak текущее время выйдет за last bar в CSV. Fallback: `open_position` использует last_close, `check_exit` упирается в time-stop с last available bar. PnL близок к 0 на такие сделки. Sprint 6 — live QUIK candle stream.
2. **No real QUIK** — `paper_executor.py` сам моделирует. Sprint 6 заменит на QUIK Lua bridge с реальным fill ack.
3. **No Telegram alerts on bridge errors** — Sprint 6 backlog.
4. **Slippage hardcoded per ticker** — не зависит от volatility / volume. Phase 2 default 0.04% liquid / 0.10% illiquid. Sprint 6: dynamic slippage.

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| PaperExecutor: fill + bar-by-bar SL/TP/time | реализовано | ✓ | ✓ |
| PositionTracker asyncio.Task per position | реализовано | ✓ | ✓ |
| PnL writeback `risk:daily_pnl` atomic | INCRBYFLOAT | ✓ | ✓ |
| `risk:open_positions` SADD/SREM ownership | реализовано | ✓ | ✓ |
| Cooldown SETEX после close | реализовано | ✓ | ✓ |
| Restart recovery from Redis | реализовано + tested | ✓ | ✓ |
| OPEN + CLOSE events (v1.0.1) | реализовано | ✓ | ✓ |
| Pytest зелёный | 100% | 312/312 | ✓ |

**Sprint 5 / Commit 5.3 — closed ✅**

Готовы к 5.4 (Monitor + Enricher 70b switch).
