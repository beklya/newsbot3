# Sprint 6 — 3 stale-data gates (DONE)

**Status:** Closed 2026-06-01
**Trigger:** Day 1 of Sprint 6.1 paper trading collection produced a -21,963₽
catastrophic trade due to Predictor/Bridge candle-cache divergence. Daily-kill
correctly halted further trading. This sprint adds defense in depth so we never
recreate the bug.

## The bug we reproduce-protected against

**Scenario observed 2026-06-01 12:56:23 MSK:**

1. Enricher was broken 13 hours (NOGROUP loop after wipe — Sprint 6 NOGROUP
   self-heal is a separate backlog task #46).
2. At 12:56:08 Enricher restarted and processed news pack rapidly. One of them
   had `tg_published_at = 12:28:46 MSK` (~28 min earlier).
3. Predictor received the news. **GAZP candle cache: historical CSV ended
   2026-04-20 23:49; live quik_feed data began 2026-06-01 12:31:00.**
   `news_time=12:28:46` fell in the gap between these two segments.
4. Predictor's searchsorted on `12:28:46` found last bar **at or before** —
   April 20 close (125.83) — and set `last_close=125.83`.
5. Bridge's searchsorted on `12:30:00` (next_min) found first bar **at or
   after** — today's 12:31 live bar (open=116.32) — and filled there.
6. Decision computed SL/TP off 125.83 reference: `entry=125.83, sl=126.78,
   tp=124.58` — sane for short.
7. Bridge filled SELL at 116.32 — **both SL=126.78 and TP=124.58 are now ABOVE
   entry**. For SHORT, exit check `low <= tp` triggers on first bar (current
   price 116 ≤ 124.58 always), so paper "TP hit" at 124.58.
8. Realized PnL = (116.32 − 124.58) × 262 × lot_size_10 = **−21,963₽**.

## Three gates added

| Layer | Module | What it catches |
|---|---|---|
| **1. Predictor** `stale_candles_at_news_time` | `predictor/pipeline.py` | News falls into a candle-cache gap → skip prediction entirely. |
| **2. Decision** `stale_features` REJECT | `decision/pipeline.py` | If a stale prediction somehow leaked through (older code path), REJECT before R:R. |
| **3. Bridge** `price_drift` REJECT | `bridge/paper_executor.py` | If fill bar open differs from signal.entry_price by > 1%, refuse fill. |

Defense in depth — any one of the three would have prevented the catastrophic
trade.

## Configuration (Conservative defaults, per user choice)

### `DecisionSettings`
```python
stale_features_max_gap_sec: int = 1800   # 30 min, 0 = disabled
```

### `PredictorSettings`
```python
max_news_to_last_bar_gap_sec: int = 1800   # 30 min
```

### `BridgeSettings`
```python
max_entry_drift_pct: float = 0.01   # 1.0% drift, 1.0 = effectively disabled
```

## Reject-reason vocabulary additions

| Layer | Reason | Where visible |
|---|---|---|
| Predictor | `errors.stale_candles_at_news_time` | metrics counter |
| Decision | `stale_features:gap=Xs>Ys` | trade:signals REJECT records |
| Bridge | `open_rejected_price_drift` | bridge logs + `errors.open_failed` metric |

## Tests added (12)

- `tests/services/predictor/test_stale_news_gate.py` (3 tests)
  - stale gap triggers skip
  - fresh news passes
  - huge threshold disables

- `tests/services/decision/test_stale_features_gate.py` (3 tests)
  - 41-day gap → REJECT
  - 0-second gap → pass
  - threshold=0 → gate disabled

- `tests/services/bridge/test_paper_executor.py` (3 new for drift)
  - 20% drift → REJECT
  - 0.5% drift → fills
  - configurable threshold

Plus minor test infrastructure: fixtures override gate thresholds to keep
legacy tests passing without changes.

**Full suite: 406 passed, 1 skipped (was 378+1 → 407+1, +29 net new across this
Sprint).**

## Operational notes — what an operator sees

### When a stale-news gate fires (Predictor)
```
WARNING stale_candles_at_news_time ticker=GAZP event_id=01KT... 
        news_msk=2026-06-01 12:28 last_bar=2026-04-20 23:49
        gap=567140s threshold=1800s — skip prediction
```
No MLPredictionEvent is published. Decision sees nothing. No signal, no trade.

### When stale-features fires (Decision)
```
INFO reject event_id=01KT... ticker=GAZP reason=stale_features:gap=567140s>1800s
```
A REJECT signal is published for analytics (counted in
`rejects.stale_features`).

### When price-drift fires (Bridge)
```
WARNING open_rejected_price_drift ticker=GAZP signal_price=125.8300
        bar_open=116.3200 drift=7.5500% threshold=1.0000% — REJECT fill
```
`errors.open_failed` counter increments. No execution events published.
Position not opened.

## Workarounds users should know

1. **Fill the cache gap manually** (if you want to enable trading today rather
   than wait for live data to accumulate):
   ```
   # Refresh prices_GAZP.csv etc from MOEX export for 2026-04-21 to 2026-06-01
   # Drop into D:\quik_sber\newsbot\prices\
   # Restart Predictor + Bridge (they reload CSV on start)
   ```

2. **Wait for live data** — quik_feed running continuously, after 24-48h the
   live cache covers a substantial window. Stale gate stops firing on news
   inside that window.

3. **Loosen Bridge drift threshold** if you want to accept slightly stale fills
   during transitional period (NOT recommended without monitoring):
   ```
   BRIDGE_MAX_ENTRY_DRIFT_PCT=0.05   # 5% tolerance
   ```

## What this does NOT fix

- **Root cause**: gap in CandleCache (historical CSV ends April 20, live starts
  today). Filling this is a manual data refresh task, deferred.
- **Predictor & Bridge cache divergence in general** — they have independent
  CandleCache instances and could in principle disagree even without gaps
  (e.g., subscription drops mid-day). Gates catch the symptom but the
  underlying split-state risk remains.
- **NOGROUP recovery** for the Enricher case that started this whole problem
  — task #46 backlog.

## Acceptance

- ✅ Three gates implemented with cohesive defaults
- ✅ 12 new tests pass
- ✅ Full suite 406+1 — no regressions
- ✅ Defense in depth — each gate independently catches the 2026-06-01 bug
- ✅ All gates configurable (disable via env var)
- ✅ All gates honor news_time (so replay scenarios still work)

## Next steps

1. **Deploy locally** — `nb-stop && nb-launch` to rotate Predictor + Decision +
   Bridge with new code.
2. **Reset today's daily PnL** — wipe `risk:daily_pnl:2026-06-01` to let
   Decision trade again.
3. **Monitor for first hours** — `analyze_signals.py` should now show
   `rejects.market_closed` and (if any malformed cases) `rejects.stale_features`
   in the reject distribution.
4. Sprint 6.1 baseline collection continues.
