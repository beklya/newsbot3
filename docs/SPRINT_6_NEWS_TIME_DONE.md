> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 6 — news_time pass-through through pipeline (DONE)

**Status:** Closed 2026-05-31
**Goal:** Enable honest historical replay through live pipeline by propagating
original news timestamp end-to-end, so Predictor builds features on candles AT
news_time and Bridge fills paper trades at THAT moment's price.

---

## Why this was needed

Sprint 5 pipeline always used `event.produced_at` (= "now" when the event was
created by the service) as reference time for:
- Predictor: feature window lookup (candles around news_time)
- Predictor: `last_close` (reference price for SL/TP computation)
- Bridge: paper fill bar lookup

This worked for live news (where produced_at ≈ news_time) but broke for any
historical replay through the pipeline: replayed 2024 news would build features
on 2026 candles and fill on 2026 prices, making the paper PnL meaningless.

`scripts/walk_forward_b_filter.py` does honest backtest offline, but doesn't
exercise the live LLM enrichment + B_filter logic. To test "what if THIS
historical news came through THE CURRENT bot today" we need news_time
pass-through.

---

## What changed

### Contracts (backward-compatible — Optional + default None)

| Contract | New field |
|---|---|
| `EnrichedNewsPayload` | `tg_published_at: str \| None = None` |
| `MLPredictionPayload` | `news_time: str \| None = None` |
| `TradeSignalPayload` | `news_time: str \| None = None` |

**No SCHEMA_VERSION bump needed** — Optional with default None means old events
in streams still validate. Services that don't get news_time fall back to old
behavior (event.produced_at).

### Services

| File | Change |
|---|---|
| `enricher/llm_client.py` | `parsed["tg_published_at"] = raw_event.payload.tg_published_at` — copy from raw |
| `predictor/feature_builder.py` | `ref_ts_str = a.tg_published_at or event.produced_at` |
| `predictor/pipeline.py` | propagate `news_time` to MLPredictionPayload; **also**: `last_close` lookup at news_time bar (was iloc[-1]) |
| `decision/pipeline.py` | propagate `news_time` to TradeSignalPayload (both EXECUTE and REJECT branches) |
| `bridge/paper_executor.py` | `ref_ts_str = p.news_time or signal.produced_at` for fill bar lookup |

---

## Verification

3 paper trades with honest fill_time AFTER fix:

```
BR    BUY  14441 @ 62.94 → 62.76  PnL -2954  exit=SL    fill_time=2025-10-15 13:29
LKOH  BUY  227   @ 6361 → 6373    PnL +1572  exit=TP    fill_time=2025-08-12 13:35
GAZP  BUY  417   @ 120.15 → 120.29 PnL +149  exit=time  fill_time=2024-09-04 10:04
```

vs 14 legacy trades BEFORE fix all stuck at `fill_time=2026-04-20T21:49`
(latest available bar in cache, regardless of news_time).

Discriminating features after fix:
- **Different fill_time dates** matching news source dates ✅
- **Different exit reasons** (tp/sl/time, not only time) ✅
- **Meaningful PnL magnitudes** (-2954 to +1572 RUB) ✅

---

## Replay script

`scripts/replay_historical_news.py` already builds RawNewsEvent with correct
`tg_published_at` from jsonl `timestamp` (Unix epoch UTC). After this Sprint
fix, the rest of the pipeline correctly carries that through.

Usage:
```powershell
nb-script replay_historical_news.py --count 100 --from 2024-01-01 --to 2026-04-15 --no-confirm
```

(End date 2026-04-15 chosen because Phase 2 prices CSV ends ~2026-04-20.)

---

## Timezone sanity (the easy place to mess up)

| Stage | Format | Example |
|---|---|---|
| jsonl `timestamp` field | Unix epoch (UTC by Unix definition) | `1777570315.0` |
| `RawNewsPayload.tg_published_at` | UTC ISO 8601 | `2026-04-30T17:31:55.000+00:00` |
| Phase 2 prices CSV index | naive MSK | `2026-04-30 20:31:00` |
| Predictor / Bridge conversion | UTC → +3h → strip tz | `2026-04-30 20:31:55` naive MSK |

Verified by sanity test:
```
epoch 1777570315 → ISO 2026-04-30T17:31:55+00:00 → MSK 2026-04-30 20:31:55
        (UTC)                                       (matches jsonl "datetime" field)
```
Diff = 0s ✅.

---

## What this enables

1. **Honest historical replay**: 100-1000 news from 2022-2026 via
   `replay_historical_news.py` → real Sharpe estimate before waiting weeks for
   live trades.

2. **Backtest-vs-live consistency**: same code path for both, fewer divergence
   bugs between offline backtest and production behavior.

3. **Forensic analysis**: when a live trade goes wrong, you can re-run that
   exact news event later and reproduce the prediction/decision deterministically
   (LLM aside).

---

## Tests

All 377 (+1 skipped) pre-existing tests still pass after the contract +
service changes. Backward compat preserved because all new fields are
Optional with default None.

No new tests added in this iteration — the Optional fields are exercised via
the replay flow end-to-end. Sprint 6.x backlog: add explicit unit tests for
news_time propagation (3-4 tests covering Enricher copy, Predictor read,
Decision propagate, Bridge use).

---

## Next: Sprint 6.1

After full Redis wipe (2026-05-31 23:00), pipeline is in clean state.
Monday 2026-06-01 at 10:00 MSK: QUIK Workstation start → candle_dump.lua →
quik_feed → live candles in Redis → real paper trading.

**Acceptance gate Sprint 6.1**: 100+ paper trades collected with honest
fill_time (live candles, not historical). Target ~3-4 weeks at ~5
trades/day rate.

After 6.1: compare realized Sharpe vs `walk_forward_b_filter.py` prediction
(6.42 walk-forward, 3.14 V1 holdout). If within ±30% — pipeline validated.
If gap > 30% — diagnose (look at per-ticker breakdown, exit reason
distribution, slippage realism).
