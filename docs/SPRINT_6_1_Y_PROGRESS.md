# Sprint 6.1 — Y-pivot progress log

**Дата**: 2026-06-06
**Контекст**: After A2/A3/B1 confirmed structural distribution shift, switched to
Y-stack (Y7 scrape → Y5 gap re-enrich → Y4 70B-only features → Y6 corpus extend).

---

## Done

### Y7 — telegram_news scrape gap
- Receiver session at `data/sessions/receiver.session` is valid; legacy `tg_session`
  and `duble2/tg_session` are expired.
- `scripts/scrape_gap_via_receiver_session.py` — uses receiver session to avoid
  re-auth, writes to root `D:\quik_sber\newsbot\telegram_news.jsonl`.
- Result: **+11,054 new records** appended over interfax/rian/tass/rbc, covering
  2026-04-30 → 2026-06-06.  Total corpus now 871,680 (~885MB).
- Channel distribution in gap window:
  | Channel | n | % |
  |---|---:|---:|
  | tass_agency | 1567 + similar | 49.4% |
  | rbc_news | 19% |
  | interfaxonline | 15.7% |
  | rian_ru | 15.6% |
- **CONFIRMED**: Phase 2's reported "rian 73% + interfax 27% only" — Phase 2 dataset
  ACTIVELY drops tass + rbc.  Live distribution is tass-dominated.  Major structural
  gap in v1_legacy training.

### Y5 — DI 70B re-enrich gap (in progress)
- Input: `data/reenrich_phase2/gap_2026_04_21_to_today_input.parquet`
  (14,148 events, 2026-04-21 → 2026-06-07, all 4 channels)
- Runner: `scripts/deepinfra_runner.py --concurrency 50`
- Cost: ~$0.30/M in × 4500 tokens × 14148 = **~$14.07** estimated
- Speed: 2.3 ev/s steady, ETA ~95 min from launch
- Output appends to existing
  `data/reenrich_phase2/checkpoints/checkpoint_llama_3_3_70b_versatile_v1_0_0.jsonl`
  (was 70,157, will become ~84,300).
- Purpose: provides offline fresh enrichment of recent events.  DOES NOT extend
  XGBoost training corpus (no Phase 2 trade rows for these events).

### Y4 — 70B-only features added + v6_70b_ext retrain
- Added 10 new features to `feature_builder.py`:
  - `is_actionable_int`, `is_financial_int` (event-level booleans)
  - `tf_instant`, `tf_fast`, `tf_medium`, `tf_slow` (expected_timeframe one-hot)
  - `impact_strength` (per-ticker)
  - `dir_long`, `dir_short`, `dir_neutral` (per-ticker direction one-hot)
  - `sell_the_news` (sentiment ≠ direction polarity)
- `MLPredictionPayload.feature_count` relaxed `ge=67, le=80`
- `scripts/rebuild_features_with_70b_ext.py` rebuilds features_mfe_70b_ext.parquet
  (Phase 2 + 10 new Y4 cols) — produces 70,184 × 81 cols
- `scripts/train_predictor_fold13.py --features features_mfe_70b_ext.parquet
  --out data/models/predictor/v6_70b_ext` — auto-detected 78 features, trained 16/16
  models in 39 sec, verify clean

### Y4 replay on VPS 1766 events

| Metric | v1_legacy baseline | **v6_70b_ext** | Δ |
|---|---:|---:|---|
| EXECUTE | 43 | **10** | −33 |
| Total PnL | −43 394 ₽ | **−13 833 ₽** | **+29 561** ✅ |
| Win rate | 25.6% | 20.0% | slightly worse |
| **Sharpe** | **−16.80** | **−10.77** | **+6.03 improvement** |
| Per-trade | −1009 ₽ | −1383 ₽ | worse |
| Exit reasons | TP:7 SL:29 Time:7 | TP:2 SL:8 Time:0 | — |

### Y4 per-ticker breakdown — what v6 keeps vs rejects

v6 rejected 33 of 43 v1 trades through stricter R:R (380 → 554 rr_rejects).
Of 10 trades v6 kept:

| Ticker | n | PnL | Wins | Comment |
|---|---:|---:|---:|---|
| GAZP | 5 | −12,504 | 0 | All SL — anti-selection unchanged |
| ROSN | 1 | −2,500 | 0 | SL |
| VTBR | 3 | −1,972 | 1 | Mixed (1 TP, 2 SL) |
| NG | 1 | +3,143 | 1 | TP — only profitable trade |

8 of 10 v6 trades overlap with v1.  Y4 made model SELECTIVE but didn't change
the bad-pick patterns.

---

## Decision matrix at this point

| Question | Answer |
|---|---|
| Did Y4 (70B features) improve Sharpe? | Yes — measurable +6.03 improvement |
| Is Y4 alone enough for tradeable Sharpe? | No — still −10.77 |
| Is GAZP still destroying PnL? | Yes — all 5 GAZP trades = SL |
| Does v6 generalize to live distribution? | Partially — too few trades to be sure |
| Is Y6 (corpus extend) necessary? | Likely yes — train corpus missing tass+rbc + 2026 regime |

---

## Pending

### Y5 completion — wait ~85 min, then aggregate
After Y5 done:
1. `python sprint4/reenrich/aggregate_checkpoint.py --model llama-3.3-70b-versatile`
   → produces fresh full_70k+gap_70b.parquet
2. Optionally overlay fresh enrichment into VPS replay JSONL — test if cached
   enrichment differs materially from fresh.  Probably small effect (same model
   same prompt) but quantifies LLM non-determinism.

### Y6 — full corpus 2025 → today re-enrich + re-derive
- Input: `data/reenrich_phase2/y6_2025_to_today_input.parquet`
  (194,216 events, 4 channels, 17-month window)
- Cost: **~$207** DI 70B
- Wall time: ~22 hours
- After enrichment: re-derive Phase 2 trade rows using newsbot2 `target_mfe.py`
  (requires stitched futures prices for 2025-2026 — Y6.5 task or stocks-only initial)
- Extended training corpus: Phase 2 (70k) + 2025-2026 re-enriched (~120k stocks-only?)
- Retrain v7 with extended corpus

### Y6.5 — stitched futures prices 2025-2026 (sub-task of Y6)
- Need BRH5/K5/M5/N5/Q5/U5/V5/X5/Z5/F6/G6/H6/J6/K6/M6/N6 (16 monthly Brent)
- Need NG similarly (~16 monthly)
- Need Si quarterly (5 contracts: M5/U5/Z5/H6/M6)
- Need MIX quarterly (5: same)
- Stitched continuous series with roll-adjustment

---

## Files snapshot

| Path | Purpose |
|---|---|
| `scripts/scrape_gap_via_receiver_session.py` | Y7 scrape (receiver session) |
| `scripts/extract_gap_2026_for_reenrich.py` | Y5/Y6 input extract from telegram_news.jsonl |
| `scripts/deepinfra_runner.py` | DI 70B enrichment runner (existing) |
| `scripts/rebuild_features_with_70b_ext.py` | Y4 — 81-col features parquet |
| `data/reenrich_phase2/features_mfe_70b_ext.parquet` | 70,184 × 81 |
| `data/models/predictor/v6_70b_ext/` | Y4 trained model |
| `data/reenrich_phase2/gap_2026_04_21_to_today_input.parquet` | Y5 input |
| `data/reenrich_phase2/y6_2025_to_today_input.parquet` | Y6 input |
| `data/replay/v6_70b_ext_{report,trades}.csv/.json` | Y4 replay output |
| `BACKUP_2026-06-06_pre_y_sweep/` (D:\quik_sber\newsbot\) | 1148MB snapshot |
