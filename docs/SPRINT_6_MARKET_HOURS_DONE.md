# Sprint 6 — market-hours gate + stale-bar gate (DONE)

**Status:** Closed 2026-06-01
**Trigger:** Day 1 of Sprint 6.1 paper trading collection, observed 2 paper
trades fill on cached prices from 2026-04-20 (42-day stale) because QUIK
Workstation wasn't yet running. Bridge silently fell back to last cached
close. Result: 2 garbage trades polluting the Sprint 6.1 baseline before
the milestone could even start.

**Goal:** Two independent filters that together prevent meaningless paper
trades:

1. **Market-hours gate (Decision)** — refuse signals fired outside MOEX
   trading hours.
2. **Stale-bar gate (Bridge)** — refuse fills when the candle cache is
   too old relative to the signal's reference time.

---

## What changed

### New module

**`src/services/decision/market_hours.py`** (~110 lines)

```python
def is_market_open(now_utc, asset_class, *, skip_weekends=True) -> (bool, reason)
def is_market_open_for_ticker(now_utc, ticker, *, skip_weekends=True) -> (bool, reason)
```

Hours:
| Asset class | Open | Close |
|---|---|---|
| equity | 06:50 MSK | 23:49:59 MSK |
| futures / commodity / currency | 08:50 MSK | 23:49:59 MSK |

Skip weekends = True by default (Saturday, Sunday). MOEX has weekend
sessions (Доп.сессия выходного дня) ~3-4 days/year on production-calendar
working weekends, but Phase 2 backtest didn't cover them → distribution
shift → off by default.

Technical futures clearing break (14:00-14:05) ignored — 5 minutes of
noise not worth code complexity.

### Config additions

**DecisionSettings:**
```python
market_hours_enabled: bool = True             # env: MARKET_HOURS_ENABLED
market_hours_skip_weekends: bool = True       # env: MARKET_HOURS_SKIP_WEEKENDS
```

**BridgeSettings:**
```python
stale_bar_threshold_sec: int = 600            # env: STALE_BAR_THRESHOLD_SEC
                                              # min 60s, default 10 min
```

### Pipeline wiring

**`decision/pipeline.py`** — gate inserted after enrichment cache lookup
(#2.5), before R:R (#3). Cheap reject — saves CPU on closed-market signals.

Uses `news_time or produced_at` as reference time (NOT wall clock). This
ensures **honest historical replay still works**: a 2024-09-04 news event
checks "was the market open on Sept 4 2024?", not "is the market open
NOW?".

Reject reason format: `market_closed:<sub_reason>` where sub_reason is
`weekend` | `before_open` | `after_close`.

REJECT signals are published to `trade:signals` (consistent with other
gates) for post-mortem analytics.

**`bridge/paper_executor.py`** — stale-bar check inserted only in the
"no future bar" fallback path (when `searchsorted` returns `len(bars)`).

```python
gap_sec = (ts_open_msk - bars.index[-1]).total_seconds()
if gap_sec > settings.stale_bar_threshold_sec:
    log.warning("open_rejected_stale_candles ...")
    return None
# else: fall back to last-bar close (within tolerance — normal jitter case)
```

The gate **does not** trigger when `searchsorted` finds a future bar
(replay scenario: all bars present, signal in past — fills on correct
historical bar). Only triggers when signal is genuinely ahead of all
available data.

---

## Tests added (20)

**`tests/services/decision/test_market_hours.py`** (17 tests):
- Stock weekday open/morning/main/evening sessions
- Stock weekday before_open / after_close
- Saturday + Sunday → weekend
- skip_weekends=False mode (passes hour check)
- Futures hours (08:50 cutoff)
- Equity vs Futures routing via ticker
- Legacy ticker name routing (MX → MIX)
- Unknown ticker raises KeyError (defensive)
- Naive UTC input handling

**`tests/services/bridge/test_paper_executor.py`** (3 new tests):
- 31-min gap → REJECT (None returned)
- 6-min gap → fallback fills OK (within default 10-min threshold)
- Configurable threshold: 60s → 6-min gap rejected

**Full suite:** 397 passed + 1 skipped (was 377+1). +20 new, no regressions.

---

## Why these defaults

### Market hours: `enabled=True, skip_weekends=True`

| Scenario | Without gate | With gate |
|---|---|---|
| Saturday 14:00 news event | Decision fires, Bridge fills on Friday-close cached bar with hours of stale | REJECT(weekend) — clean |
| Monday 03:00 MSK overnight news | Fires, fills 3+ hours before market open | REJECT(before_open) — clean |
| Monday 12:00 normal news | EXECUTE | EXECUTE (no change) |
| Replayed 2024 news during weekend in 2024 | Bridge fills on Friday-close | REJECT(weekend) — replay knows it was Saturday |

### Stale-bar: `threshold=600s` (user-chosen, was 5min recommended)

| Gap | Result | Rationale |
|---|---|---|
| 0–60s | fallback fills | normal jitter, QUIK 1-min lag is fine |
| 60–600s | fallback fills | data behind but recoverable, fill on ~few-min old close acceptable |
| > 600s | REJECT | QUIK feed is broken, fill price not representative |
| Replay (any gap on past signal with future bars in cache) | fills on correct historical bar | gate doesn't trigger |

User chose 10 min over my 5-min recommendation — tolerant of network hiccups.

### Gap measured signal_time, NOT wall_clock

Critical for historical replay. If gap were wall-clock based, every
replayed news (from 2024 streamed through pipeline today) would have
`gap = 2 years` and be rejected. signal_time-relative gate stays correct.

---

## Operational notes

### How to verify after deploy

```powershell
# 1. New REJECT counter visible in Monitor heartbeats
nb-summary
# Look at decision metrics — rejects.market_closed should be 0 during weekday hours

# 2. Try replay during off-hours via script (signals will REJECT cleanly)
nb-script replay_historical_news.py --count 20 --from 2024-01-01 --to 2024-01-07 --no-confirm
nb-script analyze_signals.py
# Many signals → REJECT(market_closed:weekend) or (before_open)
```

### How to disable temporarily

```bash
# In .env or VPS systemd env
MARKET_HOURS_ENABLED=false
```

### Reject reason vocabulary

After this Sprint, `trade:signals` REJECT records can carry:

| reason prefix | source |
|---|---|
| `rr_below_threshold` | R:R check (was already there) |
| `direction_filter` family | B_filter (was already there) |
| `daily_kill_active` | RiskManager |
| `cooldown_active` | RiskManager |
| `max_open_positions reached ...` | RiskManager |
| **`market_closed:weekend`** | NEW |
| **`market_closed:before_open`** | NEW |
| **`market_closed:after_close`** | NEW |
| **`unknown_ticker_no_asset_class`** | NEW (defensive — shouldn't fire) |

For Bridge, the stale-bar reject is **not** published as an execution
event (consistent with existing `open_failed` silent path) — visible only
in Bridge logs and metric counter `errors.open_failed`. Sprint 6.x
backlog: properly emit failure execution events with structured reason.

---

## Acceptance

- ✅ Gates implemented
- ✅ Defaults: skip_weekends=True, stale threshold=600s
- ✅ news_time-relative (replay-safe)
- ✅ 20 new tests pass
- ✅ Full suite 397 passed (no regressions)

## Next

Restart Decision + Bridge services (VPS and local):
```powershell
# Local (Bridge runs here):
nb-stop ; nb-launch
# Decision runs locally too — nb-launch covers it
# Enricher/Receiver on VPS don't need restart (no changes there)
```

Then sit back and let Sprint 6.1 paper trading collection proceed
cleanly — no more April-cached fills, no more weekend bogus trades.
