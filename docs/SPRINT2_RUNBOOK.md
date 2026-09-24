# Sprint 2 — Telegram Receiver / Runbook

## Status

* **Commit 1** (config + .env.example) — DONE
* **Commit 2** (Telethon wrapper, dry-run) — DONE
* **Commit 3-4** (RawNewsEvent build + IdempotencyGuard + StreamPublisher) — pending contracts review
* **Commit 5** (heartbeat + graceful shutdown) — pending
* **Commit 6** (24h soak test) — pending

## What works in Commit 1+2

The receiver can connect to Telegram, resolve all 4 channels, optionally
replay the last N hours, and listen for live messages. Each message is
logged to stdout. **Nothing is published to Redis yet** — that is Commit 3-4.

## Folder layout (place these in newsbot3 root)

```
newsbot3/
├── .env                    # copy from .env.example, fill secrets
├── .env.example            # committed template
├── src/
│   └── services/
│       └── receiver/
│           ├── __init__.py
│           ├── __main__.py
│           ├── config.py
│           └── client.py
├── data/
│   └── sessions/           # auto-created on first run
└── docs/
    └── SPRINT2_RUNBOOK.md  # this file
```

## First-time setup

1. Copy the template:
   ```
   copy .env.example .env
   ```
   The template already has the working credentials from Phase 1.
   Verify they are still valid before the first run.

2. Confirm Telethon is installed in the venv:
   ```
   pip show telethon
   ```
   If missing: `pip install telethon`.

3. **First run = interactive.** Telethon will ask for the SMS code from
   Telegram to create `data/sessions/receiver.session`. After that, all
   subsequent runs are headless.

## Run modes

### Calibration run (recommended first)

Pulls the last 1 hour of history, then switches to realtime:

```
python -m src.services.receiver --backfill-hours 1
```

Expected output:
```
[INFO] receiver.main: starting receiver (channels=['interfaxonline', ...], backfill=1h)
[INFO] receiver.client: telethon: authorized as +XXXXXXXXXXX
[INFO] receiver.client: channel resolved: @interfaxonline -> Интерфакс
[INFO] receiver.client: channel resolved: @rian_ru -> РИА Новости
[INFO] receiver.client: channel resolved: @tass_agency -> ТАСС
[INFO] receiver.client: channel resolved: @rbc_news -> РБК
[INFO] receiver.client: backfill: cutoff=2026-05-07T12:00:00+00:00
[INFO] receiver.client: [backfill] @interfaxonline msg_id=12345 at=... | ...
...
[INFO] receiver.client: backfill: total dispatched = 87
[INFO] receiver.client: listening on 4 channels, edited=True
[INFO] receiver.client: [live] @rbc_news msg_id=99988 at=... | ...
```

### Production run

Realtime only, no backfill:

```
python -m src.services.receiver
```

### Disabling edited messages

If the volume of edits turns out to be excessive and pollutes the
stream, set in `.env`:
```
HANDLE_EDITED_MESSAGES=false
```

## Stop conditions

* `Ctrl+C` — graceful shutdown via SIGINT
* `taskkill` (Windows) — graceful shutdown via SIGTERM where supported

## What to verify before signing off Commit 2

- [ ] All 4 channels resolve (no `channel resolve FAILED` errors)
- [ ] `data/sessions/receiver.session` is created
- [ ] At least one live message arrives within ~5 min (most active hours)
- [ ] Backfill of 1h returns a plausible count (≥10 messages mid-day)
- [ ] No tracebacks; SIGINT exits cleanly

## Open items for Commit 3+

To wire up publishing (Commit 3-4), please share the contents of:
* `src/contracts/raw_news.py`
* `src/contracts/base.py` (specifically the envelope factory pattern)
* `src/infra/publisher.py` (signature of `publish()`)
* `src/infra/idempotency.py` (signature of `acquire()`)

With those in hand, the only file that needs to change is `client.py` —
specifically the `_dispatch()` method.
