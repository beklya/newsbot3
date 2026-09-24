> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 5.11 — VPS Deployment & Observability (DONE)

**Status:** Closed 2026-05-31
**Goal:** Move Receiver+Enricher to a VPS, keep local services for QUIK integration.

---

## Sub-sprints

### 5.11.0 — VPS provisioning

- Budget VPS (1 vCPU / 1 GB / 10 GB NVMe)
- Ubuntu 22.04 LTS, SSH key auth, UFW firewall, non-root user with sudo

### 5.11.1 — Phase 1: Receiver+Enricher on VPS

- Python 3.12 + Redis 6.0.16 installed on VPS
- 1 GB swap configured (safety net for 1 GB RAM tier)
- Code synced via `tar+scp` (excluded .venv, data, docs, sprint4)
- venv created, full `requirements.txt` installed
- Telegram session copied from Windows (no re-auth needed)
- `/etc/systemd/system/newsbot-receiver.service`, `newsbot-enricher.service`
- systemd auto-restart on failure, journald logging

### 5.11.2 — Phase 1.5: Local services + SSH tunnel

- SSH tunnel `127.0.0.1:6380 -> VPS:6379` for local services to access VPS Redis
- 5 local services (quik_feed/predictor/decision/bridge/monitor) read REDIS_URL env var → tunneled VPS Redis
- `scripts/launch_local_services.ps1` — 5-tab Windows Terminal launcher with dedup guard
- 19 nb-* helper functions in PowerShell profile
- `scripts/redis_inspect.py` + 6 others patched: read REDIS_URL from env, Python 3.14 asyncio fix

### 5.11.3 — Tunnel watchdog + cache TTL bumps

- `scripts/ssh_tunnel_watchdog.ps1` — long-running loop, checks TCP every 15s, respawns dead tunnels
- Auto-start from PowerShell profile (`Get-CimInstance` dedup)
- `EnricherSettings.enrichment_cache_ttl_sec` validator bumped 3600 → 604800 (7 days)
- Reason: 5-minute default caused massive `enrichment_missing` rate during any backlog (predictor restart, network drop)
- VPS `.env` set to `ENRICHMENT_CACHE_TTL_SEC=86400` (24h)

### 5.11.x bug fixes encountered

| Issue | Fix |
|---|---|
| ExecutionPolicy blocks PowerShell profile | `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| Python 3.14 + Windows asyncio + tunneled localhost | `asyncio.WindowsSelectorEventLoopPolicy()` in scripts |
| Redis 6.0 doesn't support exclusive XRANGE `(ID` | Patched `monitor/aggregator.py` to use `_next_id_after` |
| Profile encoding (em-dash + no BOM) breaks PS 5.1 parser | Rewrote profile ASCII-only |
| `$matches[N]` inside `$()` in double-quoted strings | Assigned to local var first |

---

## Final architecture

```
VPS (VPS_HOST, Ubuntu 22.04):
  ├── Redis 6.0.16 (localhost only, AOF, maxmemory 600mb)
  ├── newsbot-receiver.service (systemd, autorestart)
  └── newsbot-enricher.service (systemd, autorestart, TTL=24h)

Windows local PC:
  ├── 5 services in Windows Terminal tabs (nb-launch)
  │     quik_feed, predictor, decision, bridge, monitor
  ├── SSH tunnel watchdog (PowerShell profile auto-start)
  │     127.0.0.1:6380 -> VPS:6379 (auto-restart on disconnect)
  └── QUIK Workstation + candle_dump.lua → quik_live/candles.csv

Pipeline:
  Telegram --> Receiver(VPS) --> news:raw
           --> Enricher(VPS) --> news:enriched
           --> Predictor(local) --> ml:predictions
           --> Decision(local) --> trade:signals
           --> Bridge(local paper) --> trade:executions
  
  quik_feed(local) --> candles:1m (broadcast)
  Monitor(local) -- system:heartbeats
```

---

## Files created/modified in Sprint 5.11

### Created
- `scripts/launch_local_services.ps1` + `.bat`
- `scripts/ssh_tunnel_watchdog.ps1` + `.bat`
- `scripts/replay_historical_news.py`
- `scripts/analyze_signals.py`
- `scripts/analyze_pnl.py`
- `scripts/debug_news_time.py`
- `docs/CHEATSHEET.md`
- `docs/SPRINT_PHASE2_MINIPC_PLAN.md`
- `C:\Users\<user>\Documents\WindowsPowerShell\Microsoft.PowerShell_profile.ps1`

### Modified — Receiver/Enricher optional proxy support
- `src/services/receiver/config.py` — proxy_enabled, proxy_url, resolve_proxy_tuple()
- `src/services/receiver/client.py` — wired TelegramClient(proxy=...)
- `src/services/enricher/config.py` — proxy_enabled, proxy_url, cache_ttl validator
- `src/services/enricher/key_pool.py` — proxy_url → httpx SOCKS5
- `src/services/enricher/llm_client.py` — pass proxy to pool, SCHEMA_VIOLATION retryable=False
- `src/services/monitor/aggregator.py` — Redis 6.0 XRANGE compat (`_next_id_after`)
- `requirements.txt` — httpx[socks], python-socks[asyncio], PySocks, groq
- `.env.example` — proxy section
- `scripts/redis_inspect.py`, `watch_enriched.py`, `analyze_soak.py` — REDIS_URL from env + asyncio Selector fix
- `scripts/check_telegram_health.py` — REDIS_URL from env

### Modified — observability fixes
- `scripts/clean_redis_for_restart.py` — `--wipe` mode (nuclear cleanup)

---

## Observability commands (final)

See `docs/CHEATSHEET.md` for full reference. Quick:

```powershell
nb-status                           # all services + tunnel + watchdog + VPS
nb-summary                          # stream lengths
nb-enriched 10                      # last 10 enriched (parsed)
nb-watch                            # live tail enriched (color)
nb-tg                               # Telegram MTProto health
nb-log-receiver / -enricher         # VPS journal live
nb-restart-receiver / -enricher     # VPS systemctl restart
nb-launch / -stop                   # local services
nb-tunnel-restart                   # force tunnel respawn
```

---

## Lessons learned

1. **Python 3.14 ProactorEventLoop + tunneled localhost TCP = broken**. Have to fall back to SelectorEventLoop, which is deprecated in 3.16. Sprint 6+ may need new asyncio API (`asyncio.Runner(loop_factory=...)`).

2. **Redis 6.0 lacks `(STREAM_ID` exclusive XRANGE**. Ubuntu 22.04 default. For 7.x need third-party PPA.

3. **Cache TTL 5min is too short** for any non-steady-state pipeline. 24h is safe upper bound, doesn't hurt prod.

4. **PowerShell ExecutionPolicy + non-ASCII chars in profile + Unblock-File ADS marker** — three things that conspire to break PS profile loading. Solutions: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`, ASCII-only profile, `Unblock-File $PROFILE`.

5. **systemd `Restart=on-failure` with broken config causes restart loop** (every 10s). User-visible damage: filled journal, wasted CPU. Mitigation: validate config locally before sed-edit on VPS.

---

## Status at sprint close (2026-05-31 23:00 UTC)

- VPS Receiver+Enricher uptime since 17:38 UTC, processing live news
- Local services all running after Redis wipe baseline reset
- 0 events in DLQ
- Pipeline clean state ready for Sprint 6.1 paper PnL collection start (Monday 2026-06-01)

Next: Sprint 6 work (news_time pass-through for honest historical replay — see `docs/SPRINT_6_NEWS_TIME_DONE.md`).
