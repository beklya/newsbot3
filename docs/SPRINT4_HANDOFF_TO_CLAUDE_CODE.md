> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 4 / Commit 4.2 — Handoff to Claude Code

Сжатый контекст. Все детали — в `docs/SPRINT*_DONE.md`.

---

## Где мы сейчас (2026-05-18)

**Phase 3 / Sprint 4 — LLM validation track.**

| Закрытые коммиты | Файлы для просмотра |
|---|---|
| **4.0** exits comparison (baseline best, Sharpe 4.98) | `D:\quik_sber\newsbot\newsbot3\sprint4\exits\` + `docs\SPRINT4_COMMIT_4_0_DONE.md` |
| **4.1** instrument registry + validator (127/127 tests) | `src\contracts\instruments.py`, `tests\contracts\test_instruments.py`, `tests\contracts\test_enriched_news_normalization.py` + `docs\SPRINT4_COMMIT_4_1_DONE.md` |
| **4.2.a** dataset discovery (860k records 2016-2026) | `sprint4\discovery\` + `discovery_telegram_news_report.txt` |

**Активный коммит: 4.2.b — stratified sampling plan** (написать на основе 4.2.a discovery)

---

## Discovery 4.2.a — facts on the ground

**Source:** `D:\quik_sber\newsbot\duble3\telegram_news.jsonl` (880 MB, 860,626 records)

Schema (json line):
```
id, tg_msg_id, source, channel, datetime, timestamp, date, time,
headline, full_text, collected_at, analysis (=null везде)
```

| Field | Coverage | Notes |
|---|---|---|
| `full_text` | 100% | Median 252 chars, p95 904, max 3216 |
| `headline` | 100% | Короткая версия |
| `timestamp` | 100% | UTC seconds, validated |
| `datetime` | 100% | MSK string, **100% совпадает с timestamp** |
| `tg_msg_id` | 100% | Все уникальны → **дубликатов по msg_id нет** |
| `analysis` | 0% | Пустое — enrichment лежит в отдельных файлах |

**Дубликаты:** 0.62% по SHA-256 текста (5,331 из 860k).
**Emoji:** 23.4% записей (201k). **Известная проблема:** Groq 400 на "🗣 Пескова" — нужен fix в re-enrichment.

**Каналы (4):**
| | events | first_seen |
|---|---|---|
| tass_agency | 353,512 | 2016-06-13 |
| rian_ru | 304,386 | **2017-02-28** |
| rbc_news | 135,394 | 2016-09-07 |
| interfaxonline | 67,334 | **2017-10-24** |

**Events per year (с 2022 начинаются цены MOEX):**
- 2022: 155,513 (пик 2022-02/03 — СВО, 18k/мес)
- 2023: 109,147 (-30% спад)
- 2024: 142,862
- 2025: 146,680
- 2026: 37,440 (первые 4 месяца)

---

## Архитектура источников данных (картина целиком)

```
D:\quik_sber\newsbot\duble3\telegram_news.jsonl       # SOURCE (860k, 2016-2026)
  ├─ full_text 100%, headline 100%, MSK + UTC validated
  │
  ├─> [truncated to 200 chars в Phase 2]
  │   D:\quik_sber\newsbot\api\enriched_news_full.jsonl
  │   └─ ~394K записей с price moves (22 horizons × 19 tickers)
  │      │
  │      └─> [legacy 8b llama enrichment, ДРУГОЙ промпт]
  │          D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 3 - дедупликация\
  │            ├─ news_pool.jsonl
  │            └─ news_pool_clean.jsonl (отфильтрованный по сессиям)
  │
  └─> [Sprint 3 enricher, новый промпт v1.0.0]
      Memurai stream: news:enriched (живой, ~46 events за 4ч soak)
```

**Memory note:** `enriched_news_full.jsonl` упомянут в memory как 2.5GB / 394K. Проверить актуальный размер в Claude Code.

---

## Sprint 4 — оставшиеся коммиты

| # | Что | Зависимость |
|---|---|---|
| **4.2.b** | Stratified sampling plan: какие даты, баланс по каналам, сколько записей в выборке | discovery 4.2.a ✅ |
| **4.3** | Sampling code + сэмпл 30K (или другой объём по 4.2.b) | 4.2.b |
| **4.4** | Pre-flight llama-4-scout (если доступно) — оценить новую модель Groq | 4.3 |
| **4.5** | Re-enrichment llama-3.1-8b + новый promt | 4.3, 4.4 |
| **4.6** | Re-enrichment llama-3.3-70b + новый promt | 4.5 |
| **4.7** | **Factorial 2x2 analysis:** {legacy/new promt} × {8b/70b} | 4.5, 4.6, legacy news_pool.jsonl |
| **4.8** | Calibration per (category × horizon) | 4.7 |
| **4.9** | Hybrid candidates evaluation (Phase 2 baseline / A / B) | 4.8 |
| **4.10** | Dynamic-horizon prototype + per-ticker excluded list (SBER!) | 4.9 |

**Бюджет:** $25-35 на re-enrichment (8b + 70b на calibration window).

---

## Главное про factorial sravnenie промптов (для commit 4.7)

the author указал в чате 4.2.a:

> Есть файл чистых новостей, есть файл чистых новостей с обрезанными до 200 символов новостями + price moves. После этого этот файл прошел через legacy 8b llama enrichment. **Но там другой пропт использовался**, нужно будет сравнить тот промпт с тем, что используется сейчас и на 8b llama. После этого сравнить более удачный промпт с 70b llama.

**План factorial:**
1. Найти legacy promt в `newsbot2\решение проблем\Проблема 3\` (или соседних папках)
2. Найти new promt v1.0.0 в `newsbot3\src\services\enricher\prompts\` (или где он лежит)
3. Run 8b × legacy promt = baseline (уже есть в news_pool.jsonl, **не платим** заново)
4. Run 8b × new promt = новая enrichment
5. Сравнить: какой промпт лучше — выбрать **winner_promt**
6. Run 70b × winner_promt = финал
7. Сравнить 8b × winner_promt vs 70b × winner_promt — есть ли смысл платить за 70b

---

## Phase 2 best combo (для калибровки в 4.9)

- h=60min, rr=2.0, model=mx_specific
- 3300 trades, Sharpe 4.98 (Phase 2 баSELINE = production-honest ~5.15)
- **SBER -146k во всех 5 exit strategies** → кандидат на per-ticker exclude
- Trailing SL вредит на новостных импульсах (-66% Sharpe)
- Partial-стратегии не окупаются при текущем cost_rub

---

## Key contracts (Sprint 1, актуально v1.1.0)

`src/contracts/`:
- `base.py` — MessageEnvelope (event_id, trace[], add_trace())
- `raw_news.py` — RawNewsEvent v1.0.0 (channel, message_id, text, text_hash sha256, has_media, is_reply, is_forward)
- `enriched_news.py` — EnrichedNewsEvent **v1.1.0** (+prompt_version, +is_financial, +expected_timeframe, +urgency, +category, +is_actionable). Sprint 4.1: **TickerImpact.ticker валидируется через registry** (legacy→canonical)
- `ml_prediction.py` — MLPredictionEvent v1.0.0 (feature_count=67 exact, 2 horizons)
- `trade_signal.py` — TradeSignalEvent v1.0.0 (action EXECUTE/REJECT, side BUY/SELL ⚠️ inconsistent с long/short в Enriched)
- `execution_result.py` — ExecutionResultEvent v1.0.0 (status FILLED/PARTIAL/REJECTED/TIMEOUT/ERROR)
- **`instruments.py`** ⭐ — 19 инструментов с lot_size, normalize_ticker(), CANONICAL_TICKERS

---

## Tooling / env

- **Python 3.14**, venv at `.venv\Scripts\activate.bat`
- **Memurai 8.1.240** at localhost:6379 (Windows service, Redis-compatible)
- **Pytest 9.0.3, pydantic 2.13.4, redis-py 5.3 async, telethon, ulid**
- **Console:** `set PYTHONIOENCODING=utf-8 && chcp 65001` для UTF-8 (cp1251 default ломает эмодзи в выводе)
- Project root: `D:\quik_sber\newsbot\newsbot3`
- Phase 2 trades: `D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet`
- Prices (19 CSV): `D:\quik_sber\newsbot\prices\prices_*.csv` (2022-2026, naive MSK)

---

## Backlog (не блокеры)

1. **Latency trend p95 1.8s → 9s** за 4h soak — investigate в Sprint 4
2. **DLQ empty_financial 87%** — улучшить промпт чтоб курсы ЦБ → is_financial=False
3. **Side mismatch BUY/SELL vs long/short** — унификация Sprint 5
4. **TICK_VALUE periodic re-verification** через QUIK API
5. **SBER excluded list** — per-ticker filter через LLM в 4.10
