# Sprint 4 / Commit 4.1 — Instrument Registry / DONE

**Закрыт:** май 2026
**Длительность:** ~3 часа (включая 2 итерации фикса cp1251 + golden samples Sprint 3 debt)

---

## Цель

Интегрировать `instruments.py` (созданный в коммите 4.0) в основной проект:
- Реестр 19 инструментов как single source of truth
- Validator-based нормализация legacy → canonical в EnrichedNewsEvent
- Без version bump контракта (soft compatibility layer)

---

## Что сделано

### Файлы

| Файл | Назначение | LOC |
|------|------------|-----|
| `src/contracts/instruments.py` | Реестр 19 инструментов + helpers | ~150 |
| `tests/contracts/test_instruments.py` | Unit-тесты реестра | 83 теста |
| `tests/contracts/test_enriched_news_normalization.py` | Integration-тесты validator | 38 тестов |
| `src/contracts/enriched_news.py` (patched) | `+field_validator` на `TickerImpact.ticker` | +30 LOC |
| `scripts/generate_golden_samples.py` (rebuilt) | Sprint 3 debt — pereread под контракт v1.1.0 | ~200 |

### Тесты — 127/127 passed

| Файл | Тесты | Время |
|------|-------|-------|
| `test_instruments.py` | 83 | 0.16s |
| `test_enriched_news_normalization.py` | 38 | 0.21s |
| `test_schemas.py` (Sprint 1) | 6 | < 1s |

---

## Что делает validator

```python
class TickerImpact(BaseModel):
    ticker: str = Field(..., description="MOEX ticker (canonical or legacy)")
    ...

    @field_validator("ticker", mode="before")
    @classmethod
    def normalize_ticker_field(cls, v: str) -> str:
        normalized = try_normalize_ticker(v)
        if normalized is None:
            raise ValueError(f"Unknown ticker {v!r}")
        return normalized
```

**Behaviour:**
- LLM возвращает `"Si"` → объект создаётся с `ticker="SI"`
- LLM возвращает `"MX"` → `ticker="MIX"`
- LLM возвращает `"YNDX"` → `ticker="YDEX"`
- LLM возвращает `"GOLD"` → `ticker="GLDRUB"`
- LLM возвращает `"SBER"` → `ticker="SBER"` (idempotent)
- LLM возвращает `"AAPL"` → **ValidationError** → DLQ как schema_violation
- LLM возвращает `"sber"` (lowercase) → ValidationError (защита от format hallucinations)

---

## Реестр инструментов — 19 тикеров

### Equity (12)

| Canonical | Legacy | lot_size | USD? |
|-----------|--------|----------|------|
| SBER | — | 10 | no |
| GAZP | — | 10 | no |
| ROSN | — | 10 | no |
| MTSS | — | 10 | no |
| LKOH | — | 1 | no |
| GMKN | — | 1 | no |
| NVTK | — | 1 | no |
| TATN | — | 1 | no |
| MGNT | — | 1 | no |
| PLZL | — | 1 | no |
| YDEX | YNDX | 1 | no |
| VTBR | — | **10000** | no |

### Futures (4)

| Canonical | Legacy | lot_size | USD? |
|-----------|--------|----------|------|
| MIX | MX | 1 | no |
| SI | Si | 1 | no |
| BR | — | 1 | **YES** |
| NG | — | 1 | **YES** |

### Commodity (1)

| Canonical | Legacy | lot_size | USD? |
|-----------|--------|----------|------|
| GLDRUB | GOLD | 1 | **YES** |

### Currency (2)

| Canonical | Legacy | lot_size | USD? |
|-----------|--------|----------|------|
| CNY | — | 1 | no |
| USDRUB | — | **1000** | no |

---

## Sprint 3 tech debt — Resolved

При прогоне `pytest tests\contracts\` обнаружились pre-existing проблемы:

1. **`enriched_news_v1.json` v1.0.0 не валидируется** контрактом v1.1.0
   (отсутствовали `prompt_version`, `is_financial`, `expected_timeframe`)
2. **`raw_news_v1.json` event_id ≠ enriched.raw_event_id** — chain сломан
3. **`generate_golden_samples.py` использовал устаревший контракт** v1.0.0

Это **Sprint 3 tech debt**, не моя регрессия. Починен заодно:
- Перегенерированы все 5 golden samples под v1.1.0
- event_id chain восстановлен (raw → enriched → prediction → signal → execution)

---

## Решения и обоснования

| Решение | Обоснование |
|---------|-------------|
| **Не делать version bump 1.1.0 → 1.2.0** | Validator — backward-compatible, существующие 1.1.0 потребители продолжают работать |
| **Hard reject lowercase ticker (sber)** | Защита от LLM-галлюцинаций со странным форматом; whitelist жёсткий по casing |
| **Hard fail на unknown ticker** | Лучше отправить event в DLQ как `schema_violation`, чем тихо потерять данные |
| **VTBR `lot_size=10000`** | Зафиксировано из Phase 2 backtest_mfe.py:108-114 — особенность MOEX |
| **USDRUB `lot_size=1000`** | То же — особенность инструмента |

---

## Memurai state (на момент закрытия 4.1)

Из soak отчёта `docs/soak_state_before_sprint4.txt`:
- `news:enriched`: ~46 событий за 4.07h
- DLQ: 15 events (13 `empty_financial`, 2 `api_error`)
- Pool cooldown: 0%
- p95 latency растёт: 1.8s → 9.0s (backlog для Sprint 4.2+)

В enriched stream **нет legacy имён** (LLM-promt v1.0.0 уже возвращает canonical через whitelist),
поэтому миграция данных не требуется. Стрим можно очистить перед Sprint 5 без потерь.

---

## Backlog для будущих коммитов

1. **Latency trend p95 1.8s → 9s за 4h** — Sprint 4.2+ investigation
2. **`empty_financial` DLQ доминирует** (87% DLQ) — Sprint 4 промпт v1.0.1
   нужно добавить правило для курсов ЦБ (нет MOEX-тикера → tickers=[])
3. **Side mismatch `BUY/SELL` vs `long/short`** между TradeSignal и EnrichedNews —
   Sprint 5 backlog (методологическое унификация)
4. **`empty_financial` → `is_financial=False`** автоматически (не DLQ),
   если whitelist filter дал пустой список — Sprint 4.2 / промпт v1.0.1

---

## Verification commands

```cmd
REM Reproduce Sprint 4.1 results:
cd /d D:\quik_sber\newsbot\newsbot3
.venv\Scripts\activate.bat

REM 1. Прогон всех contract тестов
pytest tests\contracts\ -v
# Expected: 127/127 passed (38 + 83 + 6)

REM 2. Smoke pipeline (Sprint 1 demo)
python scripts\demo_pipeline.py
# Expected: 3/3 зелёные

REM 3. Registry CLI inspection
python -c "from src.contracts.instruments import INSTRUMENTS, all_canonical_tickers; print(f'{len(INSTRUMENTS)} instruments, canonical: {sorted(all_canonical_tickers())}')"
```

---

## DoD — все критерии выполнены

| Критерий | Целевое | Факт | Статус |
|----------|---------|------|--------|
| instruments.py в `src/contracts/` | ✅ | ✅ | OK |
| 19 инструментов с lot_size | 19 | 19 | OK |
| Unit-тесты registry | ≥ 50 | 83 | OK |
| Integration-тесты validator | ≥ 20 | 38 | OK |
| validator на TickerImpact.ticker | ✅ | ✅ | OK |
| Все contract-тесты зелёные | 100% | 127/127 | OK |
| Snapshot документ | ✅ | этот файл | OK |

**Sprint 4 / Commit 4.1 — закрыт ✅**

---

## Next: Sprint 4 / Commit 4.2

**Dataset Discovery — telegram_news.jsonl**

Цель: понять, что лежит в источнике сырых новостей с 2022 года.

Задачи:
1. Профилировать `D:\quik_sber\newsbot\duble3\telegram_news.jsonl`
   - Total events, по годам/каналам/типам
   - Распределение длины текста
   - Дубликаты, пропуски, edited messages
2. **TZ verification** (критично!) — какая TZ в `datetime` поле?
   В Sprint 4 коммит 4.0 мы зафиксировали:
   - `prices/*.csv`: naive MSK
   - `phase2_mfe_trades.parquet`: naive (видимо MSK)
   - `telegram_news.jsonl`: подтверждено что `datetime` — MSK строка, `timestamp` — UTC seconds
3. Stratified sampling план для re-enrichment:
   - 2025-07-01 → 2025-12-31 (calibration window)
   - 2026-01-01 → 2026-04-30 (validation holdout)
