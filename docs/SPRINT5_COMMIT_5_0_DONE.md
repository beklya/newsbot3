# Sprint 5 / Commit 5.0 — Housekeeping / DONE

**Закрыт:** 2026-05-25
**Длительность:** ~1ч
**Цель:** layout cleanup + contract bumps до v1.0.1 + HeartbeatPublisher refactor в `src/infra/` — всё что нужно подготовить до начала имплементации 5.1-5.5.

---

## A. Layout cleanup

Удалены пустые placeholder'ы (size=0 верифицированы):
- `src/decision/`, `src/predictor/`, `src/bridge/`, `src/monitor/`, `src/analyzer/`, `src/receiver/`
- (живые сервисы остаются в `src/services/{receiver,enricher}/`)

Удалены архивные дубликаты + .bak файлы:
- `src/contracts/Новая папка/enriched_news.py` — pre-Sprint 4.1 версия (нет validator-нормализации ticker'а)
- `src/contracts/enriched_news.py.bak`
- `src/infra/для отката/consumer.py` — pre-Sprint 3 stripped версия (нет PEL recovery)
- `scripts/generate_golden_samples.py.bak`
- `scripts/Новая папка/generate_golden_samples.py` — pre-Sprint 4.1.1 версия
- `tests/contracts/golden/Новая папка/` — старые golden samples

Финальная структура `src/`:
```
src/
  __init__.py
  contracts/   {6 контрактов + instruments registry}
  infra/       {consumer, publisher, idempotency, heartbeat}
  services/    {receiver, enricher}
```

Pre-deletion grep подтвердил: ни один production-файл не импортирует из удалённых путей.

---

## B. Contract bumps до v1.0.1

### TradeSignalEvent v1.0.0 → v1.0.1

`src/contracts/trade_signal.py`:

- Все EXECUTE-only поля сделаны `Optional` (default `None`):
  `side`, `horizon`, `entry_price`, `stop_loss`, `take_profit`, `quantity`,
  `risk_rub`, `expected_pnl_rub`, `rr_ratio`
- Сняты contract-level constraints:
  - `rr_ratio: ge=2.0` — теперь Decision config threshold
  - `open_positions: le=3` — теперь Decision MAX_OPEN_POSITIONS
- Добавлен `@model_validator`: if `action == "EXECUTE"` → все execute-поля required (ValidationError if missing)

**Зачем:** Decision (Sprint 5.2) публикует REJECT events для post-mortem analytics. Старый контракт не позволял REJECT (gt=0.0 / ge=1 / ge=2.0 на required полях).

### ExecutionResultEvent v1.0.0 → v1.0.1

`src/contracts/execution_result.py`:

Добавлены exit fields (Optional, default None):
- `realized_pnl_rub: float | None`
- `exit_reason: Literal["tp", "sl", "time", "kill"] | None`
- `exit_price: float | None` (gt=0 если задан)
- `exit_time: str | None`
- `duration_sec: int | None` (ge=0 если задан)

**Зачем:** Bridge (Sprint 5.3) публикует ДВА события на сделку — OPEN (entry, exit_*=None) + CLOSE (exit_* заполнены). Старый контракт не имел места под realized PnL.

**Migration:** NO_OP. Sprint 4 ничего не публиковал в `trade:signals` / `trade:executions` — оба stream'а впервые заполнятся в 5.2 / 5.3, дренаж не нужен.

---

## C. HeartbeatPublisher refactor

`src/services/receiver/heartbeat.py` + `src/services/enricher/heartbeat.py` (две почти одинаковые копии) → **`src/infra/heartbeat.py`** (single source).

Канонической взята enricher-версия как функциональный superset:
- Producer parametrized в task name (`f"{self.producer}.heartbeat"`)
- `SnapshotFn = Callable[[], Dict[str, Any]]` (receiver had `Dict[str, int]` — narrower subtype, backward compat)
- Лог-сообщения включают `producer=%s`

Обновлены импорты в:
- `src/services/receiver/client.py:20`
- `src/services/enricher/__main__.py:29`
- `tests/services/enricher/test_heartbeat.py:8`

Sprint 5.1-5.4 импортируют из `src.infra.heartbeat` (избежали 4 копий).

---

## D. Тесты

**Новые контракт-тесты (15 cases):**
- `tests/contracts/test_trade_signal_v1_0_1.py` (7 cases) — REJECT возможен, EXECUTE требует поля, rr_ratio/open_positions constraints сняты, round-trip OK
- `tests/contracts/test_execution_result_v1_0_1.py` (8 cases) — OPEN без exit_*, CLOSE с exit_*, exit_reason literal, validators на exit_price/duration_sec

**Golden samples** — регенерированы через `python scripts/generate_golden_samples.py`. Все 5 файлов с обновлёнными ULID, schema_version'ы остаются совместимыми (test_golden_sample_loads валидирует через `model_validate(data)` — старые версии 1.0.0 валидно парсятся новым 1.0.1 контрактом, т.к. все добавленные поля Optional).

---

## Verification

```powershell
cd D:\quik_sber\newsbot\newsbot3
.venv\Scripts\python.exe -m pytest -q
# Expected: 232 passed (217 baseline + 15 new contract tests)
```

Result: **232 passed in 9.67s** ✅

```powershell
# Sanity: новые контракты грузятся, заменённые импорты работают
.venv\Scripts\python.exe -c "from src.infra.heartbeat import HeartbeatPublisher; from src.contracts.trade_signal import TradeSignalEvent; from src.contracts.execution_result import ExecutionResultEvent; print('ok')"
# Expected: ok
```

---

## Files changed

**Deleted:**
- 6 placeholder `__init__.py` (decision/predictor/bridge/monitor/analyzer/receiver — на уровне src/)
- 2 archive folders + 4 .bak / archive duplicate files

**Modified:**
- `src/contracts/trade_signal.py` — v1.0.0 → v1.0.1 (Optional fields + model_validator)
- `src/contracts/execution_result.py` — v1.0.0 → v1.0.1 (exit fields)
- `src/services/receiver/client.py:20` — import path
- `src/services/enricher/__main__.py:29` — import path
- `tests/services/enricher/test_heartbeat.py:8` — import path
- `tests/contracts/golden/*.json` — регенерированы (5 файлов)

**New:**
- `src/infra/heartbeat.py` — single HeartbeatPublisher
- `tests/contracts/test_trade_signal_v1_0_1.py`
- `tests/contracts/test_execution_result_v1_0_1.py`

**Removed (после refactor):**
- `src/services/receiver/heartbeat.py`
- `src/services/enricher/heartbeat.py`

---

## DoD

| Критерий | Целевое | Факт | Status |
|----------|---------|------|--------|
| Layout cleanup — `ls src/` чистый | {contracts, infra, services, __init__} | ✓ | ✓ |
| TradeSignal v1.0.1 (Optional + EXECUTE validator) | реализовано | ✓ | ✓ |
| ExecutionResult v1.0.1 (exit fields) | реализовано | ✓ | ✓ |
| HeartbeatPublisher refactor в src/infra/ | single source | ✓ | ✓ |
| Pytest зелёный | 100% pass | 232/232 | ✓ |
| Golden samples регенерированы | 5 файлов | ✓ | ✓ |

**Sprint 5 / Commit 5.0 — closed ✅**

Готовы к 5.1 (Predictor service + Fold 13 training).
