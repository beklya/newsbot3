> ⚠️ **Исторический документ.** Числа Phase 2 (Sharpe 4.87), B_filter (6.42 / V1 3.14) и выводы на их основе позже опровергнуты: разметка корпуса Phase 2 содержала утечку будущего, а косты брокера были занижены. См. README (раздел «Результаты») и `SPRINT_6_3_DONE.md`.

# Sprint 4 / Commit 4.3 — Stratified Sample / DONE

**Закрыт:** 2026-05-18
**Длительность:** ~1 час (план + код + два прогона + sanity)
**Артефакты:** `sprint4/sampling/`

---

## Цель

Превратить план 4.2.b в реальные parquet-выборки **C1** (calibration, 2025-04..12) и
**V1** (validation holdout, 2026-01..04) для re-enrichment 4.5/4.6 и factorial 4.7.

---

## Что сделано

### Файлы

| Файл | Назначение | LOC |
|------|-----------|-----|
| `sprint4/sampling/build_sample.py` | Stratified sampler (polars-based) | ~360 |
| `sprint4/sampling/plan.md` | План 4.2.b (zero changes since approval) | — |
| `sprint4/sampling/data/calibration_sample.parquet` | C1 sample | 30,518 rows × 17 cols |
| `sprint4/sampling/data/validation_sample.parquet` | V1 holdout | 8,000 rows × 17 cols |
| `sprint4/sampling/data/stratification_report.txt` | Per-stratum breakdown + diagnostics | — |

### Зависимости

- Добавлен `polars>=0.20` в `requirements.txt` (установлен polars 1.40.1)
- Использует уже-присутствующие numpy, pyarrow

---

## Результаты vs план

| Метрика | План 4.2.b | Факт | Δ |
|---|---:|---:|---|
| C1 total | 31,200 | **30,518** | -682 |
| C1 session | 22,000 | 22,466 | +466 |
| C1 offsession | 8,000 | 8,052 | +52 |
| C1 anchor force-added | ~1,200 | 530 | -670 |
| V1 total | 8,000 | **8,000** | 0 |
| V1 session | 5,500 | 5,531 | +31 |
| V1 offsession | 2,000 | 1,969 | -31 |
| V1 pad | 500 | 500 | 0 |
| C1↔V1 overlap | 0 | **0** | ✓ |
| Dedup drop (full_text) | ~0.62% | 0.62% | ✓ |

**Расхождение по anchors** (530 vs ~1,200): план переоценил число Phase 2 best-combo trades
в C1 окне. Реальность: best combo (h=60, rr=2.0, mx_specific) даёт всего 3,300 trades
за 2023-01 → 2026-04, из них только **770** попадают в C1 окно 2025-04..12 (остальные
2,530 — 2023-2024 + начало 2026). Из 770 trades 760 уникальных news-anchor'ов нашлось
в 60-секундном окне (10 trades без news в окне — нормально). Из 760 anchors 230 уже
выпали в random sample → force-added только 530.

**Это не баг плана — план перезаложил.** Реальный масштаб anchor coverage хороший:
760 anchor news = 11.5% всех Phase 2 best-combo сделок, чего хватает для прямого
сравнения нового enrichment'а с Phase 2 baseline на пересечении.

---

## Sanity checks — все прошли

| Проверка | Целевое | Факт |
|---|---|---|
| text_hash uniqueness | 100% | 30,518 / 30,518 |
| Channel mix C1 | 41/35/16/8 ±2% | 40.42 / 35.18 / 15.82 / 8.58 |
| Month uniformity C1 | 3,400 ±100 / мес | 3,355-3,421 |
| Session/offsess split C1 | 22k / 8k | 22,466 / 8,052 |
| **Anchor time_diff** | ∈ [0, 60s] от Phase 2 ts_open | min=1.0s, max=59.0s, median=29.0s |
| Legacy enrichment coverage C1 | — | 25,492 / 30,518 = 83.5% |
| Legacy enrichment coverage V1 | — | 6,017 / 8,000 = 75.2% |
| Text length p50/p95/max | < cap 8000 | 210 / 912 / 3,968 |
| C1↔V1 overlap | 0 | 0 |

---

## Эмодзи — неожиданно высокий процент

| Сэмпл | has_emoji | % |
|---|---:|---:|
| C1 (2025-04..12) | 16,514 | 54.1% |
| V1 (2026-01..04) | 5,553 | 69.4% |
| Global (4.2.a, 10 лет) | 201,172 | 23.4% |

Не баг — глобальные 23% размазаны по 2016-2026, эмодзи в Telegram-новостях массово
появились только в последние ~2 года. Наша выборка 2025-2026 ловит реальную сегодняшнюю
плотность эмодзи в источниках. Это даёт жирный стресс-тест Groq 400 (`🗣 Пескова` issue
из SPRINT3_DONE.md) для коммита 4.5 — если sanitization понадобится, будет видно сразу.

---

## Реальный бюджет 4.5/4.6

| Phase | Cell | n events | Cost |
|-------|------|---------:|-----:|
| 4.5 | 8b × new_prompt × no-prices | 30,518 | **~$7.1** |
| 4.6 | 70b × winner_prompt × no-prices | 8,000 (random из C1) | **~$22** |
| **Итого** | | | **~$29.1** |

В коридоре плана $25-35.

---

## Решения и обоснования

| Решение | Обоснование |
|---|---|
| **polars 1.40 вместо pandas chunks** | 70 сек cold / 16 сек warm vs ~5 мин pandas. Лучшее ROI на повторных прогонах при fine-tune страт |
| **Hardcoded Phase 2 best combo фильтр** | Без него 359,433 trades → ~50k unique anchors → C1 раздуется до 60k+ rows → бюджет ломается. Best combo даёт чистое сравнение со Sharpe 4.98 baseline |
| **Anchor = последняя новость в [t-60s, t]** | Логично: trade открылся через ≤60s после новости. Берём ближайшую по времени, не первую |
| **`id_arr[i_hi - 1]`** | Одно anchor news на один trade. Если несколько новостей в окне, остальные могут попасть через random sample |
| **Dedup по full_text, не text_hash** | Polars native unique() быстрее SHA256 на 860k. SHA256 считаем только на 40k финальных записей для downstream join-key |
| **Equal-monthly + natural-channel mix** | Месяц не имеет behavioural bias (вс. сезонность есть, но мы не калибруем под неё). Канал имеет editorial bias — сохраняем |

---

## Verification commands

```powershell
cd /d D:\quik_sber\newsbot\newsbot3
.venv\Scripts\activate.bat

REM 1. Воспроизвести sample (deterministic, seed=42)
set PYTHONIOENCODING=utf-8
python sprint4\sampling\build_sample.py --seed 42

REM 2. Quick inspect
python -c "import polars as pl; df = pl.read_parquet('sprint4/sampling/data/calibration_sample.parquet'); print(df.shape); print(df.schema)"

REM 3. Read full report
type sprint4\sampling\data\stratification_report.txt
```

Ожидаемое: C1 = 30,518 rows, V1 = 8,000 rows, identical across runs (seed fixed).

---

## Backlog для будущих коммитов

1. **4.5 sanitization fix (если нужен).** Если 8b на C1 даст >2% Groq 400 на emoji-rich
   записях (54% покрытия — большой риск), добавить `unicodedata.normalize` + strip Cat:So
   в `src/services/enricher/llm_client.py` перед промптом. Затем перезапуск 4.5 на failed
   subset. Цена fix: $0.5-1.
2. **4.7 normalization layer.** Legacy vs new output schemas разные — для factorial нужен
   unified comparable schema (см. plan.md "Comparison normalization"). Это **не блокер 4.5**,
   но должен быть в начале 4.7.
3. **Anchor coverage shortfall investigation.** 10 best-combo trades в C1 окне **не** имеют
   news в 60s окне. Это нормально (часть Phase 2 trades могут срабатывать по price-pattern
   features без news trigger), но стоит подтвердить, что эти 10 — не баг матчинга по TZ.

---

## DoD — все критерии выполнены

| Критерий | Целевое | Факт | Статус |
|----------|---------|------|--------|
| build_sample.py запускается за < 5 мин на 880MB | ✅ | 70 сек cold / 16 сек warm | OK |
| C1 ≈ 30k, V1 = 8k, disjoint | ≈30k / 8k | 30,518 / 8,000 / overlap=0 | OK |
| Channel proportions ±2% от natural | ±2% | ±0.6% | OK |
| Все Phase 2 best-combo anchors в C1 окне → C1 | ≥90% | 760/770 = 98.7% | OK |
| Anchor time_diff в [0, 60s] | 100% | min=1, max=59, median=29 | OK |
| Stratification report с per-stratum breakdown | ✅ | + cross-set safety, anchors, emoji, legacy | OK |
| Воспроизводимость (fixed seed) | ✅ | seed=42 hardcoded | OK |

**Sprint 4 / Commit 4.3 — закрыт ✅**

---

## Next: Sprint 4 / Commit 4.5

**Re-enrichment 8b × new prompt × C1 sample**

Цель: прогнать 30,518 C1-записей через `llama-3.1-8b-instant` с current production
prompt v1.0.0 → собрать enriched payloads → сохранить в parquet для 4.7 factorial.

Зависимости перед стартом:
- Скрипт `sprint4/reenrich/run_groq_batch.py` (~200 LOC, аналог Sprint 3 enricher без Redis)
- Budget approval: **~$7.1 реального cost**

Промпт берём как есть (`src/services/enricher/prompts/v1_0_0.md`). Никаких изменений
до 4.7 factorial — иначе сравнение распадётся.
