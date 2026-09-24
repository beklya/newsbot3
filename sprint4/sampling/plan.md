# Sprint 4 / Commit 4.2.b — Stratified Sampling Plan

**Статус:** план (для одобрения). Реализация — коммит 4.3.
**Зависимости:** 4.2.a discovery (860,626 events 2016-2026, 4 канала, TZ verified).
**Бюджет:** ~$29 на re-enrichment 8b + 70b через Groq Cloud.

---

## Цели

| ID | Назначение | n events | Окно | Где используется |
|----|------------|---------:|------|------------------|
| **C1** | Calibration sample для re-enrichment + factorial + per-bin calibration | 31,200 | 2025-04-01 → 2025-12-31 (9 мес) | 4.5, 4.6, 4.7, 4.8 |
| **V1** | Validation holdout — НЕ трогать до 4.9 | 8,000 | 2026-01-01 → 2026-04-30 (4 мес) | 4.9 OOS |

**Почему окно начинается 2025-04, а не 2022 (когда стартуют цены):**
- 2022-2024 покрыты Phase 2 backtest'ом — те результаты уже зафиксированы (Sharpe 4.98 на 2023-2026).
- 2025 — последний полный год до holdout. Дополнительные 2 года в C1 удвоили бы стоимость без существенного выигрыша по покрытию категорий/каналов.
- 2025-04 чтобы покрыть весеннюю активность ЦБ + летнюю просадку новостей + осенний пик.
- Сезонность по году уже подтверждена 4.2.a (max 2025-09 = 15.5k, min 2025-12 = 9.3k).

---

## Источники данных

| Путь | Размер | Содержание |
|------|-------:|-----------|
| `D:\quik_sber\newsbot\newsbot3\docs\legacy promt\telegram_news.jsonl` | 880 MB | Источник C1/V1 — full_text без обрезки, 860,626 records (= копия `D:\quik_sber\newsbot\duble3\telegram_news.jsonl`) |
| `D:\quik_sber\newsbot\newsbot3\docs\legacy promt\news_pool.jsonl` | 2.46 GB | Legacy 8b enrichment (qwen2.5/llama3.1 через Ollama) с price_moves. NOT-deduped, **включая off-session**. Используется как factorial baseline `(8b × legacy_prompt × prices)` в 4.7 |
| `D:\quik_sber\newsbot\newsbot2\решение проблем\news_pool_clean.jsonl` | — | Deduped + session-only subset news_pool.jsonl. На нём строилась Phase 2 strategy. **Неполный** — для cross-check, не как primary source |
| `D:\quik_sber\newsbot\newsbot2\решение проблем\проблема 1 - утечка\bucket_v2.py` | — | Leak-test: sentiment ↔ sign(realized_move) по бакетам \|move\|. Подтверждает, что legacy prompt с price_moves НЕ имеет look-ahead leak |
| `D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet` | — | 3,300 Phase 2 trades — источник anchor-новостей |
| `D:\quik_sber\newsbot\prices\prices_*.csv` × 19 | ~300 MB | Naive MSK, для 4.8 calibration горизонтов |

**Leak-status legacy промпта:** проверен в `newsbot2/решение проблем/проблема 1 - утечка/`. Hit-rate sentiment ↔ sign(move) в диапазоне честного edge (~55-65%), не 100%. → Legacy 8b × prices остаётся валидным baseline в 4.7 factorial без необходимости re-run без price_moves.

---

## Бюджет

| Phase | Cell | n events | Tokens/event | Cost |
|-------|------|---------:|-------------:|-----:|
| 4.7 baseline | 8b × legacy_prompt × prices | (existing) | — | **$0** (already enriched in news_pool.jsonl) |
| 4.5 | 8b × new_prompt × no-prices | 31,200 | 4,000 in + 400 out | **~$7.3** |
| 4.6 | 70b × winner_prompt × no-prices | 8,000 (random из C1) | 4,000 in + 400 out | **~$22** |
| **Итого** | | | | **~$29.3** |

Pricing assumptions (Groq Cloud, май 2026):
- llama-3.1-8b-instant: $0.05/M input + $0.08/M output
- llama-3.3-70b-versatile: $0.59/M input + $0.79/M output

Запас в плановом коридоре $25-35 → есть room на ±15% разброс по реальным token counts.

---

## Стратификация C1 (31,200 events)

### Three-tier структура

| Страта | Окно времени | n | Зачем |
|--------|-------------|--:|-------|
| **C1_session** | будни 10:00-23:50 MSK (main + evening MOEX session) | 22,000 | Основная калибровка price-move horizons (5m/15m/30m/60m/120m доступны напрямую) |
| **C1_offsession** | будни 00:00-09:59 MSK + выходные/праздники | 8,000 | Категориальная робастность; overnight horizon = return от close до open следующей сессии |
| **C1_anchor_force** | Phase 2 trades ±60s, force-include **поверх квот** | ~1,200 | Прямое сравнение нового enrichment'а со Sharpe 4.98 baseline на тех же новостях |

**Почему anchors поверх квот, не вместо:** Phase 2 trades в окне 2025-04→2025-12 ожидаемо ~1,000-1,500. Это 3-5% C1. Если вытеснять random sample, страты с высокой anchor-плотностью (геополитические дни 2025-02 для рублёвых пар, например) ломают канальный/месячный баланс. Лишние ~$0.28 на 8b за +1,200 events — приемлемо.

### Месячный баланс — равномерный, не пропорциональный

22k session events / 9 месяцев = **2,444/месяц** (округляем).
8k off-session events / 9 месяцев = **889/месяц**.

**Почему равномерный, а не natural-mix:**
- Natural distribution 2025-04 → 2025-12 неоднородна: 2025-09 = 15.5k vs 2025-11 = 9.9k → пропорциональный sample даст 50% разброса между бакетами.
- Для 4.8 per-month calibration нужна примерно равная статистическая мощность во всех бакетах.
- Месячный mix — это не источник bias, который нужно сохранять (в отличие от каналов, где есть behavioural difference).

### Канальный баланс — пропорциональный natural mix

Внутри каждого месячного бакета — natural mix по каналам (из 4.2.a):
- tass_agency: 41% → ~1,002/мес session + ~365/мес offsession
- rian_ru: 35% → ~855/мес session + ~311/мес offsession
- rbc_news: 16% → ~391/мес session + ~142/мес offsession
- interfaxonline: 8% → ~196/мес session + ~71/мес offsession

**Почему пропорционально, а не равномерно:**
- Каналы имеют разный editorial bias (tass = официоз, rbc = аналитика, interfax = breaking). LLM prompt должен оценивать на той пропорции, которая будет в проде.
- Минимальная страта (interfax × месяц × offsession ≈ 71 events) достаточна для категориальных counts; precision на хвостах — задача 4.8 top-up'ов.

### Время суток внутри session-страты

Без дополнительной разбивки. Внутри 10:00-23:50 распределение естественно: пик 11:00-14:00 (open + macro releases), вечером 19:00-21:00 (US session impact). LLM-качество vs время суток — не первичная гипотеза, не тратим страту.

### Дедупликация

- **Внутри C1:** убрать дубли по `SHA256(full_text)`. Discovery 4.2.a: 0.62% dup rate → ожидаемо ~190 дублей на 30k. Sampler ремплит при коллизии в той же страте.
- **C1 ↔ V1:** disjoint по `tg_msg_id` (Phase 2 anchor news не должны заехать в V1 — anchor logic привязана к 2025, V1 — 2026, естественно).
- **C1 ↔ news_pool.jsonl (legacy):** НЕ дедупим — нужны те же tg_msg_id в обеих enrichment, иначе 4.7 factorial разваливается. Вместо этого пишем `has_legacy_enrichment: bool` в каждую запись C1.

### Эмодзи / unicode

**Не санитизируем pre-prompt.** 23.4% записей (4.2.a) содержат эмодзи; "🗣 Пескова" — известный Groq 400 trigger (SPRINT3_DONE.md). Это **часть теста на робастность** обоих промптов и обеих моделей. Логируем счётчик 400-ответов отдельно в `stratification_report.txt`.

### Длина текста

Capped `min(len(full_text), 8000)` — соответствует контракту `RawNewsPayload.text max_length=10_000` минус headroom на prompt template + system. 32 записи с full_text > 4000 (из 4.2.a) — все попадают под cap.

---

## Стратификация V1 (8,000 events)

Та же логика, окно 2026-01-01 → 2026-04-30 (4 месяца):

| Страта | n | Per-month |
|--------|--:|----------:|
| V1_session | 5,500 | ~1,375/мес |
| V1_offsession | 2,000 | ~500/мес |
| V1_anchor_pad | 500 | (Phase 2 holdout trades если есть) |
| **Total** | **8,000** | |

**Без Phase 2 anchor force-include для V1** — Phase 2 не обучен на 2026, нет cross-comparison задачи. 500 anchor-pad оставлено на случай, если Sprint 5 будет генерировать новые backtests на V1 окне.

V1 **не enriched** в 4.5/4.6 — только сохранён как parquet с метаданными. Enrichment V1 — задача 4.9 (один раз, через winner модель × winner prompt).

---

## Sampling алгоритм (для 4.3)

```
1. Stream-read telegram_news.jsonl → polars LazyFrame
2. Filter window: 2025-04-01 ≤ datetime ≤ 2025-12-31 (UTC bound на timestamp)
3. Compute:
     - text_hash = SHA256(full_text)
     - has_emoji = bool (Unicode category 'So' check)
     - is_session = (weekday < 5) AND (10:00 ≤ MSK time ≤ 23:50)
     - stratum_month = datetime.month
     - stratum_channel = channel
4. Drop duplicate text_hash (keep first)
5. Load phase2_mfe_trades.parquet → extract (entry_time_utc, ticker, channel?)
6. For each trade in 2025-04..2025-12:
     find news with same channel AND timestamp ∈ [entry_time - 60s, entry_time]
     mark first match as anchor (one anchor per trade max)
7. C1_session sampling:
     for month in 4..12:
       for ch in [tass, rian, rbc, interfax]:
         target = round(2444 * channel_share[ch])
         pool = events[is_session AND stratum_month == month AND channel == ch]
         sample(pool, n=target, seed=42, without_replacement)
8. C1_offsession sampling — same loop, is_session=False, target=round(889 * share)
9. C1_anchor_force: union all anchor events; deduplicate with C1_session/offsession
   (anchors that landed in random sample naturally — don't double-count;
    anchors not in sample — add)
10. V1 sampling — analogous, 2026-01..2026-04, no anchor force, 500 pad slot
11. Write parquet + stratification_report.txt
```

**Seed:** `42`. Воспроизводимость критична для cross-team review.

---

## Comparison normalization для 4.7 (заметка)

Legacy и new промпты дают **разные output schemas** (детали — в моём предыдущем сообщении). Для factorial-сравнения в 4.7 нужен normalization layer:

```python
# Unified schema для 4.7 metrics
{
    "tg_msg_id": str,
    "model": "8b" | "70b",
    "prompt": "legacy" | "new",
    "with_prices": bool,
    # comparable fields:
    "tickers_predicted": set[str],   # legacy: {ticker} ∪ tickers_affected; new: {t.ticker for t in tickers}
    "direction_majority": "long"|"short"|"neutral",
    "confidence_avg": float,
    "category": str,
    "urgency": str,
    "is_actionable_proxy": bool,     # legacy: causal AND price_driven; new: is_actionable
    "is_financial_proxy": bool,      # legacy: ticker IS NOT NULL; new: is_financial
}
```

Sell-the-news (sentiment ≠ direction) — фича только new-промпта, в normalization теряется. **Это известное методологическое ограничение** 4.7 и должно быть зафиксировано в SPRINT4_COMMIT_4_7_DONE.md: factorial оценивает agreement на грубых метриках, sell-the-news edge — отдельная гипотеза, проверяется только на new-промпте.

Поля для capture в sample-parquet, которые этот normalization layer потом использует: `tg_msg_id`, `has_legacy_enrichment` (для join на `news_pool.jsonl`).

---

## Output артефакты 4.3

```
sprint4/sampling/
├── plan.md                              # этот документ (артефакт 4.2.b)
├── build_sample.py                      # sampling-скрипт (артефакт 4.3)
├── data/
│   ├── calibration_sample.parquet       # 31,200 × (см. schema ниже)
│   ├── validation_sample.parquet        # 8,000 × тот же schema
│   └── stratification_report.txt        # фактические counts vs план,
│                                        # χ² на (month × channel), overlap с news_pool.jsonl,
│                                        # emoji counts, anchor coverage
```

**Schema parquet:**

| Поле | Тип | Source |
|------|-----|--------|
| tg_msg_id | int64 | из jsonl |
| id | str | из jsonl (12-char hash) |
| channel | str (categorical) | из jsonl |
| datetime_msk | datetime | из jsonl `datetime` (MSK) |
| timestamp_utc | int64 | из jsonl `timestamp` |
| headline | str | из jsonl |
| full_text | str (capped 8000) | из jsonl |
| text_hash | str (64-char) | computed SHA256 |
| has_emoji | bool | computed |
| is_session | bool | computed (Mon-Fri AND 10:00-23:50 MSK) |
| stratum_month | int (4..12 для C1, 1..4 для V1) | computed |
| stratum_channel | str | = channel |
| has_phase2_anchor | bool | computed |
| phase2_trade_id | str \| null | computed |
| has_legacy_enrichment | bool | computed (existence в news_pool.jsonl) |
| sample_set | str ("C1" \| "V1") | constant per file |
| sample_seed | int | constant 42 |

---

## DoD коммита 4.2.b (этот документ)

| Критерий | Статус |
|----------|--------|
| Окна C1 и V1 зафиксированы с обоснованием | ✅ |
| Бюджет рассчитан, помещается в плановый коридор $25-35 | ✅ $29.3 |
| Страты определены (session/offsession/anchor) | ✅ |
| Месячный + канальный balance с rationale | ✅ |
| Dedup правила для C1/V1/legacy | ✅ |
| Sampling алгоритм описан на уровне псевдокода | ✅ |
| Output schema parquet полностью определена | ✅ |
| Comparison normalization для 4.7 учтена в выборе capture fields | ✅ |
| Leak-status legacy промпта явно зафиксирован | ✅ ссылка на bucket_v2.py |

---

## Followups (не блокеры 4.2.b)

1. **70b targeted top-up.** Если 4.6 покажет узкое CI на cbr (~150 events → ±8% hit-rate), доп. sample 300-500 cbr-only events ($1-2). Решение по результатам 4.6.
2. **Off-session calibration horizon.** Для C1_offsession нет 5m/15m цены — единственный horizon = `open_next_session`. В 4.8 это отдельный калибровочный bucket, не объединять с session-метриками.
3. **Sell-the-news edge.** Не покрывается factorial-метрикой (legacy её не знает). В 4.7 как отдельная under-evaluation секция: precision/recall sell-the-news на C1 только для new-промпта; компарить с realized sign(move) после события.
4. **Emoji 400 fix.** Если 4.5 покажет >2% Groq 400 на эмодзи — добавить sanitization layer перед промптом (`unicodedata.normalize` + strip Cat:So) в src/services/enricher/. Sprint backlog, не 4.x.

---

## Готовность к 4.3

План закрыт. Следующий шаг — `build_sample.py` (4.3) запустить sampling, проверить `stratification_report.txt`, после approval — стартовать 4.5 re-enrichment.

Команда запуска (после 4.3 написания):
```powershell
cd D:\quik_sber\newsbot\newsbot3
.venv\Scripts\activate.bat
python sprint4\sampling\build_sample.py --seed 42 --output sprint4\sampling\data
```

Ожидаемое время: < 5 мин на full 880MB stream-pass + parquet write.
