"""
ollama_analyzer.py — Анализ новостей через локальную Ollama (Qwen2.5:7b)
=========================================================================
Бесплатная альтернатива Anthropic Batch API.
Требует запущенный Ollama сервер с моделью qwen2.5:7b.

Запуск Ollama сервера (в отдельном окне):
  A:\\newsbot\\ollama-windows-amd64\\ollama.exe serve

Запуск анализа:
  python ollama_analyzer.py              — обработать всё
  python ollama_analyzer.py --limit 100  — тест на 100 записях
  python ollama_analyzer.py --workers 4  — 4 параллельных потока

Скорость: ~2-4 сек/запись × 333к = ~250-500 часов на 1 потоке
С 4 потоками: ~60-120 часов
"""

import os, sys, json, re, time, logging, argparse, platform
import threading
from pathlib import Path
from datetime import datetime
from queue import Queue
import requests

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

if platform.system() == "Windows" and os.path.exists(r"A:\newsbot\news"):
    BASE_DIR = r"A:\newsbot\news"
elif platform.system() == "Windows" and os.path.exists(r"D:\quik_sber\newsbot\api"):
    BASE_DIR = r"D:\quik_sber\newsbot\api\LLM"
else:
    BASE_DIR = r"C:\newsbot"

OLLAMA_URL    = "http://localhost:11434/api/generate"
OLLAMA_MODEL  = "llama3.1:8b"

ENRICHED_FILE   = os.path.join(BASE_DIR, "enriched_news_full.jsonl")
NEWS_POOL       = os.path.join(BASE_DIR, "news_pool.jsonl")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "ollama_checkpoint.json")
LOG_FILE        = os.path.join(BASE_DIR, "ollama_analyzer.log")

SAVE_EVERY   = 50    # сохранять checkpoint каждые N записей
NUM_WORKERS  = 4     # параллельных потоков (RTX 2080 потянет 2-4)

ALLOWED_TICKERS = {
    "SBER","GAZP","LKOH","YNDX","ROSN","GMKN","NVTK","TATN",
    "MGNT","MTSS","PLZL","VTBR","Si","MX","BR","NG","GOLD","CNY","USDRUB"
}

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ============================================================
# ПРОМПТ (короткий — для экономии времени)
# ============================================================

SYSTEM_PROMPT = """Ты финансовый аналитик MOEX. Отвечай ТОЛЬКО валидным JSON без markdown и пояснений.

Формат:
{"sentiment":"bullish"|"bearish"|"neutral","confidence":0-100,"ticker":"SBER"|"GAZP"|"LKOH"|"YNDX"|"ROSN"|"GMKN"|"NVTK"|"TATN"|"MGNT"|"MTSS"|"PLZL"|"VTBR"|"Si"|"MX"|"BR"|"NG"|"GOLD"|"CNY"|"USDRUB"|null,"tickers_affected":[],"asset_class":"equity"|"futures"|"commodity"|"currency"|"market"|null,"urgency":"high"|"medium"|"low","category":"geopolitics"|"macro"|"cbr"|"corporate"|"commodity"|"currency"|"other","reason":"3-5 слов","price_driven":true|false,"causal":true|false}

Правила выбора ticker (ГЛАВНЫЙ инструмент):
- Новость о конкретной компании → ticker этой компании
- ЦБ поднял ставку → ticker="Si", tickers_affected=["Si","MX","SBER","VTBR"]
- ЦБ снизил ставку → ticker="MX", tickers_affected=["MX","SBER","VTBR","Si"]
- Санкции против России → ticker="Si", tickers_affected=["Si","MX","GAZP","SBER"]
- Санкции против Газпрома → ticker="GAZP", tickers_affected=["GAZP","NVTK","MX"]
- Рост нефти → ticker="BR", tickers_affected=["BR","LKOH","ROSN","NVTK"]
- Геополитика (война, переговоры) → ticker="MX", tickers_affected=["MX","Si","SBER","GAZP"]
- Курс доллара/рубля → ticker="Si" или "USDRUB"
- Юань → ticker="CNY"
- ticker=null только если новость не имеет отношения к финансам

Правила sentiment:
- confidence < 60 → sentiment = "neutral"
- ЦБ ставка вверх → bearish MX, bullish Si, category="cbr", urgency="high"
- ЦБ ставка вниз → bullish MX, bearish Si, category="cbr", urgency="high"
- Дивиденды выше ожиданий → bullish ticker акции
- Конкретные цифры в тексте → повышай confidence на 15-20

Правила при наличии данных о ценах:
- ticker = инструмент с НАИБОЛЬШИМ абсолютным движением за 15м логично связанный с новостью
- tickers_affected = все инструменты отреагировавшие > 0.3% за 15м
- НИКОГДА ticker=null если хотя бы один инструмент вырос/упал > 0.5%
- confidence: движение>2%→85+, 1-2%→70, 0.5-1%→55, <0.5%→40
- urgency=high если движение > 1.5% за 15м
- price_driven=true если движение явно связано с новостью
- causal=true если новость ПРИЧИНА движения (не репортаж об уже случившемся)"""

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def build_prompt(rec):
    """Формирует текст запроса для одной записи"""
    moves  = rec.get("price_moves", {})
    top3   = sorted(moves.items(),
                    key=lambda x: abs(x[1].get("15m", 0)),
                    reverse=True)[:3]
    moves_str = " | ".join(
        f"{t}: {d.get('15m',0):+.2f}%(15m) {d.get('60m',0):+.2f}%(1h) {d.get('1d',0):+.2f}%(1d)"
        for t, d in top3
    )
    text  = (rec.get("full_text") or "")[:300].strip()
    parts = [f"Headline: {rec['headline']}"]
    if text and text != rec['headline']:
        parts.append(f"Text: {text[:200]}")
    if moves_str:
        parts.append(f"Price moves: {moves_str}")
    return "\n".join(parts)


def parse_response(text):
    """Извлекает JSON из ответа модели — устойчиво к мусору"""
    text = text.strip()

    # Убираем markdown
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass

    # Ищем первый полный JSON объект
    m2 = re.search(r"\{.*?\}", text, re.DOTALL)
    if m2:
        try:
            return json.loads(m2.group())
        except Exception:
            pass

    # Пробуем починить обрезанный JSON — добавляем закрывающие скобки
    for suffix in ["}", '"}', '"}}', '"}}}']:
        try:
            return json.loads(text + suffix)
        except Exception:
            pass

    # Пробуем напрямую
    return json.loads(text)


def validate_analysis(a):
    """Валидирует и нормализует ответ LLM"""
    a.setdefault("sentiment",        "neutral")
    a.setdefault("confidence",       0)
    a.setdefault("ticker",           None)
    a.setdefault("tickers_affected", [])
    a.setdefault("asset_class",      None)
    a.setdefault("urgency",          "low")
    a.setdefault("category",         "other")
    a.setdefault("reason",           "")
    a.setdefault("price_driven",     False)
    a.setdefault("causal",           False)

    if a["ticker"] and a["ticker"] not in ALLOWED_TICKERS:
        a["ticker"]     = None
        a["confidence"] = min(a["confidence"], 50)
    a["tickers_affected"] = [
        t for t in a.get("tickers_affected", [])
        if t in ALLOWED_TICKERS
    ]
    return a


def analyze_ollama(rec):
    """Отправляет запрос в локальную Ollama"""
    prompt = build_prompt(rec)
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model":   OLLAMA_MODEL,
                "prompt":  prompt,
                "system":  SYSTEM_PROMPT,
                "stream":  False,
                "format":  "json",
                "options": {
                    "temperature": 0.1,
                    "num_predict": 400,
                }
            },
            timeout=60
        )
        resp.raise_for_status()
        raw = resp.json()["response"]
        analysis = parse_response(raw)
        return validate_analysis(analysis)
    except json.JSONDecodeError as e:
        log.debug(f"JSON parse error: {e} | raw: {raw[:100]}")
        return validate_analysis({})
    except Exception:
        raise

# ============================================================
# CHECKPOINT
# ============================================================

_checkpoint_lock = threading.Lock()

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE) as f:
            return set(json.load(f).get("done_ids", []))
    except Exception:
        return set()


def save_checkpoint(done_ids):
    with _checkpoint_lock:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"done_ids": list(done_ids),
                       "updated_at": datetime.now().isoformat()}, f)


def load_pool_ids():
    seen = set()
    if Path(NEWS_POOL).exists():
        with open(NEWS_POOL, encoding="utf-8") as f:
            for line in f:
                try:
                    seen.add(json.loads(line)["id"])
                except Exception:
                    pass
    return seen

# ============================================================
# ВОРКЕР
# ============================================================

_write_lock = threading.Lock()
_stats      = {"done": 0, "signals": 0, "errors": 0}
_done_ids   = set()


def worker(q, out_fh):
    """Поток-воркер: читает из очереди, анализирует, пишет результат"""
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            break

        rec = item
        try:
            analysis = analyze_ollama(rec)
            record   = {
                "id":           rec.get("id",""),
                "datetime":     rec.get("datetime",""),
                "headline":     rec.get("headline",""),
                "channel":      rec.get("channel",""),
                "price_moves":  rec.get("price_moves",{}),
                "max_mover":    rec.get("max_mover",""),
                "max_move_15m": rec.get("max_move_15m",0),
                "analysis":     analysis
            }

            with _write_lock:
                out_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_fh.flush()
                _done_ids.add(rec["id"])
                _stats["done"] += 1
                if (analysis["sentiment"] != "neutral"
                        and analysis["confidence"] >= 60):
                    _stats["signals"] += 1

                # Прогресс каждые 10 записей
                if _stats["done"] % 10 == 0:
                    log.info(
                        f"  [{_stats['done']:>6,}] "
                        f"сигналов={_stats['signals']:,} "
                        f"ошибок={_stats['errors']:,} | "
                        f"{rec.get('datetime','')[:16]} "
                        f"{analysis['sentiment']:8s} {analysis['confidence']:3d}% "
                        f"{analysis.get('ticker') or '—':8s} | "
                        f"{rec.get('headline','')[:50]}"
                    )

                # Checkpoint
                if _stats["done"] % SAVE_EVERY == 0:
                    save_checkpoint(_done_ids)

        except Exception as e:
            with _write_lock:
                _stats["errors"] += 1
                log.warning(f"  Ошибка [{rec.get('id','')}]: {e}")

        q.task_done()

# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def run(args):
    log.info("=" * 60)
    log.info("🦙 Ollama Local Analyzer")
    log.info(f"   Модель:   {OLLAMA_MODEL}")
    log.info(f"   Источник: {ENRICHED_FILE}")
    log.info(f"   Пул:      {NEWS_POOL}")
    log.info(f"   Потоков:  {args.workers}")
    log.info("=" * 60)

    # Проверяем Ollama
    try:
        r = requests.get("http://localhost:11434", timeout=5)
        log.info("✅ Ollama доступна")
    except Exception:
        log.error("❌ Ollama не запущена!")
        log.error("   Запусти: ollama.exe serve")
        sys.exit(1)

    # Загружаем уже обработанные ID
    done_ids = load_checkpoint() | load_pool_ids()
    _done_ids.update(done_ids)
    log.info(f"   Уже обработано: {len(done_ids):,} — пропускаем")

    # Читаем записи для обработки
    log.info("Загрузка записей...")
    records = []
    with open(ENRICHED_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
                if r.get("headline") and r.get("id") not in done_ids:
                    records.append(r)
            except Exception:
                pass

    if args.limit:
        records = records[:args.limit]

    total = len(records)
    log.info(f"   К обработке: {total:,}")

    # Оценка времени
    secs_per_rec = 3.0  # ~3 сек на RTX 2080
    hours = total * secs_per_rec / args.workers / 3600
    log.info(f"   Ожидаемое время: ~{hours:.1f} часов ({args.workers} потоков)")
    log.info("   Ctrl+C для паузы (прогресс сохранится)")
    time.sleep(2)

    # Запускаем воркеры
    q = Queue(maxsize=args.workers * 4)
    out_fh = open(NEWS_POOL, "a", encoding="utf-8")

    threads = []
    for _ in range(args.workers):
        t = threading.Thread(target=worker, args=(q, out_fh), daemon=True)
        t.start()
        threads.append(t)

    try:
        for rec in records:
            q.put(rec)
        q.join()
    except KeyboardInterrupt:
        log.info("\n⏸ Остановлено. Сохраняем прогресс...")
    finally:
        # Останавливаем воркеры
        for _ in threads:
            q.put(None)
        for t in threads:
            t.join(timeout=5)
        out_fh.close()
        save_checkpoint(_done_ids)

    log.info("")
    log.info("=" * 60)
    log.info(f"✅ Завершено!")
    log.info(f"   Обработано: {_stats['done']:,}")
    log.info(f"   Сигналов:   {_stats['signals']:,}")
    log.info(f"   Ошибок:     {_stats['errors']:,}")

    # Итог пула
    try:
        pool_size = sum(1 for _ in open(NEWS_POOL, encoding="utf-8"))
        log.info(f"   Пул всего:  {pool_size:,} записей")
    except Exception:
        pass

    log.info(f"\n   Следующий шаг: py backtester.py")


def main():
    parser = argparse.ArgumentParser(description="Анализ новостей через локальную Ollama")
    parser.add_argument("--limit",   type=int, default=None,
                        help="Обработать только первые N записей (тест)")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS,
                        help=f"Кол-во параллельных потоков (default: {NUM_WORKERS})")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
