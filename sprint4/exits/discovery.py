"""
Sprint 4 / Commit 4.0 — Шаг 1: Discovery
==========================================

Цель:
  Профилировать исходные данные ПЕРЕД написанием симулятора exit-стратегий.
  Закрыть открытые вопросы по TZ, naming, покрытию, распределениям.

Что делает:
  1. Загружает phase2_mfe_trades.parquet — инспектирует колонки, dtypes, ranges
  2. Загружает 1-2 файла prices_*.csv — проверяет TZ, формат, монотонность
  3. Сверяет тикеры из trades ↔ prices/ (несоответствия = бомба замедленного действия)
  4. Считает распределения exit_reason / per ticker / per year — sanity для Phase 2 чисел
  5. Подтверждает (или опровергает) ассумпции про pred_mfe_pct в %
  6. Печатает summary + рекомендации перед написанием simulator.py

Запуск:
  cd D:\quik_sber\newsbot\newsbot3\sprint4\exits
  python discovery.py

Выход:
  - stdout: structured report
  - data/discovery_report.json — машиночитаемый отчёт для последующих скриптов
  - data/discovery_<timestamp>.log — копия stdout
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


# =============================================================================
# Конфигурация путей
# =============================================================================
TRADES_PATH = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)

PRICES_DIR = Path(r"D:\quik_sber\newsbot\prices")

# Зафиксированный whitelist v1.1.0 ↔ имена CSV-файлов.
# Источник истины: будет вынесено в src/contracts/instruments.py в коммите 4.1.
INSTRUMENT_FILE_MAP: dict[str, str] = {
    "SBER":   "prices_SBER.csv",
    "GAZP":   "prices_GAZP.csv",
    "LKOH":   "prices_LKOH.csv",
    "YDEX":   "prices_YDEX.csv",     # legacy: YNDX → YDEX после reorganization 2024
    "ROSN":   "prices_ROSN.csv",
    "GMKN":   "prices_GMKN.csv",
    "NVTK":   "prices_NVTK.csv",
    "TATN":   "prices_TATN.csv",
    "MGNT":   "prices_MGNT.csv",
    "MTSS":   "prices_MTSS.csv",
    "PLZL":   "prices_PLZL.csv",
    "VTBR":   "prices_VTBR.csv",
    "MIX":    "prices_MIX.csv",      # legacy: MX → MIX
    "SI":     "prices_SI.csv",
    "BR":     "prices_BR.csv",
    "NG":     "prices_NG.csv",
    "GLDRUB": "prices_GLDRUB.csv",   # legacy: GOLD → GLDRUB, доступен с 2023-07-12
    "CNY":    "prices_CNY.csv",
    "USDRUB": "prices_USDRUB.csv",
}

# Папка вывода артефактов
OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)


# =============================================================================
# Helpers для печати
# =============================================================================
def section(title: str) -> None:
    """Заголовок секции в stdout (для читаемого лога)."""
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def subsection(title: str) -> None:
    print()
    print(f"--- {title} ---")


# =============================================================================
# Часть 1: TRADES PARQUET
# =============================================================================
def inspect_trades(path: Path) -> dict[str, Any]:
    """
    Загружает phase2_mfe_trades.parquet и возвращает структурированный отчёт.

    Что проверяем:
      - Файл существует и читается
      - Ожидаемые колонки присутствуют: ts_open, ts_close, ticker, side,
        entry, exit, pred_mfe_pct, pred_mae_pct, exit_reason, duration_min, pnl_rub
      - Дополнительные колонки (если есть) — фиксируем для дальнейшего использования
      - dtype колонок (особенно ts_open/ts_close — должны быть datetime)
      - Ranges значимых колонок: pred_mfe_pct, pred_mae_pct (подтвердить, что в %)
      - Распределение exit_reason — ожидаем ~40% tp / 35% sl / 25% time
      - Распределение per ticker — какие имена реально использовались
      - Распределение per year — для оценки покрытия годами
      - TZ-info для ts_open / ts_close
    """
    report: dict[str, Any] = {"path": str(path)}

    if not path.exists():
        report["error"] = f"File not found: {path}"
        print(f"[ERR] {report['error']}")
        return report

    print(f"[INFO] Loading {path.name} ...")
    df = pd.read_parquet(path)
    report["n_rows"] = len(df)
    report["n_cols"] = len(df.columns)

    print(f"  rows: {len(df):,}")
    print(f"  cols: {len(df.columns)}")

    # ── Колонки и dtypes ────────────────────────────────────────────────────
    subsection("Columns & dtypes")
    cols_info = {}
    for c in df.columns:
        dtype_str = str(df[c].dtype)
        n_null = int(df[c].isna().sum())
        cols_info[c] = {"dtype": dtype_str, "n_null": n_null}
        print(f"  {c:<25s} {dtype_str:<25s} nulls={n_null}")
    report["columns"] = cols_info

    # Ожидаемые колонки (из ТЗ + exits_analysis_phase2.txt)
    expected = {
        "ts_open", "ts_close", "ticker", "side", "entry", "exit",
        "pred_mfe_pct", "pred_mae_pct", "exit_reason", "duration_min",
    }
    missing = expected - set(df.columns)
    extra = set(df.columns) - expected
    if missing:
        print(f"  [WARN] missing expected columns: {sorted(missing)}")
    if extra:
        print(f"  [INFO] extra columns (not in expected set): {sorted(extra)}")
    report["missing_expected_cols"] = sorted(missing)
    report["extra_cols"] = sorted(extra)

    # ── Timestamps TZ-info ──────────────────────────────────────────────────
    subsection("Timestamps TZ-info")
    for c in ("ts_open", "ts_close"):
        if c not in df.columns:
            continue
        col = df[c]
        if pd.api.types.is_datetime64_any_dtype(col):
            tz = col.dt.tz
            tz_str = str(tz) if tz is not None else "naive (no tz)"
            print(f"  {c}: dtype={col.dtype}, tz={tz_str}")
            print(f"    min={col.min()}  max={col.max()}")
            report.setdefault("timestamps", {})[c] = {
                "dtype": str(col.dtype),
                "tz": tz_str,
                "min": str(col.min()),
                "max": str(col.max()),
            }
        else:
            print(f"  [WARN] {c} is not datetime, dtype={col.dtype}")

    # ── pred_mfe_pct / pred_mae_pct — проверка ассумпции "в процентах" ──────
    subsection("pred_mfe_pct / pred_mae_pct — sanity (ожидаем значения в %, типичный 0.1-3.0)")
    for c in ("pred_mfe_pct", "pred_mae_pct"):
        if c not in df.columns:
            continue
        s = df[c].dropna()
        stats = {
            "min": float(s.min()),
            "p01": float(s.quantile(0.01)),
            "p50": float(s.quantile(0.50)),
            "p95": float(s.quantile(0.95)),
            "p99": float(s.quantile(0.99)),
            "max": float(s.max()),
        }
        report.setdefault("pred_stats", {})[c] = stats
        print(f"  {c}:")
        for k, v in stats.items():
            print(f"    {k:<5s} {v:>10.4f}")
        # Sanity: если p50 < 0.01 → это похоже на доли, а не %
        if stats["p50"] < 0.01:
            print(f"  [WARN] {c}.p50 = {stats['p50']:.5f} — это похоже на доли, не на %!")
        elif stats["p50"] > 10:
            print(f"  [WARN] {c}.p50 = {stats['p50']:.2f} — подозрительно высоко для %")
        else:
            print(f"  [OK] значения соответствуют формату %  (p50={stats['p50']:.3f}%)")

    # ── exit_reason distribution ────────────────────────────────────────────
    subsection("exit_reason distribution (ожидаем ~40% tp / 35% sl / 25% time)")
    if "exit_reason" in df.columns:
        vc = df["exit_reason"].value_counts(dropna=False)
        total = len(df)
        dist = {}
        for k, v in vc.items():
            pct = 100.0 * v / total
            dist[str(k)] = {"count": int(v), "pct": round(pct, 2)}
            print(f"  {str(k):<10s} {v:>8,}  ({pct:>5.1f}%)")
        report["exit_reason_dist"] = dist
    else:
        print("  [WARN] exit_reason column missing")

    # ── Tickers ──────────────────────────────────────────────────────────────
    subsection("Tickers in trades")
    if "ticker" in df.columns:
        vc = df["ticker"].value_counts(dropna=False)
        tickers_in_trades = {}
        for k, v in vc.items():
            pct = 100.0 * v / len(df)
            tickers_in_trades[str(k)] = {"count": int(v), "pct": round(pct, 2)}
            print(f"  {str(k):<10s} {v:>8,}  ({pct:>5.1f}%)")
        report["tickers_in_trades"] = tickers_in_trades
    else:
        print("  [WARN] ticker column missing")

    # ── Years ────────────────────────────────────────────────────────────────
    subsection("Years coverage (по ts_open)")
    if "ts_open" in df.columns and pd.api.types.is_datetime64_any_dtype(df["ts_open"]):
        years = df["ts_open"].dt.year.value_counts(dropna=False).sort_index()
        years_dict = {}
        for y, v in years.items():
            pct = 100.0 * v / len(df)
            years_dict[int(y)] = {"count": int(v), "pct": round(pct, 2)}
            print(f"  {y}    {v:>8,}  ({pct:>5.1f}%)")
        report["years"] = years_dict

    # ── Side ────────────────────────────────────────────────────────────────
    if "side" in df.columns:
        subsection("Side distribution")
        vc = df["side"].value_counts(dropna=False)
        side_dict = {}
        for k, v in vc.items():
            side_dict[str(k)] = int(v)
            print(f"  {str(k):<10s} {v:>8,}")
        report["side_dist"] = side_dict

    # ── duration_min ────────────────────────────────────────────────────────
    if "duration_min" in df.columns:
        subsection("duration_min stats (ожидаем 1-60 для Phase 2 horizon range)")
        s = df["duration_min"].dropna()
        dur_stats = {
            "min": float(s.min()),
            "p50": float(s.quantile(0.50)),
            "p95": float(s.quantile(0.95)),
            "max": float(s.max()),
        }
        report["duration_stats"] = dur_stats
        for k, v in dur_stats.items():
            print(f"  {k:<5s} {v:>8.2f}")

    return report


# =============================================================================
# Часть 2: PRICES CSV (TZ verification + format)
# =============================================================================
def inspect_prices_csv(ticker: str, csv_path: Path) -> dict[str, Any]:
    """
    Инспектирует один prices CSV: TZ, формат datetime-колонки, монотонность,
    шаг (должен быть 1 минута), range дат.

    Не загружает весь файл — берём head(1000) + tail(1000), этого достаточно
    для TZ-вердикта.
    """
    report: dict[str, Any] = {"ticker": ticker, "path": str(csv_path)}

    if not csv_path.exists():
        report["error"] = "file_not_found"
        print(f"  [ERR] {csv_path.name} — not found")
        return report

    file_size_mb = csv_path.stat().st_size / 1024 / 1024
    report["file_size_mb"] = round(file_size_mb, 1)
    print(f"  [{ticker}] {csv_path.name}  ({file_size_mb:.1f} MB)")

    # Читаем только заголовок + первые/последние строки
    head = pd.read_csv(csv_path, nrows=5)
    print(f"    columns: {list(head.columns)}")
    print(f"    head sample:")
    for _, row in head.head(2).iterrows():
        print(f"      {dict(row)}")

    report["columns"] = list(head.columns)

    # Догадываемся, какая колонка — datetime
    # Возможные имена: datetime, date, ts, timestamp, time, Date, DateTime
    dt_col_candidates = [c for c in head.columns if c.lower() in {
        "datetime", "date", "ts", "timestamp", "time"
    }]
    if not dt_col_candidates:
        # Возможно, datetime разбит на 2 колонки (Date + Time)
        if "<DATE>" in head.columns and "<TIME>" in head.columns:
            dt_col_candidates = ["<DATE>+<TIME>"]
        else:
            print(f"    [WARN] не нашли datetime колонку среди {list(head.columns)}")
            report["datetime_col"] = None
            return report

    dt_col = dt_col_candidates[0]
    report["datetime_col"] = dt_col
    print(f"    datetime column: {dt_col}")

    # TZ-info: смотрим формат первого значения
    if dt_col != "<DATE>+<TIME>":
        sample_val = str(head[dt_col].iloc[0])
        print(f"    sample datetime value: {sample_val!r}")
        # Эвристики
        has_tz_marker = any(m in sample_val for m in ["+", "Z", "UTC", "MSK"])
        report["tz_marker_in_string"] = has_tz_marker
        if has_tz_marker:
            print(f"    [INFO] TZ-маркер найден в строке → есть явная TZ")
        else:
            print(f"    [INFO] TZ-маркер НЕ найден → naive datetime (MSK?)")

    # Читаем все строки для расчёта min/max date и шага
    # (для CSV ~70 MB это 5-10 сек — приемлемо в discovery)
    try:
        if dt_col != "<DATE>+<TIME>":
            full = pd.read_csv(csv_path, usecols=[dt_col])
            full[dt_col] = pd.to_datetime(full[dt_col], errors="coerce")
            full = full.dropna()
            dt_series = full[dt_col]
        else:
            full = pd.read_csv(csv_path, usecols=["<DATE>", "<TIME>"])
            full["dt"] = pd.to_datetime(
                full["<DATE>"].astype(str) + " " + full["<TIME>"].astype(str),
                errors="coerce",
            )
            full = full.dropna()
            dt_series = full["dt"]

        report["n_rows_total"] = len(full)
        if len(dt_series) >= 2:
            dt_min = dt_series.iloc[0]
            dt_max = dt_series.iloc[-1]
            print(f"    date range: {dt_min}  →  {dt_max}")
            report["date_min"] = str(dt_min)
            report["date_max"] = str(dt_max)

            # Монотонность
            mono = dt_series.is_monotonic_increasing
            report["is_monotonic_increasing"] = bool(mono)
            print(f"    monotonic increasing: {mono}")

            # Шаг между соседними барами
            diffs = dt_series.diff().dropna()
            vc_step = diffs.value_counts().head(3)
            print(f"    top-3 bar steps:")
            for step, count in vc_step.items():
                pct = 100.0 * count / len(diffs)
                print(f"      {str(step):<20s} {count:>8,}  ({pct:.1f}%)")
            report["top_step"] = str(vc_step.index[0])
    except Exception as e:
        print(f"    [ERR] failed to compute date range: {e}")
        report["error_full_scan"] = str(e)

    return report


def inspect_all_prices(prices_dir: Path, sample_n: int = 3) -> dict[str, Any]:
    """
    Инспектирует sample_n CSV-файлов из prices/ + проверяет наличие всех остальных.

    sample_n=3 достаточно для подтверждения TZ-конвенции (если у одного MSK,
    у других почти наверняка тоже MSK — все экспортированы одним инструментом).
    """
    print(f"[INFO] Prices directory: {prices_dir}")
    report: dict[str, Any] = {"prices_dir": str(prices_dir)}

    if not prices_dir.exists():
        report["error"] = "prices_dir_not_found"
        print(f"[ERR] {prices_dir} does not exist")
        return report

    files_present = {p.name for p in prices_dir.glob("prices_*.csv")}
    report["files_present"] = sorted(files_present)

    # Покрытие: какие из whitelist есть, каких не хватает
    coverage: dict[str, dict[str, Any]] = {}
    for ticker, fname in INSTRUMENT_FILE_MAP.items():
        coverage[ticker] = {
            "expected_file": fname,
            "present": fname in files_present,
        }
    n_present = sum(1 for v in coverage.values() if v["present"])
    print(f"[INFO] coverage: {n_present}/{len(INSTRUMENT_FILE_MAP)} tickers have CSV")
    for t, info in coverage.items():
        marker = "✓" if info["present"] else "✗"
        print(f"  {marker} {t:<8s} {info['expected_file']}")
    report["coverage"] = coverage

    # Inspect sample_n random tickers (берём детерминированно: первые n присутствующих)
    section("Sample CSV inspection (TZ + format)")
    sample_tickers = [t for t, info in coverage.items() if info["present"]][:sample_n]
    sample_reports: dict[str, Any] = {}
    for t in sample_tickers:
        path = prices_dir / coverage[t]["expected_file"]
        sample_reports[t] = inspect_prices_csv(t, path)
    report["sample_inspections"] = sample_reports

    return report


# =============================================================================
# Часть 3: Сверка tickers trades ↔ prices/
# =============================================================================
def cross_check_tickers(trades_report: dict, prices_report: dict) -> dict[str, Any]:
    """
    Главный sanity-check: какие тикеры из trades.parquet НЕ имеют файла в prices/,
    и какие файлы в prices/ не используются.

    Это критично — если в trades есть YNDX, а в prices/ только prices_YDEX.csv,
    то симулятор упадёт на этих сделках. Лучше узнать сейчас.
    """
    section("Cross-check: trades tickers ↔ prices files")
    out: dict[str, Any] = {}

    trades_tickers = set(trades_report.get("tickers_in_trades", {}).keys())
    coverage = prices_report.get("coverage", {})
    whitelist_tickers = set(coverage.keys())

    # Тикеры в trades, но НЕ в whitelist (т.е. для них нет mapping вообще)
    in_trades_not_in_whitelist = trades_tickers - whitelist_tickers
    # Тикеры в whitelist, но НЕ в trades (CSV есть, но Phase 2 их не торговал)
    in_whitelist_not_in_trades = whitelist_tickers - trades_tickers
    # Тикеры в trades, в whitelist, но без CSV-файла
    in_trades_no_csv = {
        t for t in (trades_tickers & whitelist_tickers)
        if not coverage[t]["present"]
    }
    # Полностью покрытые
    fully_covered = {
        t for t in (trades_tickers & whitelist_tickers)
        if coverage[t]["present"]
    }

    print(f"  ✓ fully covered:        {len(fully_covered):>3}  {sorted(fully_covered)}")
    print(f"  ⚠ in trades, no CSV:    {len(in_trades_no_csv):>3}  {sorted(in_trades_no_csv)}")
    print(f"  ⚠ in trades, no map:    {len(in_trades_not_in_whitelist):>3}  {sorted(in_trades_not_in_whitelist)}")
    print(f"  ℹ in whitelist, no trd: {len(in_whitelist_not_in_trades):>3}  {sorted(in_whitelist_not_in_trades)}")

    out["fully_covered"] = sorted(fully_covered)
    out["in_trades_no_csv"] = sorted(in_trades_no_csv)
    out["in_trades_not_in_whitelist"] = sorted(in_trades_not_in_whitelist)
    out["in_whitelist_not_in_trades"] = sorted(in_whitelist_not_in_trades)

    # КРИТИЧЕСКАЯ проверка
    blocker = in_trades_not_in_whitelist | in_trades_no_csv
    if blocker:
        # Подсчитаем, какая доля сделок страдает
        affected_pct = sum(
            trades_report["tickers_in_trades"][t]["pct"] for t in blocker
            if t in trades_report["tickers_in_trades"]
        )
        print(f"  [BLOCKER] {len(blocker)} tickers без CSV — затронуто {affected_pct:.1f}% сделок")
        out["blocker_affected_pct"] = round(affected_pct, 2)
    else:
        print(f"  [OK] все тикеры из trades имеют CSV — путь к simulator открыт")
        out["blocker_affected_pct"] = 0.0

    return out


# =============================================================================
# Главный orchestrator
# =============================================================================
def main() -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    print(f"Discovery started at: {started_at}")
    print(f"Python: {sys.version}")
    print(f"pandas: {pd.__version__}")

    report: dict[str, Any] = {
        "started_at": started_at,
        "trades_path": str(TRADES_PATH),
        "prices_dir": str(PRICES_DIR),
    }

    section("PART 1 / TRADES PARQUET")
    report["trades"] = inspect_trades(TRADES_PATH)

    section("PART 2 / PRICES CSV (sample inspection)")
    report["prices"] = inspect_all_prices(PRICES_DIR, sample_n=3)

    section("PART 3 / CROSS-CHECK TICKERS")
    report["cross_check"] = cross_check_tickers(report["trades"], report["prices"])

    # ── Финальные выводы ────────────────────────────────────────────────────
    section("DISCOVERY SUMMARY — что мы узнали")

    findings: list[str] = []

    # F1: формат pred_mfe_pct
    pred_stats = report["trades"].get("pred_stats", {})
    if "pred_mfe_pct" in pred_stats:
        p50 = pred_stats["pred_mfe_pct"]["p50"]
        if 0.05 < p50 < 5.0:
            findings.append(f"✓ pred_mfe_pct в формате %, p50={p50:.3f}% — формула (pred/100) корректна")
        else:
            findings.append(f"⚠ pred_mfe_pct p50={p50:.5f} — неоднозначно, проверь формулу!")

    # F2: TZ trades
    ts_info = report["trades"].get("timestamps", {}).get("ts_open", {})
    if ts_info:
        findings.append(f"✓ ts_open dtype={ts_info.get('dtype')} tz={ts_info.get('tz')}")

    # F3: TZ prices
    sample_inspections = report["prices"].get("sample_inspections", {})
    if sample_inspections:
        tz_findings = []
        for t, info in sample_inspections.items():
            has_tz = info.get("tz_marker_in_string", None)
            tz_findings.append(f"{t}: tz_marker={has_tz}")
        findings.append(f"  prices TZ: {', '.join(tz_findings)}")

    # F4: cross-check blocker
    cc = report["cross_check"]
    if cc.get("blocker_affected_pct", 0) > 0:
        findings.append(
            f"🔴 BLOCKER: {cc['blocker_affected_pct']:.1f}% сделок без CSV-покрытия "
            f"— тикеры: {cc['in_trades_no_csv'] + cc['in_trades_not_in_whitelist']}"
        )
    else:
        findings.append("✓ все тикеры из trades имеют CSV — можно писать simulator")

    # F5: exit_reason matches Phase 2 expectation?
    er = report["trades"].get("exit_reason_dist", {})
    if er:
        tp_pct = er.get("tp", {}).get("pct", 0)
        sl_pct = er.get("sl", {}).get("pct", 0)
        time_pct = er.get("time", {}).get("pct", 0)
        # Ожидаем грубо 40/35/25
        if abs(tp_pct - 40) < 10 and abs(sl_pct - 35) < 10 and abs(time_pct - 25) < 10:
            findings.append(
                f"✓ exit_reason {tp_pct:.0f}/{sl_pct:.0f}/{time_pct:.0f} "
                f"≈ ожидаемому 40/35/25 (Phase 2)"
            )
        else:
            findings.append(
                f"⚠ exit_reason {tp_pct:.0f}/{sl_pct:.0f}/{time_pct:.0f} "
                f"расходится с ожидаемым 40/35/25 — проверь, тот ли parquet?"
            )

    for f in findings:
        print(f"  {f}")
    report["findings"] = findings

    # ── Сохранить JSON-отчёт ────────────────────────────────────────────────
    out_json = OUT_DIR / "discovery_report.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n[INFO] JSON report saved: {out_json}")

    # ── Рекомендации для следующего шага ────────────────────────────────────
    section("NEXT STEPS — рекомендации перед simulator.py")
    print(
        "  1. Просмотри блоки [WARN] и [BLOCKER] выше.\n"
        "  2. Если cross-check без блокеров и pred_mfe_pct в %, можно писать simulator.py:\n"
        "       - base.py (ExitStrategy interface)\n"
        "       - prices_cache.py (CSV → parquet кэш)\n"
        "       - baseline.py (для воспроизведения Phase 2 Sharpe 4.87 — sanity)\n"
        "  3. TZ-вердикт: зафиксируй явно в docs/SPRINT4_CONVENTIONS.md.\n"
        "     Если prices/CSV naive — считаем, что MSK (как и telegram_news.jsonl).\n"
        "  4. Если есть BLOCKER — решаем (либо догружаем CSV, либо исключаем тикер).\n"
    )

    finished_at = datetime.now(timezone.utc).isoformat()
    print(f"\nDiscovery finished at: {finished_at}")


if __name__ == "__main__":
    main()
