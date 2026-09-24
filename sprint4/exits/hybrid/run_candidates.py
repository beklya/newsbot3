"""
sprint4/exits/hybrid/run_candidates.py — Sprint 4.9 hybrid candidates backtest.

Прогоняет 4 кандидата на Phase 2 anchor subset (~760 trades в C1 окне 2025-04..12):

  A. Baseline             — Phase 2 fixed TP/SL (без LLM)
  B. + DirectionFilter    — skip trades где LLM direction != side OR confidence < 0.5
  C. B + ImpactScale      — size_lots × impact_strength
  D. C + SBER exclude     — + skip ticker=SBER (4.0 finding)

+ Reality-check для 4.10: corr(LLM expected_timeframe, Phase 2 best realized horizon).

Output:
  - candidates_comparison.xlsx (per-cand metrics + paired comparison)
  - reality_check_4_10.json (go/no-go для DynamicHorizon)
  - hybrid_trade_results.parquet (trade-level results across cands для debugging)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[3]
EXITS_DIR = PROJECT_ROOT / "sprint4" / "exits"
sys.path.insert(0, str(Path(__file__).resolve().parent))  # llm_signal_lookup, filter, sizer
sys.path.insert(0, str(EXITS_DIR))  # baseline, base, trades_loader, prices_cache, metrics
sys.path.insert(0, str(PROJECT_ROOT))

from llm_signal_lookup import LLMSignal, LLMSignalLookup  # noqa: E402
from trade_filter import (  # noqa: E402
    AlwaysInclude, DirectionFilter, PerTickerExcludeFilter, SequentialFilter, TradeFilter,
)
from size_adjuster import IdentitySize, ImpactScale, SizeAdjuster  # noqa: E402

from baseline import BaselineFixedTpSl  # noqa: E402
from base import Trade, ExitResult  # noqa: E402
from trades_loader import load_trades_best_combo  # noqa: E402
from prices_cache import PricesCache  # noqa: E402
from metrics import compute_metrics  # noqa: E402

DEFAULT_TRADES = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet")
DEFAULT_C1 = PROJECT_ROOT / "sprint4" / "sampling" / "data" / "calibration_sample.parquet"
DEFAULT_LLM_8B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "c1_llama_3_1_8b_instant_v1_0_0.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sprint4" / "exits" / "hybrid" / "data"

ANCHOR_WINDOW_SEC = 60
EXCLUDED_TICKERS = frozenset({"SBER"})  # из 4.0 findings

# LLM expected_timeframe -> minutes для reality-check
TIMEFRAME_MIN = {"instant": 5, "short": 15, "medium": 60, "slow": 180}

# Phase 2 winning horizons — minute marks that Phase 2 best combo evaluated
PHASE2_HORIZONS_MIN = [5, 15, 30, 60, 120, 180]

log = logging.getLogger("4_9_candidates")


# ----------------------------------------------------------------------------
# Anchor mapping (trade → news_id)
# ----------------------------------------------------------------------------
def build_anchor_mapping(
    trades: list[Trade], c1_sample: pl.DataFrame, window_sec: int = ANCHOR_WINDOW_SEC,
) -> dict[int, str]:
    """For each trade index, find news_id from C1 sample within [ts_open - window, ts_open].

    Returns: {trade_idx: news_id} only for trades with anchor news.
    """
    # Filter C1 to events that ARE phase2 anchors (precomputed flag)
    anchors_df = c1_sample.filter(pl.col("has_phase2_anchor"))
    log.info("anchors in C1 sample: %d", anchors_df.height)

    if anchors_df.height == 0:
        return {}

    # Build sorted (ts_utc, id) list — anchors_df has timestamp_utc
    import numpy as np
    anchors_sorted = anchors_df.sort("timestamp_utc")
    ts_arr = anchors_sorted["timestamp_utc"].to_numpy()
    id_arr = anchors_sorted["id"].to_list()

    # Use explicit MSK timezone — naive datetime arithmetic зависит от системной locale.
    # Phase 2 trades convention: ts_open naive MSK (UTC+3).
    from datetime import timezone, timedelta
    MSK = timezone(timedelta(hours=3))

    mapping: dict[int, str] = {}
    for i, trade in enumerate(trades):
        ts_msk = trade.ts_open
        # explicit MSK→UTC conversion (timezone-independent)
        ts_utc = ts_msk.replace(tzinfo=MSK).timestamp()
        lo = ts_utc - window_sec
        hi = ts_utc
        # Find anchor in window
        idx_lo = int(np.searchsorted(ts_arr, lo, side="left"))
        idx_hi = int(np.searchsorted(ts_arr, hi, side="right"))
        if idx_lo == idx_hi:
            continue
        # Take latest in window (closest to trade entry)
        mapping[i] = id_arr[idx_hi - 1]
    return mapping


# ----------------------------------------------------------------------------
# Per-candidate runner
# ----------------------------------------------------------------------------
def run_candidate(
    name: str, trade_filter: TradeFilter, sizer: SizeAdjuster,
    trades_with_anchors: list[tuple[int, Trade, Optional[str]]],
    lookup: LLMSignalLookup, cache: PricesCache, base_strategy: BaselineFixedTpSl,
) -> tuple[list[tuple[Trade, ExitResult]], dict]:
    """Returns (list of (trade, exit_result), summary dict)."""
    log.info("=== candidate %s ===", name)
    t0 = time.perf_counter()

    results: list[tuple[Trade, ExitResult]] = []
    n_skipped_filter = 0
    n_skipped_no_bars = 0
    n_size_adjusted = 0

    for trade_idx, trade, news_id in trades_with_anchors:
        sig = lookup.get(news_id) if news_id else None

        if not trade_filter.include(trade.side, trade.ticker, sig):
            n_skipped_filter += 1
            continue

        adjusted_size = sizer.adjust(trade.size_lots, sig, ticker=trade.ticker)
        if adjusted_size != trade.size_lots:
            n_size_adjusted += 1
        modified_trade = replace(trade, size_lots=adjusted_size)

        # Fetch bars
        ts_to = trade.time_stop_ts + timedelta(minutes=2)  # buffer
        try:
            bars = cache.get_bars(modified_trade.ticker, modified_trade.ts_open, ts_to)
        except FileNotFoundError:
            n_skipped_no_bars += 1
            continue
        if bars.empty:
            n_skipped_no_bars += 1
            continue

        result = base_strategy.simulate(modified_trade, bars)
        results.append((modified_trade, result))

    summary = {
        "candidate": name,
        "n_input": len(trades_with_anchors),
        "n_simulated": len(results),
        "n_skipped_filter": n_skipped_filter,
        "n_skipped_no_bars": n_skipped_no_bars,
        "n_size_adjusted": n_size_adjusted,
        "elapsed_sec": round(time.perf_counter() - t0, 2),
    }
    log.info("  %s: simulated=%d skipped_filter=%d skipped_no_bars=%d adjusted=%d in %.1fs",
             name, summary["n_simulated"], summary["n_skipped_filter"],
             summary["n_skipped_no_bars"], summary["n_size_adjusted"], summary["elapsed_sec"])
    return results, summary


# ----------------------------------------------------------------------------
# Reality-check 4.10
# ----------------------------------------------------------------------------
def reality_check_4_10(
    trades_with_anchors: list[tuple[int, Trade, Optional[str]]],
    lookup: LLMSignalLookup, cache: PricesCache,
) -> dict:
    """Compute corr between LLM expected_timeframe and Phase 2 best realized horizon."""
    pairs = []  # (llm_timeframe_min, best_horizon_min)
    for trade_idx, trade, news_id in trades_with_anchors:
        if news_id is None:
            continue
        sig = lookup.get(news_id)
        if sig is None or sig.expected_timeframe is None:
            continue
        llm_min = TIMEFRAME_MIN.get(sig.expected_timeframe)
        if llm_min is None:
            continue

        # Compute realized return at each horizon and find max-abs
        best_horizon = None
        best_abs_r = 0.0
        try:
            for h in PHASE2_HORIZONS_MIN:
                bars = cache.get_bars(trade.ticker, trade.ts_open, trade.ts_open + timedelta(minutes=h + 1))
                if bars.empty:
                    continue
                base_close = float(bars.iloc[0]["close"])
                future_bars = bars[bars.index >= trade.ts_open + timedelta(minutes=h)]
                if future_bars.empty:
                    continue
                future_close = float(future_bars.iloc[0]["close"])
                if base_close == 0:
                    continue
                ret = abs((future_close - base_close) / base_close)
                if ret > best_abs_r:
                    best_abs_r = ret
                    best_horizon = h
        except FileNotFoundError:
            continue
        if best_horizon is None:
            continue
        pairs.append((llm_min, best_horizon))

    if len(pairs) < 30:
        return {
            "n_pairs": len(pairs),
            "correlation": None,
            "go_dynamic_horizon": False,
            "note": "Too few pairs (need ≥30) for reliable correlation",
        }

    import numpy as np
    a = np.array([p[0] for p in pairs], dtype=float)
    b = np.array([p[1] for p in pairs], dtype=float)
    # Spearman rank correlation
    rank_a = np.argsort(np.argsort(a))
    rank_b = np.argsort(np.argsort(b))
    if rank_a.std() == 0 or rank_b.std() == 0:
        spearman = 0.0
    else:
        spearman = float(np.corrcoef(rank_a, rank_b)[0, 1])

    # Agreement on coarse buckets (instant/short → short; medium/slow → long)
    n_agree = sum(1 for x, y in pairs if (x <= 15) == (y <= 15))
    agreement_pct = n_agree / len(pairs) * 100

    # AND, не OR: только обе метрики выше threshold = реальный edge.
    # OR давал false-positive на degenerate cases (LLM почти всегда "medium" →
    # binary agreement высокий случайно, при spearman ~0 = нет rank-correlation).
    go = (spearman >= 0.4) and (agreement_pct >= 60.0)
    return {
        "n_pairs": len(pairs),
        "spearman_correlation": round(spearman, 3),
        "binary_agreement_pct": round(agreement_pct, 2),
        "go_dynamic_horizon": bool(go),
        "threshold_spearman": 0.4,
        "threshold_binary": 60.0,
    }


# ----------------------------------------------------------------------------
# Paired metrics
# ----------------------------------------------------------------------------
def paired_pnl_delta(
    cand_results: list[tuple[Trade, ExitResult]],
    baseline_results: list[tuple[Trade, ExitResult]],
) -> dict:
    """Compute per-trade PnL diff vs baseline для overlapping trades."""
    base_map = {(t.ts_open, t.ticker): r.realized_pnl for t, r in baseline_results}
    deltas: list[float] = []
    for trade, result in cand_results:
        key = (trade.ts_open, trade.ticker)
        if key not in base_map:
            continue
        deltas.append(result.realized_pnl - base_map[key])

    if not deltas:
        return {"n_overlap": 0, "mean_delta": 0.0, "total_delta": 0.0}

    import numpy as np
    arr = np.array(deltas)
    return {
        "n_overlap": len(deltas),
        "mean_delta_per_trade": round(float(arr.mean()), 2),
        "median_delta": round(float(np.median(arr)), 2),
        "total_delta": round(float(arr.sum()), 2),
        "n_better": int((arr > 0).sum()),
        "n_worse": int((arr < 0).sum()),
        "n_same": int((arr == 0).sum()),
    }


# ----------------------------------------------------------------------------
# Excel report
# ----------------------------------------------------------------------------
def write_excel(path: Path, summaries: list[dict], metrics: list[dict],
                paired: list[dict], reality: dict) -> None:
    import xlsxwriter
    wb = xlsxwriter.Workbook(str(path), {"nan_inf_to_errors": True})

    def _write(sheet: str, rows: list[dict], note: str = ""):
        ws = wb.add_worksheet(sheet[:31])
        offset = 0
        if note:
            ws.write(0, 0, note)
            offset = 2
        if not rows:
            return
        cols = list(rows[0].keys())
        for j, c in enumerate(cols):
            ws.write(offset, j, c)
        for i, r in enumerate(rows):
            for j, c in enumerate(cols):
                v = r[c]
                if isinstance(v, (int, float, str, bool)):
                    ws.write(i + offset + 1, j, v)
                elif v is None:
                    ws.write(i + offset + 1, j, "")
                else:
                    ws.write(i + offset + 1, j, str(v))

    _write("summary", summaries, "Per-candidate run summary")
    _write("metrics", metrics, "Per-candidate Sharpe / PnL / Win-rate / etc.")
    _write("paired_vs_baseline", paired, "Paired Δ PnL per trade vs Cand A baseline")
    _write("reality_check_4_10", [reality], "LLM timeframe vs Phase 2 best horizon — go/no-go для 4.10")

    wb.close()
    log.info("Excel: %s", path)


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4.9 hybrid candidates backtest")
    parser.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    parser.add_argument("--c1-sample", type=Path, default=DEFAULT_C1)
    parser.add_argument("--llm", type=Path, default=DEFAULT_LLM_8B,
                        help="LLM aggregate parquet (8b или 70b — определяет тон filter'а)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-confidence", type=float, default=0.5)
    parser.add_argument("--excluded-tickers", type=str, default="SBER",
                        help="Comma-separated tickers для cand D exclude")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    # 1. Load trades (best combo)
    trades = load_trades_best_combo(args.trades)
    log.info("trades best combo: %d", len(trades))

    # 2. Load C1 sample + LLM signals
    c1 = pl.read_parquet(str(args.c1_sample))
    log.info("C1 sample: %d rows", c1.height)

    lookup = LLMSignalLookup.from_parquet(args.llm)
    log.info("LLM signals: %d events with enrichment", lookup.size)

    # 3. Anchor mapping
    anchor_map = build_anchor_mapping(trades, c1)
    log.info("anchor mapping: %d trades linked to news", len(anchor_map))

    trades_with_anchors: list[tuple[int, Trade, Optional[str]]] = [
        (i, t, anchor_map.get(i)) for i, t in enumerate(trades)
    ]
    n_with_signal = sum(1 for _, _, nid in trades_with_anchors if nid and lookup.has(nid))
    log.info("trades with LLM signal (anchor ∩ enriched): %d", n_with_signal)

    # 4. PricesCache warmup
    log.info("warming up PricesCache...")
    cache = PricesCache(cache_dir=EXITS_DIR / "data" / "cache")
    cache.warmup()

    # 5. Filters / sizers per candidate
    excluded = frozenset(t.strip() for t in args.excluded_tickers.split(",") if t.strip())
    candidates = [
        ("A_baseline", AlwaysInclude(), IdentitySize()),
        ("B_direction_filter", DirectionFilter(min_confidence=args.min_confidence), IdentitySize()),
        ("C_direction_plus_size", DirectionFilter(min_confidence=args.min_confidence), ImpactScale()),
        ("D_C_plus_exclude", SequentialFilter([
            DirectionFilter(min_confidence=args.min_confidence),
            PerTickerExcludeFilter(excluded_tickers=excluded),
        ]), ImpactScale()),
    ]

    # 6. Run all candidates
    base_strategy = BaselineFixedTpSl()
    all_results: dict[str, list[tuple[Trade, ExitResult]]] = {}
    summaries: list[dict] = []
    metrics_rows: list[dict] = []

    # ВАЖНО: 4.9 scope = только trades с anchor (где есть LLM signal context).
    # Cand A baseline на этом же subset для apples-to-apples paired comparison.
    anchor_only = [(i, t, nid) for i, t, nid in trades_with_anchors if nid is not None]
    log.info("anchor-only subset (для всех candidates): %d trades", len(anchor_only))

    for name, filt, sizer in candidates:
        results, summary = run_candidate(
            name, filt, sizer, anchor_only, lookup, cache, base_strategy,
        )
        all_results[name] = results
        summaries.append(summary)

        # Metrics
        m = compute_metrics(results)
        m["candidate"] = name
        metrics_rows.append({k: v for k, v in m.items() if isinstance(v, (int, float, str, bool)) or v is None})

    # 7. Paired comparison vs baseline
    baseline_res = all_results["A_baseline"]
    paired_rows = []
    for name in ["B_direction_filter", "C_direction_plus_size", "D_C_plus_exclude"]:
        pd_stats = paired_pnl_delta(all_results[name], baseline_res)
        pd_stats["candidate"] = name
        paired_rows.append(pd_stats)

    # 8. Reality-check
    log.info("=== Reality check for 4.10 ===")
    reality = reality_check_4_10(anchor_only, lookup, cache)
    log.info("  pairs=%d spearman=%s binary_agree=%s%% → go=%s",
             reality.get("n_pairs"),
             reality.get("spearman_correlation"),
             reality.get("binary_agreement_pct"),
             reality.get("go_dynamic_horizon"))

    # 9. Outputs
    args.output_dir.mkdir(parents=True, exist_ok=True)
    excel_path = args.output_dir / "candidates_comparison.xlsx"
    try:
        write_excel(excel_path, summaries, metrics_rows, paired_rows, reality)
    except ImportError:
        log.warning("xlsxwriter not installed — Excel skipped")

    reality_path = args.output_dir / "reality_check_4_10.json"
    reality_path.write_text(json.dumps(reality, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    log.info("reality check JSON: %s", reality_path)

    # Trade-level dump (для debugging)
    trade_rows = []
    for cand_name, cand_results in all_results.items():
        for trade, result in cand_results:
            trade_rows.append({
                "candidate": cand_name,
                "ts_open": str(trade.ts_open),
                "ticker": trade.ticker,
                "side": trade.side,
                "size_lots": trade.size_lots,
                "exit_reason": result.exit_reason,
                "realized_r": round(result.realized_r, 4),
                "realized_pnl": round(result.realized_pnl, 2),
                "duration_min": result.duration_min,
            })
    if trade_rows:
        td_df = pl.DataFrame(trade_rows)
        td_df.write_parquet(str(args.output_dir / "hybrid_trade_results.parquet"))
        log.info("trade results: %s (%d rows)", args.output_dir / "hybrid_trade_results.parquet", td_df.height)

    log.info("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
