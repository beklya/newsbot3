"""
sprint4/exits/hybrid/run_v1_holdout.py — V1 holdout backtest (Sprint 4.10 final).

OOS-проверка результатов 4.9 на untouched данных 2026-01..04:
  - Phase 2 trades в V1 окне: ~106 trades best_combo
  - V1 sample (8,000 events, 8b enriched)
  - V1 anchor pad (~104 anchor events, для 70b OOS factorial validation)
  - 4 кандидата A/B/C/D — те же что в 4.9, но на V1
  - Опционально: B_8b vs B_70b на anchor pad (does 70b uplift hold OOS?)

Outputs:
  - sprint4/exits/hybrid/data/v1/v1_holdout_results.xlsx
  - sprint4/exits/hybrid/data/v1/v1_trade_results.parquet

Reality-check 4.10: spearman correlation LLM expected_timeframe vs realized winning
horizon = -0.032 (см. 4.9 output) → DynamicHorizonExit НЕ реализуем. Per-ticker
SBER exclude уже в кандидате D.

Usage:
  python sprint4\\exits\\hybrid\\run_v1_holdout.py
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from base import Trade  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402
from prices_cache import PricesCache  # noqa: E402
from trades_loader import load_trades_best_combo  # noqa: E402

from llm_signal_lookup import LLMSignalLookup, LLMSignal  # noqa: E402
from trade_filter import (  # noqa: E402
    AlwaysInclude, DirectionFilter, PerTickerExcludeFilter, SequentialFilter,
)
from size_adjuster import IdentitySize, ImpactScale  # noqa: E402

# Defaults
DEFAULT_TRADES = Path(
    r"D:\quik_sber\newsbot\newsbot2\решение проблем"
    r"\Проблема 5 - новое начало\phase2_mfe\phase2_mfe_trades.parquet"
)
DEFAULT_V1_8B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "v1" / "v1_8b.parquet"
DEFAULT_V1_ANCHOR_8B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "v1" / "v1_anchor_pad_8b.parquet"
DEFAULT_V1_ANCHOR_70B = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "v1" / "v1_anchor_pad_70b.parquet"
DEFAULT_ANCHOR_PAD = PROJECT_ROOT / "sprint4" / "reenrich" / "data" / "v1" / "anchor_pad_v1.parquet"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "sprint4" / "exits" / "hybrid" / "data" / "v1"

# V1 window
V1_START_UTC = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
V1_END_UTC = int(datetime(2026, 5, 1, tzinfo=timezone.utc).timestamp())
ANCHOR_WINDOW_SEC = 60

# Sharpe values from 4.9 (C1 anchor subset, 762 trades)
C1_SHARPE = {
    "A_baseline": 2.76,
    "B_direction_filter": 4.04,
    "C_direction_plus_size": 2.68,
    "D_C_plus_exclude": 2.71,
}

log = logging.getLogger("v1_holdout")
MSK = timezone(timedelta(hours=3))


def filter_trades_v1_window(trades: list[Trade]) -> list[Trade]:
    """Phase 2 trades, чьи ts_open ∈ V1 окно."""
    keep = []
    for t in trades:
        ts_utc = t.ts_open.replace(tzinfo=MSK).timestamp()
        if V1_START_UTC <= ts_utc < V1_END_UTC:
            keep.append(t)
    return keep


def build_v1_anchor_map(
    trades: list[Trade], anchor_pad_path: Path,
) -> dict[int, str]:
    """Для каждого trade найти news_id из anchor_pad_v1.parquet в [-60s, 0]."""
    if not anchor_pad_path.exists():
        log.warning("anchor_pad not found at %s", anchor_pad_path)
        return {}
    anchors = pl.read_parquet(str(anchor_pad_path))
    log.info("anchor pad: %d events", anchors.height)

    anchors_sorted = anchors.sort("timestamp_utc")
    ts_arr = anchors_sorted["timestamp_utc"].to_numpy()
    id_arr = anchors_sorted["id"].to_list()

    mapping: dict[int, str] = {}
    for i, trade in enumerate(trades):
        ts_utc = trade.ts_open.replace(tzinfo=MSK).timestamp()
        lo = ts_utc - ANCHOR_WINDOW_SEC
        hi = ts_utc
        i_lo = int(np.searchsorted(ts_arr, lo, side="left"))
        i_hi = int(np.searchsorted(ts_arr, hi, side="right"))
        if i_lo == i_hi:
            continue
        mapping[i] = id_arr[i_hi - 1]
    return mapping


def load_combined_enrichment(*paths: Path) -> LLMSignalLookup:
    """Load multiple enrichment parquets, merge into single LLMSignalLookup.

    Later paths override earlier (для случая, когда anchor pad enrichment свежее).
    """
    combined_signals: dict[str, LLMSignal] = {}
    for p in paths:
        if not p or not p.exists():
            log.info("  skip missing: %s", p)
            continue
        lookup = LLMSignalLookup.from_parquet(p)
        for nid, sig in lookup._signals.items():
            combined_signals[nid] = sig
        log.info("  loaded %s: %d signals", p, lookup.size)
    return LLMSignalLookup(combined_signals)


def run_candidate(
    name: str,
    trade_filter,
    sizer,
    trades_with_anchors: list[tuple[int, Trade, Optional[str]]],
    lookup: LLMSignalLookup,
    cache: PricesCache,
    base_strategy: BaselineFixedTpSl,
) -> tuple[dict, list[dict]]:
    """Run one candidate. Returns (summary, per-trade rows)."""
    t0 = time.perf_counter()
    n_sim = 0
    n_skipped_filter = 0
    n_skipped_no_bars = 0
    n_adjusted = 0
    rows = []
    for trade_idx, trade, news_id in trades_with_anchors:
        sig = lookup.get(news_id) if news_id else None
        if not trade_filter.include(trade.side, trade.ticker, sig):
            n_skipped_filter += 1
            continue
        adjusted_size = sizer.adjust(trade.size_lots, sig, ticker=trade.ticker)
        if adjusted_size != trade.size_lots:
            n_adjusted += 1
        modified = replace(trade, size_lots=adjusted_size)
        # Fetch bars (как run_candidates.py)
        ts_to = trade.time_stop_ts + timedelta(minutes=2)
        try:
            bars = cache.get_bars(modified.ticker, modified.ts_open, ts_to)
        except FileNotFoundError:
            n_skipped_no_bars += 1
            continue
        if bars.empty:
            n_skipped_no_bars += 1
            continue
        try:
            res = base_strategy.simulate(modified, bars)
        except Exception as e:
            log.warning("simulate failed for trade %d: %s", trade_idx, e)
            n_skipped_no_bars += 1
            continue
        if res is None:
            n_skipped_no_bars += 1
            continue
        n_sim += 1
        rows.append({
            "candidate": name,
            "trade_idx": trade_idx,
            "news_id": news_id,
            "ts_open": trade.ts_open,
            "ticker": trade.ticker,
            "side": trade.side,
            "size_lots": modified.size_lots,
            "exit_reason": res.exit_reason,
            "realized_r": res.realized_r,
            "realized_pnl": res.realized_pnl,
            "duration_min": res.duration_min,
        })
    elapsed = time.perf_counter() - t0
    summary = {
        "name": name,
        "n_input": len(trades_with_anchors),
        "n_simulated": n_sim,
        "n_skipped_filter": n_skipped_filter,
        "n_skipped_no_bars": n_skipped_no_bars,
        "n_size_adjusted": n_adjusted,
        "elapsed_sec": elapsed,
    }
    log.info("  %s: simulated=%d filter_skip=%d no_bars=%d adjusted=%d in %.1fs",
             name, n_sim, n_skipped_filter, n_skipped_no_bars, n_adjusted, elapsed)
    return summary, rows


def compute_metrics(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0, "total_pnl": 0.0, "mean_pnl": 0.0, "mean_r": 0.0,
                "sharpe_daily_annualized": 0.0, "win_rate": 0.0}
    pnl = np.array([r["realized_pnl"] for r in rows])
    r_arr = np.array([r["realized_r"] for r in rows])
    mean = pnl.mean()
    std = pnl.std()
    sharpe = float(mean / std * (252 ** 0.5)) if std > 0 else 0.0
    win = float((pnl > 0).mean())
    return {
        "n": len(rows),
        "total_pnl": float(pnl.sum()),
        "mean_pnl": float(mean),
        "mean_r": float(r_arr.mean()),
        "sharpe_daily_annualized": sharpe,
        "win_rate": win,
    }


def write_excel_report(
    output_path: Path,
    summaries: list[dict],
    metrics: list[dict],
    paired_deltas: list[dict],
    extra_factorial: Optional[list[dict]] = None,
    comparison_vs_c1: Optional[list[dict]] = None,
) -> None:
    import xlsxwriter
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = xlsxwriter.Workbook(str(output_path), {"nan_inf_to_errors": True})

    def _write(sheet: str, rows: list[dict], note: str = ""):
        if not rows:
            return
        ws = wb.add_worksheet(sheet[:31])
        offset = 0
        if note:
            ws.write(0, 0, note)
            offset = 2
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

    _write("summaries", summaries, "Per-candidate execution stats")
    _write("metrics", metrics, "Per-candidate Sharpe/PnL/Win на V1")
    _write("paired_vs_A", paired_deltas, "Paired Δ PnL vs A_baseline на same-trade subset")
    if extra_factorial:
        _write("factorial_8b_vs_70b", extra_factorial,
               "B-filter с 8b vs 70b signals (factorial OOS check)")
    if comparison_vs_c1:
        _write("v1_vs_c1", comparison_vs_c1,
               "V1 Sharpe vs C1 Sharpe — degradation factor проверка overfitting")

    wb.close()
    log.info("Excel: %s", output_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sprint 4.10 V1 holdout backtest")
    parser.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    parser.add_argument("--v1-8b", type=Path, default=DEFAULT_V1_8B)
    parser.add_argument("--v1-anchor-8b", type=Path, default=DEFAULT_V1_ANCHOR_8B)
    parser.add_argument("--v1-anchor-70b", type=Path, default=DEFAULT_V1_ANCHOR_70B)
    parser.add_argument("--anchor-pad", type=Path, default=DEFAULT_ANCHOR_PAD)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-confidence", type=float, default=0.5)
    parser.add_argument("--excluded-tickers", type=str, default="SBER")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )

    # 1. Phase 2 trades в V1 окне
    all_trades = load_trades_best_combo(args.trades)
    log.info("trades best_combo loaded: %d total", len(all_trades))
    v1_trades = filter_trades_v1_window(all_trades)
    log.info("trades в V1 окне (2026-01..04): %d", len(v1_trades))
    if not v1_trades:
        log.error("no Phase 2 trades в V1 окне")
        return 2

    # 2. Anchor mapping
    anchor_map = build_v1_anchor_map(v1_trades, args.anchor_pad)
    log.info("anchor mapping: %d trades → news", len(anchor_map))

    trades_with_anchors = [
        (i, t, anchor_map.get(i)) for i, t in enumerate(v1_trades)
    ]

    # 3. LLM enrichments (combined V1 sample 8b + V1 anchor 8b)
    log.info("loading 8b enrichments...")
    lookup_8b = load_combined_enrichment(args.v1_8b, args.v1_anchor_8b)
    log.info("8b combined: %d signals", lookup_8b.size)

    lookup_70b = None
    if args.v1_anchor_70b.exists():
        log.info("loading 70b enrichments...")
        lookup_70b = load_combined_enrichment(args.v1_anchor_70b)
        log.info("70b: %d signals", lookup_70b.size)

    with_8b = sum(1 for _, _, nid in trades_with_anchors if nid and lookup_8b.has(nid))
    log.info("trades с 8b signal: %d / %d", with_8b, len(v1_trades))
    if lookup_70b:
        with_70b = sum(1 for _, _, nid in trades_with_anchors if nid and lookup_70b.has(nid))
        log.info("trades с 70b signal: %d / %d", with_70b, len(v1_trades))

    # 4. Candidates (те же что 4.9)
    base_strategy = BaselineFixedTpSl()
    cache = PricesCache()
    log.info("warming up PricesCache...")
    cache.warmup()

    excluded = frozenset(t.strip() for t in args.excluded_tickers.split(",") if t.strip())

    candidates = [
        ("A_baseline", AlwaysInclude(), IdentitySize()),
        ("B_direction_filter",
            DirectionFilter(min_confidence=args.min_confidence),
            IdentitySize()),
        ("C_direction_plus_size",
            DirectionFilter(min_confidence=args.min_confidence),
            ImpactScale()),
        ("D_C_plus_exclude",
            SequentialFilter(filters=[
                DirectionFilter(min_confidence=args.min_confidence),
                PerTickerExcludeFilter(excluded_tickers=excluded),
            ]),
            ImpactScale()),
    ]

    summaries = []
    all_rows = []
    log.info("=== Running candidates на V1 (8b signals) ===")
    for name, filt, sizer in candidates:
        s, rows = run_candidate(
            name, filt, sizer, trades_with_anchors, lookup_8b, cache, base_strategy,
        )
        summaries.append(s)
        all_rows.extend(rows)

    # 5. Metrics + paired delta
    metrics_rows = []
    for cand_name in [c[0] for c in candidates]:
        cand_rows = [r for r in all_rows if r["candidate"] == cand_name]
        m = compute_metrics(cand_rows)
        m["candidate"] = cand_name
        metrics_rows.append(m)
        log.info("  %s: n=%d total_pnl=%+10.0f sharpe=%.2f win%%=%.1f",
                 cand_name, m["n"], m["total_pnl"], m["sharpe_daily_annualized"], m["win_rate"] * 100)

    baseline_rows = [r for r in all_rows if r["candidate"] == "A_baseline"]
    baseline_idx = {r["trade_idx"]: r["realized_pnl"] for r in baseline_rows}
    paired_rows = []
    for cand_name in ["B_direction_filter", "C_direction_plus_size", "D_C_plus_exclude"]:
        cand_rows = [r for r in all_rows if r["candidate"] == cand_name]
        deltas = []
        for r in cand_rows:
            base_pnl = baseline_idx.get(r["trade_idx"])
            if base_pnl is None:
                continue
            deltas.append(r["realized_pnl"] - base_pnl)
        if deltas:
            paired_rows.append({
                "candidate": cand_name,
                "n_paired": len(deltas),
                "delta_sum": float(sum(deltas)),
                "delta_mean": float(np.mean(deltas)),
                "n_positive": int(sum(1 for d in deltas if d > 0)),
            })

    # 6. Factorial check: B with 8b vs B with 70b
    extra_factorial = None
    if lookup_70b and lookup_70b.size > 0:
        log.info("=== Factorial 8b vs 70b на B_direction_filter ===")
        b_filt = DirectionFilter(min_confidence=args.min_confidence)
        _, rows_8b = run_candidate(
            "B_8b", b_filt, IdentitySize(), trades_with_anchors, lookup_8b, cache, base_strategy,
        )
        _, rows_70b = run_candidate(
            "B_70b", b_filt, IdentitySize(), trades_with_anchors, lookup_70b, cache, base_strategy,
        )
        m_8b = compute_metrics(rows_8b)
        m_70b = compute_metrics(rows_70b)
        extra_factorial = [
            {"model": "8b", **m_8b},
            {"model": "70b", **m_70b},
        ]
        for e in extra_factorial:
            log.info("  B с %s: n=%d total_pnl=%+10.0f sharpe=%.2f",
                     e["model"], e["n"], e["total_pnl"], e["sharpe_daily_annualized"])

    # 7. Comparison vs C1 (from 4.9)
    log.info("")
    log.info("=== V1 vs C1 — degradation factor (Sprint 4.9 baselines) ===")
    comparison_vs_c1 = []
    for m in metrics_rows:
        c1 = C1_SHARPE.get(m["candidate"], 0.0)
        v1 = m["sharpe_daily_annualized"]
        if c1 > 0:
            deg_pct = (1 - v1 / c1) * 100
            log.info("  %s: C1=%.2f → V1=%.2f (degradation %+.1f%%)",
                     m["candidate"], c1, v1, deg_pct)
            comparison_vs_c1.append({
                "candidate": m["candidate"],
                "c1_sharpe": c1,
                "v1_sharpe": v1,
                "degradation_pct": deg_pct,
                "v1_n_trades": m["n"],
            })

    # 8. Write outputs
    args.output_dir.mkdir(parents=True, exist_ok=True)
    excel_path = args.output_dir / "v1_holdout_results.xlsx"
    write_excel_report(
        excel_path, summaries, metrics_rows, paired_rows,
        extra_factorial, comparison_vs_c1,
    )

    trades_path = args.output_dir / "v1_trade_results.parquet"
    if all_rows:
        pl.DataFrame(all_rows).write_parquet(str(trades_path))
        log.info("trade results: %s (%d rows)", trades_path, len(all_rows))

    log.info("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
