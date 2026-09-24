r"""
scripts/walk_forward_b_filter.py
==================================
Sprint 5.7 Track B — LLM-as-direction-filter поверх Phase 2 trades.
Walk-forward 13 folds, Phase 2 baseline (h=60, rr=2.0, mx_specific) + B_direction_filter.

Контекст:
  Sprint 4.10 V1 holdout показал B_direction_filter Sharpe 2.71 на single fold
  с 8b enrichment + anchor pad. Этот скрипт проверяет: даёт ли тот же подход
  consistent Sharpe ~2.7+ на ВСЕХ 13 фолдах с 70B v1.0.0 prod enrichment.

Архитектура:
  1. Load Phase 2 trades (best_combo h=60, rr=2.0, mx_specific) — все ~3,300 trades
  2. Load 70B v1.0.0 enrichment (full_70k_70b.parquet) → LLMSignalLookup
  3. Для каждого фолда (mirror Phase 2 walk-forward):
     a) Filter trades в fold's test window
     b) Для каждого trade: anchor matching (news event в [-60s, ts_open])
     c) B_direction_filter: include trade if (llm.direction == trade.side AND llm.confidence >= 0.5)
     d) Simulate через BaselineFixedTpSl с реальными ценами
     e) Compute per-fold metrics (Sharpe, win rate, PnL, trade count)
  4. Aggregate mean Sharpe + per-fold breakdown

Usage:
  python scripts/walk_forward_b_filter.py
  python scripts/walk_forward_b_filter.py --enrichment data/reenrich_phase2/full_70k_70b.parquet
  python scripts/walk_forward_b_filter.py --min-confidence 0.5

Output:
  data/reenrich_phase2/walk_forward/b_filter_70b/walk_forward_b_filter_70b.xlsx
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from base import Trade  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402
from prices_cache import PricesCache  # noqa: E402
from trades_loader import load_trades_best_combo  # noqa: E402

from llm_signal_lookup import LLMSignalLookup, LLMSignal  # noqa: E402
from trade_filter import DirectionFilter, TradeFilter  # noqa: E402

# Sprint 6 verification (2026-06-06): we have three filter variants to compare
# on identical walk-forward data:
#   * DirectionFilter (sprint4 LENIENT)        — reference, walk-forward Sharpe 6.42
#   * StrictDirectionFilter                    — old broken prod semantics, Sharpe 0
#   * ProdFilterAdapter (--prod-filter)        — CURRENT src/services/decision/filter.py,
#                                                 must equal LENIENT after Sprint 6.1 revert
from types import SimpleNamespace
from src.services.decision.filter import (  # noqa: E402
    apply_direction_filter as _prod_apply_direction_filter,
)


# --- Production-strict variant (mirrors prior, broken src/services/decision/filter.py) ----
class StrictDirectionFilter(TradeFilter):
    """Mirror of the STRICT variant of src/services/decision/filter.py that was
    deployed Sep 2025 → Sprint 6.1 (reverted 2026-06-06).

    REJECT if signal missing, ticker not in LLM tickers[], or direction=neutral.
    Only INCLUDE on explicit positive endorsement matching trade side.

    Keeps this old logic AS DATA for the regression test — if walk-forward
    on this variant ever rises above ~1 trade/fold, something is wrong with
    the dataset.
    """
    def __init__(self, min_confidence: float = 0.5) -> None:
        self.min_confidence = min_confidence
        self._side_map = {"buy": "long", "sell": "short",
                          "BUY": "long", "SELL": "short"}

    def include(self, trade_side: str, trade_ticker: str,
                signal: Optional[LLMSignal]) -> bool:
        if signal is None:
            return False
        ts = signal.get_for_ticker(trade_ticker)
        if ts is None:
            return False
        if ts.direction is None or ts.direction == "neutral":
            return False
        expected = self._side_map.get(trade_side, trade_side)
        if ts.direction != expected:
            return False
        if ts.confidence is None or ts.confidence < self.min_confidence:
            return False
        return True


# --- Production-current adapter (calls real src/services/decision/filter.py) ---
class ProdFilterAdapter(TradeFilter):
    """Wraps the LIVE src/services/decision/filter.py::apply_direction_filter
    so walk-forward exercises the actual production code path.

    apply_direction_filter signature:  (event, ticker, side, min_confidence) -> FilterDecision
    walk-forward signature:            include(trade_side, trade_ticker, signal) -> bool

    The prod filter only touches event.payload.tickers[i].{ticker,direction,confidence}
    via simple attribute access — no isinstance / pydantic enforcement at runtime.
    We feed it a duck-typed SimpleNamespace built from sprint4 LLMSignal/TickerSignal
    so the test exercises the EXACT branching of the reverted filter.py.

    Side mapping: sprint4 Trade.side ∈ {"long","short"} or {"buy","sell"}. Prod
    filter expects {"BUY","SELL"}. Normalize before dispatch.
    """
    # Phase 2 Trade.side is INT (1=long, -1=short) per sprint4/exits/base.py
    # AND row_to_trade in trades_loader.py.  Live pipeline.py wires side as
    # "BUY"/"SELL" strings.  Normalize both forms so the adapter tests the same
    # decision path as production.
    _SIDE_NORMALIZE = {
        "long": "BUY", "BUY": "BUY", "buy": "BUY", 1: "BUY",
        "short": "SELL", "SELL": "SELL", "sell": "SELL", -1: "SELL",
    }

    def __init__(self, min_confidence: float = 0.5) -> None:
        self.min_confidence = min_confidence

    def include(self, trade_side: str, trade_ticker: str,
                signal: Optional[LLMSignal]) -> bool:
        prod_side = self._SIDE_NORMALIZE.get(trade_side)
        if prod_side is None:
            return False
        if signal is None:
            # apply_direction_filter expects an event with .payload.tickers.
            # Empty tickers[] mimics "no per-ticker signal" → prod returns INCLUDE
            # under LENIENT semantics. Construct an empty shim event.
            fake_event = SimpleNamespace(payload=SimpleNamespace(tickers=[]))
        else:
            tickers_shim = [
                SimpleNamespace(
                    ticker=ts.ticker,
                    direction=ts.direction if ts.direction is not None else "neutral",
                    confidence=ts.confidence if ts.confidence is not None else 0.0,
                )
                for ts in signal.tickers
            ]
            fake_event = SimpleNamespace(payload=SimpleNamespace(tickers=tickers_shim))
        decision = _prod_apply_direction_filter(
            fake_event, trade_ticker, prod_side, self.min_confidence,
        )
        return bool(decision.include)

# ============================================================
# Phase 2 walk-forward windows (test periods)
# ============================================================
PHASE2_DIR = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe")
DEFAULT_TRADES = PHASE2_DIR / "phase2_mfe_trades.parquet"
DEFAULT_ENRICHMENT = PROJECT_ROOT / "data" / "reenrich_phase2" / "full_70k_70b.parquet"

# 13 test windows mirror Phase 2 walk-forward: TRAIN_MONTHS=12, TEST_MONTHS=3, STEP=3.
# Phase 2 first events ≈ 2022-01-03. test_start = 2023-01-03, ..., 2026-01-03.
FOLDS = []
for i in range(13):
    test_start = pd.Timestamp("2023-01-03") + pd.DateOffset(months=3 * i)
    test_end = test_start + pd.DateOffset(months=3)
    FOLDS.append({"fold": i + 1, "test_start": test_start, "test_end": test_end})

MSK = timezone(timedelta(hours=3))
ANCHOR_WINDOW_SEC = 60  # news event в [-60s, ts_open] = anchor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("walk_forward_b")


# ============================================================
# Helpers
# ============================================================
def filter_trades_to_window(trades: List[Trade], start: pd.Timestamp, end: pd.Timestamp) -> List[Trade]:
    """Phase 2 trades, чьи ts_open ∈ [start, end)."""
    start_utc = int(start.replace(tzinfo=MSK).timestamp())
    end_utc = int(end.replace(tzinfo=MSK).timestamp())
    keep = []
    for t in trades:
        ts_utc = t.ts_open.replace(tzinfo=MSK).timestamp()
        if start_utc <= ts_utc < end_utc:
            keep.append(t)
    return keep


def build_anchor_map(trades: List[Trade], enrichment_df: pl.DataFrame) -> Dict[int, str]:
    """For each trade idx, find news event с timestamp_utc в [-60s, ts_open]."""
    # Sort enrichment by timestamp_utc for binary search
    enr = enrichment_df.sort("timestamp_utc")
    ts_arr = enr["timestamp_utc"].to_numpy()
    id_arr = enr["id"].to_list()

    mapping: Dict[int, str] = {}
    for i, trade in enumerate(trades):
        ts_utc = trade.ts_open.replace(tzinfo=MSK).timestamp()
        lo = ts_utc - ANCHOR_WINDOW_SEC
        hi = ts_utc
        i_lo = int(np.searchsorted(ts_arr, lo, side="left"))
        i_hi = int(np.searchsorted(ts_arr, hi, side="right"))
        if i_lo == i_hi:
            continue
        mapping[i] = id_arr[i_hi - 1]  # last news in window
    return mapping


def run_fold(
    fold_meta: dict,
    all_trades: List[Trade],
    enrichment_df: pl.DataFrame,
    lookup: LLMSignalLookup,
    filter_obj: DirectionFilter,
    cache: PricesCache,
    base_strategy: BaselineFixedTpSl,
) -> tuple[dict, List[dict]]:
    """Run one walk-forward fold. Returns (metrics, trades_log)."""
    fold = fold_meta["fold"]
    start, end = fold_meta["test_start"], fold_meta["test_end"]

    fold_trades = filter_trades_to_window(all_trades, start, end)
    n_in = len(fold_trades)
    if n_in == 0:
        return ({"fold": fold, "test_start": start.isoformat(), "test_end": end.isoformat(),
                 "n_input": 0, "n_with_anchor": 0, "n_filtered": 0, "n_simulated": 0,
                 "sharpe": 0.0, "total_pnl_rub": 0.0, "win_rate": 0.0}, [])

    anchor_map = build_anchor_map(fold_trades, enrichment_df)
    n_anchored = len(anchor_map)

    rows = []
    n_filtered = 0
    n_no_bars = 0
    for i, trade in enumerate(fold_trades):
        news_id = anchor_map.get(i)
        sig = lookup.get(news_id) if news_id else None

        if not filter_obj.include(trade.side, trade.ticker, sig):
            n_filtered += 1
            continue

        ts_to = trade.time_stop_ts + timedelta(minutes=2)
        try:
            bars = cache.get_bars(trade.ticker, trade.ts_open, ts_to)
        except FileNotFoundError:
            n_no_bars += 1
            continue
        if bars.empty:
            n_no_bars += 1
            continue
        try:
            res = base_strategy.simulate(trade, bars)
        except Exception as e:
            log.warning("simulate failed fold=%d trade_idx=%d: %s", fold, i, e)
            continue
        if res is None:
            n_no_bars += 1
            continue

        rows.append({
            "fold": fold,
            "trade_idx": i,
            "news_id": news_id,
            "ts_open": trade.ts_open,
            "ts_close": res.ts_close,
            "ticker": trade.ticker,
            "side": trade.side,
            "exit_reason": res.exit_reason,
            "realized_r": res.realized_r,
            "realized_pnl": res.realized_pnl,
            "duration_min": res.duration_min,
        })

    # Compute metrics
    n_sim = len(rows)
    if n_sim == 0:
        m = {"sharpe": 0.0, "total_pnl_rub": 0.0, "win_rate": 0.0,
             "median_pnl_rub": 0.0, "max_dd_rub": 0.0}
    else:
        df = pd.DataFrame(rows)
        pnl = df["realized_pnl"].values
        win_rate = float((pnl > 0).mean())
        df["close_dt"] = pd.to_datetime([r.get("ts_close") for r in rows], errors="coerce")
        df["date"] = df["close_dt"].dt.date
        daily = df.groupby("date")["realized_pnl"].sum()
        sharpe = float((daily.mean() / daily.std()) * np.sqrt(252)) if len(daily) > 1 and daily.std() > 0 else 0.0
        cum = pd.Series(pnl).cumsum()
        max_dd = float((cum - cum.cummax()).min())
        m = {
            "sharpe": sharpe,
            "total_pnl_rub": float(pnl.sum()),
            "median_pnl_rub": float(np.median(pnl)),
            "max_dd_rub": max_dd,
            "win_rate": win_rate,
        }

    metrics = {
        "fold": fold,
        "test_start": start.isoformat()[:10],
        "test_end": end.isoformat()[:10],
        "n_input": n_in,
        "n_with_anchor": n_anchored,
        "n_filtered_out": n_filtered,
        "n_no_bars": n_no_bars,
        "n_simulated": n_sim,
        **m,
    }
    return metrics, rows


def write_report(output_path: Path, per_fold: List[dict], summary: dict, all_trades: List[dict]) -> None:
    import xlsxwriter
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = xlsxwriter.Workbook(str(output_path), {"nan_inf_to_errors": True})

    def _ws(name, rows):
        if not rows:
            return
        ws = wb.add_worksheet(name[:31])
        cols = list(rows[0].keys())
        for j, c in enumerate(cols):
            ws.write(0, j, c)
        for i, r in enumerate(rows):
            for j, c in enumerate(cols):
                v = r[c]
                if isinstance(v, pd.Timestamp):
                    ws.write(i + 1, j, v.isoformat())
                elif isinstance(v, (int, float, str, bool)):
                    ws.write(i + 1, j, v)
                elif v is None:
                    ws.write(i + 1, j, "")
                else:
                    ws.write(i + 1, j, str(v))

    _ws("per_fold", per_fold)
    _ws("summary", [summary])
    if all_trades:
        _ws("trades", all_trades)
    wb.close()
    log.info("Excel: %s", output_path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", type=Path, default=DEFAULT_TRADES)
    ap.add_argument("--enrichment", type=Path, default=DEFAULT_ENRICHMENT,
                    help="LLM enrichment parquet (aggregate_checkpoint output)")
    ap.add_argument("--min-confidence", type=float, default=0.5,
                    help="DirectionFilter min confidence (default 0.5, Sprint 4 used 0.5)")
    ap.add_argument("--strict", action="store_true",
                    help="Use production-strict DirectionFilter "
                         "(REJECT if signal missing / ticker not in tickers[] / "
                         "neutral). Default = lenient (Sprint 4 walk-forward semantics).")
    ap.add_argument("--prod-filter", action="store_true",
                    help="Use the LIVE src/services/decision/filter.py via adapter — "
                         "exercises the actual production code path on walk-forward "
                         "data. Should match LENIENT result after Sprint 6.1 revert.")
    ap.add_argument("--label", default="b_filter_70b")
    ap.add_argument("--output-dir", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "walk_forward")
    args = ap.parse_args()

    output_dir = args.output_dir / args.label
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load Phase 2 trades
    log.info("Loading Phase 2 trades: %s", args.trades)
    all_trades = load_trades_best_combo(args.trades)
    log.info("  loaded: %d trades (h=60, rr=2.0, mx_specific)", len(all_trades))

    # Load enrichment
    log.info("Loading enrichment: %s", args.enrichment)
    enrichment_df = pl.read_parquet(str(args.enrichment))
    log.info("  enrichment: %d rows", enrichment_df.height)

    lookup = LLMSignalLookup.from_parquet(args.enrichment)
    log.info("  LLMSignalLookup: %d signals", lookup.size)

    # Setup strategy
    base_strategy = BaselineFixedTpSl()
    cache = PricesCache()
    log.info("Warming up PricesCache...")
    cache.warmup()

    if args.strict and args.prod_filter:
        raise SystemExit("--strict and --prod-filter are mutually exclusive")
    if args.strict:
        filter_obj = StrictDirectionFilter(min_confidence=args.min_confidence)
        log.info("Filter = StrictDirectionFilter (OLD broken PROD semantics) "
                 "min_confidence=%.2f", args.min_confidence)
    elif args.prod_filter:
        filter_obj = ProdFilterAdapter(min_confidence=args.min_confidence)
        log.info("Filter = ProdFilterAdapter (LIVE src/services/decision/filter.py) "
                 "min_confidence=%.2f", args.min_confidence)
    else:
        filter_obj = DirectionFilter(min_confidence=args.min_confidence)
        log.info("Filter = DirectionFilter (LENIENT / Sprint 4 reference) "
                 "min_confidence=%.2f", args.min_confidence)

    # Run per fold
    per_fold = []
    all_trade_rows = []
    log.info("=" * 60)
    log.info("WALK-FORWARD B_direction_filter (13 folds)")
    log.info("=" * 60)
    for fold_meta in FOLDS:
        t0 = time.time()
        log.info("FOLD %d/13 — test [%s → %s]",
                 fold_meta["fold"], fold_meta["test_start"].date(), fold_meta["test_end"].date())
        metrics, rows = run_fold(
            fold_meta, all_trades, enrichment_df, lookup,
            filter_obj, cache, base_strategy,
        )
        log.info("  input=%d anchored=%d filtered=%d simulated=%d sharpe=%.2f pnl=%+.0f win=%.1f%%  (%.1fs)",
                 metrics["n_input"], metrics["n_with_anchor"], metrics["n_filtered_out"],
                 metrics["n_simulated"], metrics["sharpe"], metrics["total_pnl_rub"],
                 metrics["win_rate"] * 100, time.time() - t0)
        per_fold.append(metrics)
        all_trade_rows.extend(rows)

    # Aggregate
    sharpes = [r["sharpe"] for r in per_fold if r["n_simulated"] > 0]
    pnls = [r["total_pnl_rub"] for r in per_fold]
    n_sims = [r["n_simulated"] for r in per_fold]
    summary = {
        "label": args.label,
        "min_confidence": args.min_confidence,
        "n_folds": len(per_fold),
        "n_folds_with_trades": len(sharpes),
        "mean_sharpe": float(np.mean(sharpes)) if sharpes else 0.0,
        "median_sharpe": float(np.median(sharpes)) if sharpes else 0.0,
        "min_sharpe": float(np.min(sharpes)) if sharpes else 0.0,
        "std_sharpe": float(np.std(sharpes)) if sharpes else 0.0,
        "n_folds_positive_pnl": sum(1 for p in pnls if p > 0),
        "total_pnl_rub": float(sum(pnls)),
        "mean_n_trades": float(np.mean(n_sims)) if n_sims else 0.0,
        "total_n_trades": int(sum(n_sims)),
    }

    log.info("=" * 60)
    log.info("WALK-FORWARD B_FILTER SUMMARY")
    log.info("=" * 60)
    log.info("  Label: %s  (min_confidence=%.2f)", args.label, args.min_confidence)
    log.info("  Folds:                  %d", summary["n_folds"])
    log.info("  Mean Sharpe:            %.2f  (Sprint 4.10 V1 baseline = 2.71)",
             summary["mean_sharpe"])
    log.info("  Median Sharpe:          %.2f", summary["median_sharpe"])
    log.info("  Min Sharpe:             %.2f", summary["min_sharpe"])
    log.info("  Std Sharpe:             %.2f", summary["std_sharpe"])
    log.info("  Positive PnL folds:     %d/%d", summary["n_folds_positive_pnl"], summary["n_folds"])
    log.info("  Total trades:           %d", summary["total_n_trades"])
    log.info("  Mean trades/fold:       %.0f", summary["mean_n_trades"])
    log.info("  Total PnL:              %+.0f RUB", summary["total_pnl_rub"])

    excel_path = output_dir / f"walk_forward_{args.label}.xlsx"
    write_report(excel_path, per_fold, summary, all_trade_rows)
    if all_trade_rows:
        pl.DataFrame(all_trade_rows).write_parquet(str(output_dir / f"walk_forward_{args.label}_trades.parquet"))
        log.info("Trades parquet: %s", output_dir / f"walk_forward_{args.label}_trades.parquet")

    # Acceptance gate
    log.info("")
    log.info("=== Sprint 5.7 Track B ACCEPTANCE GATE ===")
    target = 2.71
    primary = summary["mean_sharpe"] >= target
    log.info("  Mean Sharpe ≥ %.2f (Sprint 4.10 V1 baseline):  %.2f  %s",
             target, summary["mean_sharpe"], "✓" if primary else "✗")
    log.info("  Median Sharpe ≥ %.2f:                          %.2f  %s",
             target, summary["median_sharpe"], "✓" if summary["median_sharpe"] >= target else "✗")
    return 0


if __name__ == "__main__":
    sys.exit(main())
