r"""Sprint 6.1 — walk-forward across ALL folds using v7 architecture.

For each fold k in 1..N:
  - Train k-specific XGBoost (16 models) on train window [start, test_start-30min]
  - Predict on test window [test_start, test_end]
  - Apply Decision pipeline (R:R + B_filter) per prediction
  - Simulate paper-fill using sprint4 BaselineFixedTpSl + PricesCache
  - Aggregate per-fold metrics

Output: walk-forward summary + per-fold table.

Usage:
    python scripts/walk_forward_v7_all_folds.py \
        --features data/reenrich_phase2/features_mfe_v2.parquet \
        --targets  data/reenrich_phase2/targets_mfe_v2.parquet \
        --label    v7_walkfwd
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

import joblib  # noqa: E402

from scripts.train_predictor_fold13 import (  # noqa: E402
    XGB_PARAMS, TARGETS, MODEL_TYPES, get_target_columns,
    merge_features_and_targets, build_folds,
    compute_sample_weights, train_single_model,
)
# Sprint 6.1: allow override of walk-forward step for finer slicing
import scripts.train_predictor_fold13 as _tp

from base import Trade as Sprint4Trade  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402
from prices_cache import PricesCache  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("walk_forward_v7")

# Decision params (Sprint 6.1 v7-tuned)
RR_THRESHOLD = 1.0
MIN_MFE_PCT = 0.0
MIN_MAE_PCT = 0.05
TP_FRACTION = 0.7
SL_BUFFER = 1.2
SL_FLOOR_PCT = 0.0005
TP_FLOOR_PCT = 0.001
HORIZON_MIN = 60
DEFAULT_BLACKLIST = {"GAZP"}
MSK = timezone(timedelta(hours=3))


def train_models_for_fold(train_df: pd.DataFrame, feature_cols: list[str]):
    """Train all 16 models for a single fold, return dict."""
    X_train_full = train_df[feature_cols].values
    sample_weights = compute_sample_weights(train_df["_ticker"])
    mx_mask = (train_df["_ticker"] == "MX").values
    X_train_mx = train_df.loc[mx_mask, feature_cols].values if mx_mask.sum() >= 500 else None

    models: dict[tuple, object] = {}
    for target_col in get_target_columns():
        y_full = train_df[target_col].values
        for mt in MODEL_TYPES:
            if mt == "general":
                m = train_single_model(X_train_full, y_full, sample_weights,
                                        label=f"{target_col}/general")
            else:
                if X_train_mx is None:
                    continue
                y_mx = train_df.loc[mx_mask, target_col].values
                m = train_single_model(X_train_mx, y_mx, None,
                                        label=f"{target_col}/mx")
            if m is not None:
                models[(target_col, mt)] = m
    return models


def predict_one(models: dict, X: np.ndarray, ticker: str, horizon_min: int):
    """Return (rr_long, rr_short, mfe_long, mae_long, mfe_short, mae_short, last_close-agnostic)."""
    mt = "mx_specific" if ticker == "MX" else "general"
    out = {}
    for tgt in TARGETS:
        col = f"{tgt}_{horizon_min}m"
        m = models.get((col, mt)) or models.get((col, "general"))
        if m is None:
            return None
        out[tgt] = max(0.0, float(m.predict(X.reshape(1, -1))[0]))
    rr_long = out["mfe_long"] / max(out["mae_long"], MIN_MAE_PCT)
    rr_short = out["mfe_short"] / max(out["mae_short"], MIN_MAE_PCT)
    return rr_long, rr_short, out["mfe_long"], out["mae_long"], out["mfe_short"], out["mae_short"]


def evaluate_rr(rr_long, rr_short, mfe_long, mfe_short):
    if rr_long >= RR_THRESHOLD and mfe_long >= MIN_MFE_PCT:
        return ("BUY", rr_long, mfe_long)
    if rr_short >= RR_THRESHOLD and mfe_short >= MIN_MFE_PCT:
        return ("SELL", rr_short, mfe_short)
    return None


def simulate_fold(fold_meta: dict, test_df: pd.DataFrame, feature_cols: list[str],
                  models: dict, cache: PricesCache, blacklist: set[str]) -> tuple[dict, list[dict]]:
    base_strategy = BaselineFixedTpSl()
    rows = []
    n_in = len(test_df)
    n_blacklist = 0
    n_rr_rej = 0

    for idx, row in test_df.iterrows():
        ticker = row["_ticker"]
        if ticker in blacklist:
            n_blacklist += 1
            continue
        X = row[feature_cols].values.astype(np.float64)
        pred = predict_one(models, X, ticker, HORIZON_MIN)
        if pred is None:
            continue
        rr_long, rr_short, mfe_long, mae_long, mfe_short, mae_short = pred
        rr_result = evaluate_rr(rr_long, rr_short, mfe_long, mfe_short)
        if rr_result is None:
            n_rr_rej += 1
            continue
        side, rr_ratio, _ = rr_result

        # Build sprint4 Trade with PROPER entry/SL/TP from predictions
        try:
            news_ts = pd.Timestamp(row["_datetime"])
        except Exception:
            continue
        side_int = 1 if side == "BUY" else -1
        try:
            bars = cache.get_bars(ticker, news_ts, news_ts + pd.Timedelta(minutes=int(HORIZON_MIN * 1.5) + 2))
        except FileNotFoundError:
            continue
        if bars is None or len(bars) == 0:
            continue
        # Compute entry from next-min bar open AFTER news_ts (production semantics)
        next_min = (news_ts + pd.Timedelta(seconds=60)).floor("min")
        idx_e = bars.index.searchsorted(next_min)
        if idx_e >= len(bars):
            continue
        entry_price = float(bars["open"].iloc[idx_e])
        if entry_price <= 0:
            continue
        # compute_levels (Phase 2 formula)
        # For BUY: mfe_long predicted upward → TP up. For SELL: mfe_short upward → TP down.
        if side == "BUY":
            tp_dist_pct = max(mfe_long * TP_FRACTION / 100, TP_FLOOR_PCT)
            sl_dist_pct = max(mae_long * SL_BUFFER / 100, SL_FLOOR_PCT)
            tp_price = entry_price * (1 + tp_dist_pct)
            sl_price = entry_price * (1 - sl_dist_pct)
        else:  # SELL
            tp_dist_pct = max(mfe_short * TP_FRACTION / 100, TP_FLOOR_PCT)
            sl_dist_pct = max(mae_short * SL_BUFFER / 100, SL_FLOOR_PCT)
            tp_price = entry_price * (1 - tp_dist_pct)
            sl_price = entry_price * (1 + sl_dist_pct)
        actual_entry_ts = bars.index[idx_e]
        t = Sprint4Trade(
            ticker=ticker, fold=fold_meta["fold"], horizon_min=HORIZON_MIN,
            rr_threshold=rr_ratio, model_type="general",
            ts_open=actual_entry_ts, side=side_int, entry=entry_price,
            size_lots=1, sl_price=sl_price, tp_price=tp_price,
            pred_mfe_pct=mfe_long if side == "BUY" else mfe_short,
            pred_mae_pct=mae_long if side == "BUY" else mae_short,
            ts_close_phase2=actual_entry_ts + pd.Timedelta(minutes=HORIZON_MIN),
            exit_price_phase2=0.0, exit_reason_phase2="unknown",
            net_pnl_rub_phase2=0.0, cost_rub=2.0,
        )
        try:
            res = base_strategy.simulate(t, bars)
        except Exception:
            continue
        if res is None:
            continue
        rows.append({
            "fold": fold_meta["fold"], "ts_open": news_ts,
            "ts_close": res.ts_close, "ticker": ticker, "side": side,
            "rr": rr_ratio, "exit_reason": res.exit_reason,
            "realized_r": res.realized_r, "realized_pnl": res.realized_pnl,
        })

    # Per-fold stats
    if not rows:
        return ({
            "fold": fold_meta["fold"],
            "test_start": fold_meta["test_start"].isoformat()[:10],
            "test_end": fold_meta["test_end"].isoformat()[:10],
            "n_in": n_in, "n_blacklist": n_blacklist,
            "n_rr_rej": n_rr_rej, "n_trades": 0,
            "pnl_rub": 0.0, "win_rate": 0.0, "sharpe": 0.0, "max_dd": 0.0,
        }, rows)
    df = pd.DataFrame(rows)
    pnl = df["realized_pnl"].values
    win = float((pnl > 0).mean())
    df["close_dt"] = pd.to_datetime(df["ts_close"], errors="coerce")
    df["date"] = df["close_dt"].dt.date
    daily = df.groupby("date")["realized_pnl"].sum()
    sharpe = float((daily.mean() / daily.std()) * np.sqrt(252)) if len(daily) > 1 and daily.std() > 0 else 0.0
    cum = pd.Series(pnl).cumsum()
    max_dd = float((cum - cum.cummax()).min())
    return ({
        "fold": fold_meta["fold"],
        "test_start": fold_meta["test_start"].isoformat()[:10],
        "test_end": fold_meta["test_end"].isoformat()[:10],
        "n_in": n_in, "n_blacklist": n_blacklist,
        "n_rr_rej": n_rr_rej, "n_trades": len(rows),
        "pnl_rub": float(pnl.sum()), "win_rate": round(win*100, 1),
        "sharpe": round(sharpe, 2), "max_dd": round(max_dd, 0),
    }, rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2.parquet")
    ap.add_argument("--targets", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2.parquet")
    ap.add_argument("--label", default="v7_walkfwd")
    ap.add_argument("--no-blacklist", action="store_true",
                    help="Disable GAZP blacklist for control comparison")
    ap.add_argument("--blacklist", default=",".join(sorted(DEFAULT_BLACKLIST)),
                    help="Comma-separated tickers to skip")
    ap.add_argument("--step-months", type=int, default=3,
                    help="Walk-forward step in months (Phase 2 default 3; "
                         "use 2 for 19 folds, 1 for monthly).")
    ap.add_argument("--only-folds", default="",
                    help="Comma-separated fold IDs to run (1-indexed). "
                         "Empty=all. Example: '17,18,19'.")
    ap.add_argument("--skip-folds", default="",
                    help="Comma-separated fold IDs to SKIP. Example: '1,2,3'.")
    ap.add_argument("--save-per-fold", action="store_true", default=True,
                    help="Save per-fold trades.csv + metrics.json immediately after "
                         "each fold completes (resilient to crashes).")
    args = ap.parse_args()
    if args.step_months != 3:
        _tp.STEP_MONTHS = args.step_months
        log.info("STEP_MONTHS override -> %d (will affect build_folds)", args.step_months)

    blacklist = set() if args.no_blacklist else {
        t.strip().upper() for t in args.blacklist.split(",") if t.strip()
    }
    log.info("Blacklist: %s", sorted(blacklist) if blacklist else "(none)")

    df, feature_cols = merge_features_and_targets(args.targets, args.features)
    folds = build_folds(df)
    log.info("Folds: %d", len(folds))

    cache = PricesCache()
    cache.warmup()

    only_folds = {int(x.strip()) for x in args.only_folds.split(",") if x.strip()}
    skip_folds = {int(x.strip()) for x in args.skip_folds.split(",") if x.strip()}
    if only_folds:
        log.info("ONLY-FOLDS filter active: %s", sorted(only_folds))
    if skip_folds:
        log.info("SKIP-FOLDS filter active: %s", sorted(skip_folds))

    out_dir = PROJECT_ROOT / "data" / "reenrich_phase2" / "walk_forward" / args.label
    out_dir.mkdir(parents=True, exist_ok=True)
    per_fold_dir = out_dir / "per_fold"
    per_fold_dir.mkdir(exist_ok=True)

    per_fold = []
    all_rows = []
    for fold_idx, fold_meta in enumerate(folds, start=1):
        fold_meta["fold"] = fold_idx
        if only_folds and fold_idx not in only_folds:
            continue
        if fold_idx in skip_folds:
            log.info("FOLD %d SKIPPED by flag", fold_idx)
            continue
        t0 = time.time()
        train_df = df[fold_meta["train_mask"]].copy()
        test_df = df[fold_meta["test_mask"]].copy()
        log.info("FOLD %d  train_n=%d  test_n=%d", fold_meta["fold"],
                 len(train_df), len(test_df))
        models = train_models_for_fold(train_df, feature_cols)
        log.info("  trained %d models", len(models))
        # Free train_df memory before simulating
        del train_df
        gc.collect()
        metrics, rows = simulate_fold(fold_meta, test_df, feature_cols,
                                       models, cache, blacklist)
        log.info("  trades=%d pnl=%+.0f win=%.1f%% sharpe=%.2f  (%.1fs)",
                 metrics["n_trades"], metrics["pnl_rub"], metrics["win_rate"],
                 metrics["sharpe"], time.time() - t0)
        per_fold.append(metrics)
        all_rows.extend(rows)

        # === Sprint 6.1 fix: per-fold checkpoint save (resilient to crashes) ===
        if args.save_per_fold:
            try:
                fold_metrics_path = per_fold_dir / f"fold_{fold_idx:02d}_metrics.json"
                fold_metrics_path.write_text(
                    json.dumps(metrics, indent=2, ensure_ascii=False, default=str),
                    encoding="utf-8",
                )
                if rows:
                    fold_trades_path = per_fold_dir / f"fold_{fold_idx:02d}_trades.csv"
                    pd.DataFrame(rows).to_csv(fold_trades_path, index=False)
                log.info("  saved fold %d -> %s", fold_idx, per_fold_dir.name)
            except Exception as e:
                log.warning("  per-fold save failed: %s", e)

        # === Memory hygiene between folds ===
        del models, test_df, rows
        gc.collect()

    # Aggregate
    sharpes = [r["sharpe"] for r in per_fold if r["n_trades"] > 0]
    pnls = [r["pnl_rub"] for r in per_fold]
    n_trades = [r["n_trades"] for r in per_fold]
    summary = {
        "label": args.label,
        "blacklist": sorted(blacklist),
        "n_folds": len(per_fold),
        "n_folds_with_trades": len(sharpes),
        "n_folds_positive_pnl": sum(1 for p in pnls if p > 0),
        "mean_sharpe": round(float(np.mean(sharpes)), 2) if sharpes else 0.0,
        "median_sharpe": round(float(np.median(sharpes)), 2) if sharpes else 0.0,
        "min_sharpe": round(float(np.min(sharpes)), 2) if sharpes else 0.0,
        "total_trades": sum(n_trades),
        "mean_trades_per_fold": round(np.mean(n_trades), 1) if n_trades else 0.0,
        "total_pnl_rub": round(sum(pnls), 0),
    }
    log.info("=" * 60)
    log.info("WALK-FORWARD v7 SUMMARY")
    log.info("=" * 60)
    for k, v in summary.items():
        log.info("  %-28s %s", k, v)

    log.info("")
    log.info("Per fold:")
    for r in per_fold:
        log.info("  Fold %2d %s..%s  trades=%4d pnl=%+9.0f win=%5.1f%% sharpe=%5.2f",
                 r["fold"], r["test_start"], r["test_end"],
                 r["n_trades"], r["pnl_rub"], r["win_rate"], r["sharpe"])

    (out_dir / "summary.json").write_text(json.dumps({
        "summary": summary, "per_fold": per_fold,
    }, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    if all_rows:
        pd.DataFrame(all_rows).to_csv(out_dir / "trades.csv", index=False)
    log.info("Saved: %s", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
