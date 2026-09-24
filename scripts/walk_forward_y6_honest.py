r"""Sprint 6.3/6.4 — Walk-forward over the Y6 period with HONEST Sber costs.

Sprint 6.4 Phase A additions:
  --horizon {60m,120m,240m,eod,t1} — holding horizon; eod = entry day 18:45
  MSK, t1 = next trading day 18:45.  Trades are simulated GROSS (cost=0);
  the v2 cost model (scripts/costs_sber.py: day-turnover-tiered brokerage,
  exit-type slippage, overnight funding) is applied vectorized at aggregation.

Design (per Sprint 6.3 brief + 2026-06-10 decisions):
  Train:  rolling N-month window from the v2 corpus (Phase 2-DI + Y6, 2022→2026,
          features_mfe_v2.parquet — same 78 features the prod v7 models use).
  Test:   2-week windows sliding over the Y6 period (default 2025-01-01 → end
          of data), step 2 weeks → ~37 folds.
  Costs:  per-asset-class round-trip on notional (scripts/costs_sber.py):
          stocks ~0.19%, currencies ~0.50%, futures ~0.08%.
  Sizing: production compute_size (risk 0.5% × 500k equity, leverage 10×,
          floor 1 lot) — same formula as live Decision.
  Filter: LENIENT B_filter (src/services/decision/filter.py semantics)
          reproduced from feature cols dir_long/dir_short/dir_neutral +
          confidence. NOTE: the Sprint 6.1 harness (walk_forward_v7_all_folds)
          never applied the filter despite its docstring.

Two-phase architecture (the sweep optimization):
  Phase A/B (expensive, ONCE): per fold train 60m XGBoost models → predict
      every test row → simulate BOTH sides per row through sprint4
      BaselineFixedTpSl → outcomes.parquet.  Trade outcomes don't depend on
      rr_threshold / min_mfe / dir_conf / whitelist, so they are cached.
  Phase C (cheap, per combo): vectorized selection + aggregation over
      outcomes.parquet.  Baseline combo runs here; the full 240-point grid
      lives in scripts/sweep_y6_grid.py.

Usage:
    python scripts/walk_forward_y6_honest.py --label y6_honest_costs_baseline
    python scripts/walk_forward_y6_honest.py --only-folds 1,2,3   # partial
    python scripts/walk_forward_y6_honest.py --resume             # skip done folds
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

from scripts.train_predictor_fold13 import (  # noqa: E402
    TARGETS,
    compute_sample_weights, train_single_model,
)
from scripts.costs_sber import (  # noqa: E402
    WHITELIST_STRATEGIES, apply_costs_v2,
)

from src.services.decision.sizing import compute_size  # noqa: E402

from base import Trade as Sprint4Trade  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402
from instruments import all_canonical_tickers, get_lot_size, normalize_ticker  # noqa: E402
from prices_cache import PricesCache  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wf_y6_honest")

# --- Decision params shared with production (NOT swept) ---
MIN_MAE_PCT = 0.05      # floor in R:R division (DecisionSettings.min_mae_pct)
TP_FRACTION = 0.7
SL_BUFFER = 1.2
SL_FLOOR_PCT = 0.0005
TP_FLOOR_PCT = 0.001

# --- Horizons (Sprint 6.4 Phase A) ---
# Numeric → fixed minutes; eod → entry day 18:45 MSK; t1 → next trading day
# 18:45.  Target column suffix matches derive_targets_multi_horizon.py.
EOD_CUTOFF_H, EOD_CUTOFF_M = 18, 45
HORIZON_SPECS: dict[str, dict] = {
    "60m": {"suffix": "60m", "minutes": 60},
    "120m": {"suffix": "120m", "minutes": 120},
    "240m": {"suffix": "240m", "minutes": 240},
    "eod": {"suffix": "eod", "minutes": None},
    "t1": {"suffix": "t1", "minutes": None},
}

# --- Production sizing constants (DecisionSettings defaults) ---
EQUITY_RUB = 500_000.0
RISK_PER_TRADE_PCT = 0.005
LEVERAGE = 10
LOT_SIZES = {t: get_lot_size(t) for t in all_canonical_tickers()}

# --- Baseline combo (Task 2; current prod config) ---
BASELINE_COMBO = dict(rr_threshold=1.0, min_mfe_pct=0.0,
                      dir_conf=0.5, whitelist="full")


# ---------------------------------------------------------------------------
# Clean features×targets merge.
#
# Sprint 6.3 forensic (2026-06-10): train_predictor_fold13.merge_features_and_
# targets joins on `id` ONLY and drops targets' `ticker`.  Phase 2 ids are
# per-(event,ticker) so that was 1:1 historically, but Y6 ids are per-EVENT —
# a multi-ticker event cross-joins every its feature row with every its target
# row (~3.6× row inflation, wrong-ticker labels in train).  v7_70b_v2 was
# trained through that bug (fold13 train_n=226,125 from 136,798 real rows).
# Here: merge on (id, canonical ticker) → strictly 1:1.
# ---------------------------------------------------------------------------
def merge_features_targets_clean(targets_path: Path, features_path: Path,
                                 ) -> tuple[pd.DataFrame, list[str]]:
    targets = pd.read_parquet(targets_path)
    features = pd.read_parquet(features_path)
    log.info("Targets: %d rows, Features: %d rows", len(targets), len(features))

    def _canon(x) -> str | None:
        try:
            return normalize_ticker(str(x))
        except KeyError:
            return None

    targets = targets.copy()
    features = features.copy()
    targets["id"] = targets["id"].astype(str)
    features["_id"] = features["_id"].astype(str)
    targets["_tk_t"] = targets["ticker"].map(_canon)
    features["_tk_f"] = features["_ticker"].map(_canon)
    n_t, n_f = int(targets["_tk_t"].isna().sum()), int(features["_tk_f"].isna().sum())
    if n_t or n_f:
        log.warning("dropping unknown tickers: %d target rows, %d feature rows", n_t, n_f)
        targets = targets[targets["_tk_t"].notna()]
        features = features[features["_tk_f"].notna()]

    df = features.merge(
        targets.drop(columns=["datetime", "ticker"]),
        left_on=["_id", "_tk_f"], right_on=["id", "_tk_t"], how="inner",
    )
    df["_ticker"] = df["_tk_f"]  # canonical everywhere downstream
    df = df.drop(columns=["_tk_f", "_tk_t"])
    df["_dt"] = pd.to_datetime(df["_datetime"])
    df = df.sort_values("_dt").reset_index(drop=True)
    dup = int(df.duplicated(subset=["_id", "_ticker"]).sum())
    if dup:
        log.warning("post-merge duplicate (_id,_ticker) rows: %d — dropping", dup)
        df = df.drop_duplicates(subset=["_id", "_ticker"]).reset_index(drop=True)
    log.info("Merged CLEAN: %d rows (1:1 on id+ticker)", len(df))

    meta_cols = {"_id", "_datetime", "_dt", "_ticker", "id", "ticker",
                 "_entry_price", "_entry_ts"}
    target_suffixes = ([f"{H}m" for H in (1, 2, 3, 4, 5, 10, 15, 30, 45, 60,
                                          120, 240)] + ["eod", "t1"])
    all_target_cols = {f"{t}_{s}" for s in target_suffixes for t in TARGETS}
    all_target_cols |= {f"ret_{s}" for s in target_suffixes}
    feature_cols = [c for c in df.columns
                    if c not in meta_cols and c not in all_target_cols]
    log.info("Feature cols: %d", len(feature_cols))
    return df, feature_cols


# ---------------------------------------------------------------------------
# Folds: rolling train window, fixed-day test windows over the Y6 period
# ---------------------------------------------------------------------------
def build_rolling_folds(df: pd.DataFrame, train_months: int, test_days: int,
                        step_days: int, test_from: pd.Timestamp,
                        test_to: pd.Timestamp) -> list[dict]:
    purge = pd.Timedelta(minutes=30)
    folds: list[dict] = []
    test_start = test_from
    k = 0
    while test_start < test_to:
        test_end = min(test_start + pd.Timedelta(days=test_days), test_to)
        train_start = test_start - pd.DateOffset(months=train_months)
        train_end = test_start - purge
        train_mask = (df["_dt"] >= train_start) & (df["_dt"] < train_end)
        test_mask = (df["_dt"] >= test_start) & (df["_dt"] < test_end)
        n_train, n_test = int(train_mask.sum()), int(test_mask.sum())
        k += 1
        if n_train >= 500 and n_test >= 10:
            folds.append({
                "fold": len(folds) + 1,
                "train_start": train_start, "train_end": train_end,
                "test_start": test_start, "test_end": test_end,
                "train_mask": train_mask, "test_mask": test_mask,
            })
        else:
            log.info("  skip window %s..%s (train_n=%d test_n=%d)",
                     test_start.date(), test_end.date(), n_train, n_test)
        test_start += pd.Timedelta(days=step_days)
    log.info("Folds generated: %d", len(folds))
    for f in folds:
        log.info("  Fold %2d: test [%s → %s] train [%s →] train_n=%d test_n=%d",
                 f["fold"], f["test_start"].date(), f["test_end"].date(),
                 f["train_start"].date(),
                 int(f["train_mask"].sum()), int(f["test_mask"].sum()))
    return folds


# ---------------------------------------------------------------------------
# Per-fold: train 60m models only (4 targets × general/mx_specific)
# ---------------------------------------------------------------------------
def train_fold_models(train_df: pd.DataFrame, feature_cols: list[str],
                      suffix: str) -> dict:
    target_cols = [f"{t}_{suffix}" for t in TARGETS]
    X_train_full = train_df[feature_cols].values
    sample_weights = compute_sample_weights(train_df["_ticker"])
    mx_mask = (train_df["_ticker"] == "MIX").values
    X_train_mx = train_df.loc[mx_mask, feature_cols].values if mx_mask.sum() >= 500 else None

    models: dict[tuple, object] = {}
    for target_col in target_cols:
        y_full = train_df[target_col].values
        m = train_single_model(X_train_full, y_full, sample_weights,
                               label=f"{target_col}/general")
        if m is not None:
            models[(target_col, "general")] = m
        if X_train_mx is not None:
            y_mx = train_df.loc[mx_mask, target_col].values
            m_mx = train_single_model(X_train_mx, y_mx, None,
                                      label=f"{target_col}/mx")
            if m_mx is not None:
                models[(target_col, "mx_specific")] = m_mx
    return models


def predict_test_matrix(models: dict, test_df: pd.DataFrame,
                        feature_cols: list[str], suffix: str) -> pd.DataFrame | None:
    """Vectorized prediction for all test rows. Returns df with pred columns,
    or None when a general model is missing (too few clean train samples)."""
    X = test_df[feature_cols].values.astype(np.float64)
    out = pd.DataFrame(index=test_df.index)
    is_mx = (test_df["_ticker"] == "MIX").values
    for t in TARGETS:
        col = f"{t}_{suffix}"
        m_gen = models.get((col, "general"))
        if m_gen is None:
            log.warning("missing general model for %s — fold skipped", col)
            return None
        pred = m_gen.predict(X)
        m_mx = models.get((col, "mx_specific"))
        if m_mx is not None and is_mx.any():
            pred_mx = m_mx.predict(X[is_mx])
            pred = pred.copy()
            pred[is_mx] = pred_mx
        out[f"pred_{t}"] = np.maximum(0.0, pred)
    out["rr_long"] = out["pred_mfe_long"] / np.maximum(out["pred_mae_long"], MIN_MAE_PCT)
    out["rr_short"] = out["pred_mfe_short"] / np.maximum(out["pred_mae_short"], MIN_MAE_PCT)
    return out


# ---------------------------------------------------------------------------
# Per-fold: simulate BOTH sides for every test row (sweep-invariant outcomes)
# ---------------------------------------------------------------------------
def horizon_end_for_entry(spec: dict, entry_ts: pd.Timestamp,
                          bars: pd.DataFrame) -> pd.Timestamp | None:
    """End timestamp of the horizon window for this entry, or None to skip."""
    if spec["minutes"] is not None:
        return entry_ts + pd.Timedelta(minutes=spec["minutes"])
    eod = entry_ts.normalize() + pd.Timedelta(hours=EOD_CUTOFF_H, minutes=EOD_CUTOFF_M)
    if spec["suffix"] == "eod":
        # news entering near/after main-session close → no eod window
        return eod if entry_ts < eod - pd.Timedelta(minutes=15) else None
    # t1: first bar strictly after the entry day → that day's 18:45
    idx = bars.index.searchsorted(entry_ts.normalize() + pd.Timedelta(days=1))
    if idx >= len(bars):
        return None
    return (bars.index[idx].normalize()
            + pd.Timedelta(hours=EOD_CUTOFF_H, minutes=EOD_CUTOFF_M))


def simulate_fold_outcomes(fold_meta: dict, test_df: pd.DataFrame,
                           preds: pd.DataFrame, cache: PricesCache,
                           spec: dict) -> list[dict]:
    strategy = BaselineFixedTpSl()  # Phase 2 window mode (harness continuity)
    rows: list[dict] = []
    n_no_bars = 0
    # bars fetch window: fixed horizons keep the Phase 2 ×1.5 margin;
    # eod/t1 need up to 4 calendar days (weekend-safe)
    if spec["minutes"] is not None:
        fetch_td = pd.Timedelta(minutes=int(spec["minutes"] * 1.5) + 2)
    else:
        fetch_td = pd.Timedelta(days=4)

    for idx, row in test_df.iterrows():
        ticker = row["_ticker"]
        try:
            news_ts = pd.Timestamp(row["_dt"])
        except Exception:
            continue
        p = preds.loc[idx]
        try:
            bars = cache.get_bars(ticker, news_ts, news_ts + fetch_td)
        except (FileNotFoundError, KeyError):
            n_no_bars += 1
            continue
        if bars is None or len(bars) == 0:
            n_no_bars += 1
            continue
        next_min = (news_ts + pd.Timedelta(seconds=60)).floor("min")
        idx_e = bars.index.searchsorted(next_min)
        if idx_e >= len(bars):
            n_no_bars += 1
            continue
        entry = float(bars["open"].iloc[idx_e])
        if entry <= 0:
            continue
        entry_ts = bars.index[idx_e]
        end_ts = horizon_end_for_entry(spec, entry_ts, bars)
        if end_ts is None or end_ts <= entry_ts:
            continue
        horizon_min = max(1, int((end_ts - entry_ts).total_seconds() // 60))
        lot = LOT_SIZES.get(ticker, 1)

        # LLM fields for the B_filter (LENIENT) at selection time
        if row.get("dir_long", 0.0) >= 0.5:
            llm_dir = "long"
        elif row.get("dir_short", 0.0) >= 0.5:
            llm_dir = "short"
        else:
            llm_dir = "neutral"
        conf = float(row.get("confidence", 0.0))

        for side, mfe, mae in (
            ("BUY", float(p["pred_mfe_long"]), float(p["pred_mae_long"])),
            ("SELL", float(p["pred_mfe_short"]), float(p["pred_mae_short"])),
        ):
            tp_dist_pct = max(mfe * TP_FRACTION / 100, TP_FLOOR_PCT)
            sl_dist_pct = max(mae * SL_BUFFER / 100, SL_FLOOR_PCT)
            if side == "BUY":
                tp_price = entry * (1 + tp_dist_pct)
                sl_price = entry * (1 - sl_dist_pct)
            else:
                tp_price = entry * (1 - tp_dist_pct)
                sl_price = entry * (1 + sl_dist_pct)
            sl_dist_abs = abs(entry - sl_price)

            sizing = compute_size(
                ticker=ticker, side=side, entry_price=entry,
                sl_dist_abs=sl_dist_abs, pred_mfe_pct=mfe,
                tp_fraction=TP_FRACTION, equity_rub=EQUITY_RUB,
                risk_per_trade_pct=RISK_PER_TRADE_PCT, leverage=LEVERAGE,
                lot_sizes=LOT_SIZES,
            )
            qty = sizing.quantity
            notional = entry * lot * qty

            # Sprint 6.4: simulate GROSS (cost_rub=0) — v2 costs are applied
            # vectorized at aggregation time (brokerage needs the selected
            # set's day turnover; slippage needs exit_reason).
            t = Sprint4Trade(
                ticker=ticker, fold=fold_meta["fold"], horizon_min=horizon_min,
                rr_threshold=0.0, model_type="general",
                ts_open=entry_ts, side=1 if side == "BUY" else -1,
                entry=entry, size_lots=qty,
                sl_price=sl_price, tp_price=tp_price,
                pred_mfe_pct=mfe, pred_mae_pct=mae,
                ts_close_phase2=entry_ts + pd.Timedelta(minutes=horizon_min),
                exit_price_phase2=0.0, exit_reason_phase2="unknown",
                net_pnl_rub_phase2=0.0, cost_rub=0.0,
            )
            try:
                res = strategy.simulate(t, bars)
            except Exception:
                continue
            if res is None or res.exit_reason == "no_bars":
                continue
            rows.append({
                "fold": fold_meta["fold"],
                "id": row["_id"], "ticker": ticker, "side": side,
                "news_ts": news_ts, "entry_ts": entry_ts,
                "ts_close": res.ts_close, "exit_reason": res.exit_reason,
                "entry": entry, "exit_price": res.exit_price,
                "quantity": qty, "notional_rub": round(notional, 2),
                "gross_pnl": res.realized_pnl,
                "realized_r": res.realized_r,
                "duration_min": res.duration_min,
                "pred_mfe": mfe, "pred_mae": mae,
                # selection columns (identical for both side-rows)
                "rr_long": float(p["rr_long"]), "rr_short": float(p["rr_short"]),
                "mfe_long": float(p["pred_mfe_long"]),
                "mfe_short": float(p["pred_mfe_short"]),
                "llm_dir": llm_dir, "confidence": conf,
            })
    if n_no_bars:
        log.info("  fold %d: %d test rows skipped (no bars)",
                 fold_meta["fold"], n_no_bars)
    return rows


# ---------------------------------------------------------------------------
# Phase C: combo selection + aggregation (shared with sweep_y6_grid.py)
# ---------------------------------------------------------------------------
def select_trades(outcomes: pd.DataFrame, rr_threshold: float, min_mfe_pct: float,
                  dir_conf: float, whitelist: str) -> pd.DataFrame:
    """Apply universe + production R:R side choice + LENIENT B_filter.

    Side choice replicates src/services/decision/rr_logic.py exactly:
      BUY  if rr_long ≥ thr and mfe_long ≥ min_mfe and rr_long ≥ rr_short
      SELL if rr_short ≥ thr and mfe_short ≥ min_mfe and rr_short > rr_long
    LENIENT filter (src/services/decision/filter.py): include neutral; reject
    explicit direction mismatch or matched-direction confidence < dir_conf.
    """
    allowed = WHITELIST_STRATEGIES[whitelist]
    df = outcomes[outcomes["ticker"].isin(allowed)]

    buy_ok = (
        (df["rr_long"] >= rr_threshold)
        & (df["mfe_long"] >= min_mfe_pct)
        & (df["rr_long"] >= df["rr_short"])
        & (df["side"] == "BUY")
    )
    sell_ok = (
        (df["rr_short"] >= rr_threshold)
        & (df["mfe_short"] >= min_mfe_pct)
        & (df["rr_short"] > df["rr_long"])
        & (df["side"] == "SELL")
    )
    df = df[buy_ok | sell_ok]

    # LENIENT B_filter
    expected_dir = df["side"].map({"BUY": "long", "SELL": "short"})
    neutral = df["llm_dir"] == "neutral"
    match = df["llm_dir"] == expected_dir
    keep = neutral | (match & (df["confidence"] >= dir_conf))
    return df[keep]


def aggregate_trades(trades: pd.DataFrame, n_folds_total: int) -> dict:
    """v2 costs over the selected set, then headline metrics on NET PnL."""
    if trades.empty:
        return {
            "n_trades": 0, "total_pnl_rub": 0.0, "total_gross_rub": 0.0,
            "total_cost_rub": 0.0, "cost_brokerage_rub": 0.0,
            "cost_slippage_rub": 0.0, "cost_moex_rub": 0.0,
            "cost_funding_rub": 0.0,
            "mean_pnl_per_trade_rub": 0.0, "win_rate_pct": 0.0,
            "sharpe_overall": 0.0, "mean_fold_sharpe": 0.0,
            "median_fold_sharpe": 0.0, "n_folds_with_trades": 0,
            "n_folds_total": n_folds_total, "n_folds_positive_pnl": 0,
            "exit_reasons": {}, "per_ticker": {}, "per_fold": [],
        }
    t = apply_costs_v2(trades, equity_rub=EQUITY_RUB)
    t["close_dt"] = pd.to_datetime(t["ts_close"], errors="coerce")
    t["date"] = t["close_dt"].dt.date
    daily = t.groupby("date")["net_pnl"].sum()
    sharpe_overall = (
        float((daily.mean() / daily.std()) * np.sqrt(252))
        if len(daily) > 1 and daily.std() > 0 else 0.0
    )
    pnl = t["net_pnl"].values

    per_fold = []
    for fold, g in t.groupby("fold"):
        fdaily = g.groupby("date")["net_pnl"].sum()
        fsharpe = (
            float((fdaily.mean() / fdaily.std()) * np.sqrt(252))
            if len(fdaily) > 1 and fdaily.std() > 0 else 0.0
        )
        per_fold.append({
            "fold": int(fold), "n_trades": int(len(g)),
            "pnl_rub": round(float(g["net_pnl"].sum()), 2),
            "win_rate_pct": round(float((g["net_pnl"] > 0).mean()) * 100, 1),
            "sharpe": round(fsharpe, 2),
        })
    fold_pnls = [f["pnl_rub"] for f in per_fold]
    fold_sharpes = [f["sharpe"] for f in per_fold if f["n_trades"] > 0]

    by_exit = t["exit_reason"].value_counts().to_dict()
    by_ticker = (
        t.groupby("ticker")["net_pnl"]
        .agg(["count", "sum"]).round(2)
        .rename(columns={"count": "n", "sum": "pnl_rub"})
        .sort_values("pnl_rub")
    )
    return {
        "n_trades": int(len(t)),
        "total_pnl_rub": round(float(pnl.sum()), 2),
        "total_gross_rub": round(float(t["gross_pnl"].sum()), 2),
        "total_cost_rub": round(float(t["cost_total_rub"].sum()), 2),
        "cost_brokerage_rub": round(float(t["cost_brokerage_rub"].sum()), 2),
        "cost_slippage_rub": round(float(t["cost_slippage_rub"].sum()), 2),
        "cost_moex_rub": round(float(t["cost_moex_rub"].sum()), 2),
        "cost_funding_rub": round(float(t["cost_funding_rub"].sum()), 2),
        "mean_pnl_per_trade_rub": round(float(pnl.mean()), 2),
        "win_rate_pct": round(float((pnl > 0).mean()) * 100, 2),
        "sharpe_overall": round(sharpe_overall, 2),
        "mean_fold_sharpe": round(float(np.mean(fold_sharpes)), 2) if fold_sharpes else 0.0,
        "median_fold_sharpe": round(float(np.median(fold_sharpes)), 2) if fold_sharpes else 0.0,
        "n_folds_with_trades": len(fold_sharpes),
        "n_folds_total": n_folds_total,
        "n_folds_positive_pnl": sum(1 for p in fold_pnls if p > 0),
        "exit_reasons": by_exit,
        "per_ticker": by_ticker.to_dict("index"),
        "per_fold": per_fold,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2.parquet")
    ap.add_argument("--targets", type=Path, default=None,
                    help="Default: targets_mfe_v2.parquet for 60m, "
                         "targets_multi_horizon.parquet otherwise.")
    ap.add_argument("--horizon", default="60m", choices=sorted(HORIZON_SPECS),
                    help="Holding horizon: 60m/120m/240m/eod/t1.")
    ap.add_argument("--label", default=None,
                    help="Default: y6_honest_<horizon>.")
    ap.add_argument("--train-months", type=int, default=12)
    ap.add_argument("--test-days", type=int, default=14)
    ap.add_argument("--step-days", type=int, default=14)
    ap.add_argument("--test-from", default="2025-01-01")
    ap.add_argument("--test-to", default="",
                    help="Empty = end of data.")
    ap.add_argument("--only-folds", default="")
    ap.add_argument("--skip-folds", default="")
    ap.add_argument("--resume", action="store_true",
                    help="Skip folds whose per_fold outcome parquet already exists.")
    args = ap.parse_args()

    spec = HORIZON_SPECS[args.horizon]
    if args.targets is None:
        args.targets = PROJECT_ROOT / "data" / "reenrich_phase2" / (
            "targets_mfe_v2.parquet" if args.horizon == "60m"
            else "targets_multi_horizon.parquet")
    if args.label is None:
        args.label = f"y6_honest_{args.horizon}"
    log.info("HORIZON=%s  targets=%s  label=%s",
             args.horizon, args.targets.name, args.label)

    out_dir = PROJECT_ROOT / "data" / "walk_forward" / args.label
    out_dir.mkdir(parents=True, exist_ok=True)
    per_fold_dir = out_dir / "per_fold"
    per_fold_dir.mkdir(exist_ok=True)

    df, feature_cols = merge_features_targets_clean(args.targets, args.features)
    log.info("Feature cols: %d  rows: %d  tickers: %s",
             len(feature_cols), len(df), sorted(df["_ticker"].unique()))
    # Drop rows with no target for this horizon (e.g. eod-NaN for late news)
    tgt_col = f"mfe_long_{spec['suffix']}"
    if tgt_col not in df.columns:
        log.error("target column %s not in merged df — wrong --targets file?", tgt_col)
        return 1

    test_from = pd.Timestamp(args.test_from)
    test_to = pd.Timestamp(args.test_to) if args.test_to else df["_dt"].max()
    folds = build_rolling_folds(df, args.train_months, args.test_days,
                                args.step_days, test_from, test_to)
    if not folds:
        log.error("no folds")
        return 1

    cache = PricesCache()
    cache.warmup()

    only_folds = {int(x) for x in args.only_folds.split(",") if x.strip()}
    skip_folds = {int(x) for x in args.skip_folds.split(",") if x.strip()}

    fold_summaries = []
    for fold_meta in folds:
        k = fold_meta["fold"]
        if only_folds and k not in only_folds:
            continue
        if k in skip_folds:
            continue
        ck_path = per_fold_dir / f"fold_{k:02d}_outcomes.parquet"
        if args.resume and ck_path.exists():
            log.info("FOLD %d already done (resume) — skip", k)
            continue
        t0 = time.time()
        train_df = df[fold_meta["train_mask"]]
        test_df = df[fold_meta["test_mask"]]
        log.info("FOLD %d  train_n=%d test_n=%d  [%s → %s]",
                 k, len(train_df), len(test_df),
                 fold_meta["test_start"].date(), fold_meta["test_end"].date())
        models = train_fold_models(train_df, feature_cols, spec["suffix"])
        preds = predict_test_matrix(models, test_df, feature_cols, spec["suffix"])
        if preds is None:
            del models, train_df, test_df
            gc.collect()
            continue
        rows = simulate_fold_outcomes(fold_meta, test_df, preds, cache, spec)
        if rows:
            pd.DataFrame(rows).to_parquet(ck_path)
        fold_summaries.append({
            "fold": k,
            "test_start": str(fold_meta["test_start"].date()),
            "test_end": str(fold_meta["test_end"].date()),
            "train_n": int(len(train_df)), "test_n": int(len(test_df)),
            "n_outcome_rows": len(rows),
            "wall_sec": round(time.time() - t0, 1),
        })
        log.info("  fold %d done: %d outcome rows (%.1fs)",
                 k, len(rows), time.time() - t0)
        del models, preds, rows, train_df, test_df
        gc.collect()

    (out_dir / "folds_meta.json").write_text(
        json.dumps(fold_summaries, indent=2, ensure_ascii=False), encoding="utf-8")

    # Concat all per-fold outcomes (includes folds from previous --resume runs)
    parts = sorted(per_fold_dir.glob("fold_*_outcomes.parquet"))
    if not parts:
        log.error("no outcomes saved")
        return 1
    outcomes = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    outcomes.to_parquet(out_dir / "outcomes.parquet")
    log.info("Outcomes total: %d rows -> %s", len(outcomes),
             out_dir / "outcomes.parquet")

    # --- Phase C: baseline combo (Task 2) ---
    trades = select_trades(outcomes, **{
        "rr_threshold": BASELINE_COMBO["rr_threshold"],
        "min_mfe_pct": BASELINE_COMBO["min_mfe_pct"],
        "dir_conf": BASELINE_COMBO["dir_conf"],
        "whitelist": BASELINE_COMBO["whitelist"],
    })
    n_folds_total = len({p.name for p in parts})
    summary = aggregate_trades(trades, n_folds_total=n_folds_total)
    summary["combo"] = BASELINE_COMBO
    summary["label"] = args.label

    per_fold_rows = summary.pop("per_fold")
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8")
    pd.DataFrame(per_fold_rows).to_csv(out_dir / "per_fold.csv", index=False)
    trades.to_parquet(out_dir / "all_trades.parquet")

    log.info("=" * 64)
    log.info("BASELINE %s (rr=%.2f mfe>=%.4f conf>=%.2f wl=%s)",
             args.horizon,
             BASELINE_COMBO["rr_threshold"], BASELINE_COMBO["min_mfe_pct"],
             BASELINE_COMBO["dir_conf"], BASELINE_COMBO["whitelist"])
    for key in ("n_trades", "total_pnl_rub", "total_gross_rub",
                "total_cost_rub", "cost_brokerage_rub", "cost_slippage_rub",
                "cost_moex_rub", "cost_funding_rub",
                "mean_pnl_per_trade_rub", "win_rate_pct", "sharpe_overall",
                "mean_fold_sharpe", "n_folds_positive_pnl", "n_folds_total"):
        log.info("  %-26s %s", key, summary.get(key))
    log.info("Saved: %s", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
