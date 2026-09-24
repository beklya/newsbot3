r"""
scripts/walk_forward_backtest.py
==================================
Phase 2-style walk-forward backtest: 13 folds, train per fold, aggregate Sharpe.

КЛЮЧ Sprint 5.7 — успех metric = mean Sharpe ≥ +4 across 13 folds.

Архитектура (mimics newsbot2/backtest_mfe.py):
  - Walk-forward: TRAIN_MONTHS=12, TEST_MONTHS=3, STEP=3, PURGE=30min
  - 13 фолдов (test_start 2023-01..2026-01)
  - Каждый фолд:
    a) Train 16 моделей (4 targets × 2 horizons × 2 model_types) на expanding train
    b) Predict на test rows
    c) Decision logic: h=60, rr=2.0, USE_MX_SPECIFIC=True, MIN_CONFIDENCE=0.55
    d) Simulate trades с TP/SL/time-stop, real costs
    e) Compute fold Sharpe, win rate, PnL

Aggregate: mean/median Sharpe across 13 folds.

Sanity check expectations (Sprint 5.7 plan):
  - Legacy features_mfe.parquet → mean Sharpe ≈ +4.87 (= phase2_mfe_sharpe.xlsx row)
  - Если отклонение > 10% → bug в port'е

Usage:
  python scripts/walk_forward_backtest.py \\
      --features "D:\\quik_sber\\newsbot\\newsbot2\\решение проблем\\Проблема 5 - новое начало\\phase2_mfe\\features_mfe.parquet" \\
      --label legacy_baseline

  python scripts/walk_forward_backtest.py \\
      --features data\\reenrich_phase2\\features_mfe_v6_8b.parquet \\
      --label v6_8b_v2_1_0

Output:
  data/reenrich_phase2/walk_forward/<label>/walk_forward_<label>.xlsx (per_fold + summary sheets)
  data/reenrich_phase2/walk_forward/<label>/walk_forward_<label>_trades.parquet
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_DIR = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe")

# ============================================================
# Phase 2 walk-forward constants (mirror train_predictor_fold13.py + backtest_mfe.py)
# ============================================================
TRAIN_MONTHS = 12
TEST_MONTHS = 3
STEP_MONTHS = 3
PURGE = pd.Timedelta(minutes=30)
MIN_TRAIN_SAMPLES = 500
MIN_TEST_SAMPLES = 30
MIN_MX_SAMPLES = 500

XGB_PARAMS = dict(
    objective="reg:squarederror",
    n_estimators=150,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.7,
    verbosity=0,
    tree_method="hist",
    n_jobs=-1,
)

# Sprint 5.7 best combo (mx_specific h=60 rr=2 from Phase 2 mfe_sharpe.xlsx)
HORIZON = 60
RR_THRESHOLD = 2.0
USE_MX_SPECIFIC = True
MIN_MFE_PCT = 0.15
MIN_MAE_PCT = 0.05
TP_FRACTION = 0.7
SL_BUFFER = 1.2

INITIAL_EQUITY = 500_000.0
LEVERAGE = 10
RISK_PER_TRADE_PCT = 0.005
DAILY_KILL_PCT = 0.02
MAX_OPEN_POSITIONS = 3
COOLDOWN_TICKER_SEC = 60
MIN_CONFIDENCE = 0.55

COSTS_RT_PCT = {
    "SBER": 0.08, "GAZP": 0.08, "LKOH": 0.08, "YNDX": 0.08, "ROSN": 0.08,
    "TATN": 0.08, "GMKN": 0.08, "NVTK": 0.08, "VTBR": 0.08, "MGNT": 0.08,
    "MTSS": 0.08, "PLZL": 0.08,
    "Si": 0.03, "MX": 0.03, "BR": 0.03, "NG": 0.03, "GOLD": 0.03,
    "CNY": 0.03, "USDRUB": 0.03,
}
SLIPPAGE_RT_PCT = {
    "SBER": 0.04, "GAZP": 0.04, "LKOH": 0.04,
    "YNDX": 0.06, "ROSN": 0.06, "TATN": 0.06, "GMKN": 0.06,
    "NVTK": 0.10, "VTBR": 0.10, "MGNT": 0.10, "MTSS": 0.10, "PLZL": 0.10,
    "Si": 0.02, "MX": 0.02, "BR": 0.02,
    "NG": 0.04, "GOLD": 0.04, "CNY": 0.04, "USDRUB": 0.05,
}
LOT_SIZES = {
    "SBER": 10, "GAZP": 10, "LKOH": 1, "YNDX": 1, "ROSN": 10,
    "GMKN": 1, "NVTK": 1, "TATN": 1, "MGNT": 1, "MTSS": 10,
    "PLZL": 1, "VTBR": 10000,
    "Si": 1, "MX": 1, "BR": 1, "NG": 1, "GOLD": 1, "CNY": 1, "USDRUB": 1000,
}
TICKER_TO_PREFIX = {
    "SBER": "SBER", "GAZP": "GAZP", "LKOH": "LKOH",
    "YNDX": "YDEX", "ROSN": "ROSN", "NVTK": "NVTK",
    "VTBR": "VTBR", "GMKN": "GMKN", "MGNT": "MGNT",
    "MTSS": "MTSS", "TATN": "TATN", "PLZL": "PLZL",
    "Si": "SI", "MX": "MIX", "BR": "BR",
    "NG": "NG", "GOLD": "GLDRUB", "CNY": "CNY", "USDRUB": "USDRUB",
}

DEFAULT_PRICES_DIR = Path(r"D:\quik_sber\newsbot\prices")
DEFAULT_TARGETS = PHASE2_DIR / "targets_mfe.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("walk_forward")


def total_cost_pct(ticker: str) -> float:
    return COSTS_RT_PCT.get(ticker, 0.10) + SLIPPAGE_RT_PCT.get(ticker, 0.05)


# ============================================================
# Candle loading (Phase 2 reader, port from ab_backtest_v1_holdout.py)
# ============================================================
def _read_candles_csv(path: Path) -> Optional[pd.DataFrame]:
    with open(path, encoding="utf-8") as fp:
        first = fp.readline().strip()
    sep = ";" if (";" in first and "," not in first) else ","
    df = pd.read_csv(path, sep=sep, encoding="utf-8", low_memory=False)
    df.columns = [c.strip("<>").lower() for c in df.columns]
    if "datetime" in df.columns:
        df["ts"] = pd.to_datetime(df["datetime"], errors="coerce")
    elif "date" in df.columns and "time" in df.columns:
        df["ts"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str).str.zfill(6),
            format="%Y%m%d %H%M%S", errors="coerce")
    else:
        return None
    df = df.dropna(subset=["ts", "close"])
    if df.empty:
        return None
    if "volume" not in df.columns:
        for src in ("vol",):
            if src in df.columns:
                df = df.rename(columns={src: "volume"})
                break
    if "volume" not in df.columns:
        df["volume"] = 0
    return df[["ts", "open", "high", "low", "close", "volume"]]


def load_ticker_candles(ticker: str, prices_dir: Path) -> Optional[pd.DataFrame]:
    prefix = TICKER_TO_PREFIX.get(ticker, ticker)
    files = list(prices_dir.rglob(f"prices_{prefix}.csv"))
    if not files:
        files = list(prices_dir.rglob(f"{prefix}_*.csv"))
    if not files:
        return None
    dfs = [_read_candles_csv(f) for f in files]
    dfs = [d for d in dfs if d is not None and len(d) > 0]
    if not dfs:
        return None
    full = pd.concat(dfs, ignore_index=True)
    full = full.drop_duplicates(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return full.set_index("ts")


def load_all_candles(prices_dir: Path, tickers: List[str]) -> Dict[str, pd.DataFrame]:
    log.info("Loading candles for %d tickers from %s ...", len(tickers), prices_dir)
    out: Dict[str, pd.DataFrame] = {}
    for t in tickers:
        df = load_ticker_candles(t, prices_dir)
        if df is not None:
            out[t] = df
    log.info("  loaded: %d/%d tickers", len(out), len(tickers))
    return out


# ============================================================
# Simulate trade (port from ab_backtest_v1_holdout.py)
# ============================================================
@dataclass
class TradeResult:
    ticker: str
    ts_open: pd.Timestamp
    ts_close: pd.Timestamp
    side: int
    entry: float
    exit: float
    size_lots: int
    notional_rub: float
    gross_pnl_rub: float
    cost_rub: float
    net_pnl_rub: float
    exit_reason: str
    duration_min: float


def get_entry_price(candles, ts):
    next_min = (ts + pd.Timedelta(seconds=60)).floor("min")
    idx = candles.index.searchsorted(next_min)
    if idx >= len(candles):
        return None
    actual_ts = candles.index[idx]
    if (actual_ts - next_min).total_seconds() > 1800:
        return None
    return float(candles["open"].iloc[idx]), actual_ts


def simulate_trade(candles, ts, ticker, side, pred_mfe_pct, pred_mae_pct, horizon_min, equity):
    info = get_entry_price(candles, ts)
    if info is None:
        return None
    entry_price, entry_ts = info
    sl_dist_pct = max((pred_mae_pct * SL_BUFFER) / 100, 0.0005)
    tp_dist_pct = max((pred_mfe_pct * TP_FRACTION) / 100, 0.001)
    if side == 1:
        sl_price = entry_price * (1 - sl_dist_pct)
        tp_price = entry_price * (1 + tp_dist_pct)
    else:
        sl_price = entry_price * (1 + sl_dist_pct)
        tp_price = entry_price * (1 - tp_dist_pct)

    risk_rub = equity * RISK_PER_TRADE_PCT
    sl_dist_abs = abs(entry_price - sl_price)
    if sl_dist_abs <= 0:
        return None
    lot_size = LOT_SIZES.get(ticker, 1)
    notional_per_lot = lot_size * entry_price
    pnl_per_lot_at_sl = sl_dist_abs * lot_size
    n_lots = int(risk_rub / pnl_per_lot_at_sl)
    if n_lots <= 0:
        return None
    total_notional = n_lots * notional_per_lot
    max_notional = equity * LEVERAGE
    if total_notional > max_notional:
        n_lots = int(max_notional / notional_per_lot)
        if n_lots <= 0:
            return None
        total_notional = n_lots * notional_per_lot

    end_ts = entry_ts + pd.Timedelta(minutes=horizon_min)
    idx_start = candles.index.searchsorted(entry_ts)
    idx_end = candles.index.searchsorted(end_ts) + 1
    window = candles.iloc[idx_start:idx_end]
    if window.empty:
        return None

    exit_price = None
    exit_ts = None
    exit_reason = None
    for ts_bar, row in window.iterrows():
        bar_high = row["high"]
        bar_low = row["low"]
        if side == 1:
            if bar_low <= sl_price:
                exit_price = sl_price; exit_ts = ts_bar; exit_reason = "sl"; break
            if bar_high >= tp_price:
                exit_price = tp_price; exit_ts = ts_bar; exit_reason = "tp"; break
        else:
            if bar_high >= sl_price:
                exit_price = sl_price; exit_ts = ts_bar; exit_reason = "sl"; break
            if bar_low <= tp_price:
                exit_price = tp_price; exit_ts = ts_bar; exit_reason = "tp"; break
    if exit_price is None:
        exit_price = float(window["close"].iloc[-1])
        exit_ts = window.index[-1]
        exit_reason = "time"

    if side == 1:
        gross = (exit_price - entry_price) * lot_size * n_lots
    else:
        gross = (entry_price - exit_price) * lot_size * n_lots
    cost = total_notional * total_cost_pct(ticker) / 100
    net = gross - cost
    duration = (exit_ts - entry_ts).total_seconds() / 60
    return TradeResult(
        ticker=ticker, ts_open=entry_ts, ts_close=exit_ts, side=side,
        entry=entry_price, exit=exit_price, size_lots=n_lots,
        notional_rub=total_notional, gross_pnl_rub=gross,
        cost_rub=cost, net_pnl_rub=net,
        exit_reason=exit_reason, duration_min=duration,
    )


# ============================================================
# Walk-forward
# ============================================================
def get_target_columns() -> List[str]:
    return [f"{t}_{H}m" for H in (30, 60) for t in ("mfe_long", "mae_long", "mfe_short", "mae_short")]


def compute_sample_weights(tickers: pd.Series) -> np.ndarray:
    counts = tickers.value_counts()
    w = tickers.map(lambda t: 1.0 / np.sqrt(counts[t]))
    return (w / w.mean()).values


def train_single_model(X, y, sample_weight, label):
    import xgboost as xgb
    mask = ~np.isnan(y)
    if mask.sum() < 100:
        log.warning("  [%s] too few clean samples (%d) — skipping", label, mask.sum())
        return None
    Xc = X[mask]; yc = y[mask]
    wc = sample_weight[mask] if sample_weight is not None else None
    m = xgb.XGBRegressor(**XGB_PARAMS)
    m.fit(Xc, yc, sample_weight=wc)
    return m


def build_folds(df: pd.DataFrame) -> List[dict]:
    first_dt = df["_dt"].iloc[0]
    last_dt = df["_dt"].iloc[-1]
    log.info("Date range: %s → %s", first_dt.date(), last_dt.date())
    folds: List[dict] = []
    test_start = first_dt + pd.DateOffset(months=TRAIN_MONTHS)
    while True:
        test_end = test_start + pd.DateOffset(months=TEST_MONTHS)
        if test_end > last_dt:
            break
        train_end = test_start - PURGE
        train_mask = df["_dt"] < train_end
        test_mask = (df["_dt"] >= test_start) & (df["_dt"] < test_end)
        if train_mask.sum() >= MIN_TRAIN_SAMPLES and test_mask.sum() >= MIN_TEST_SAMPLES:
            folds.append({
                "test_start": test_start,
                "test_end": test_end,
                "train_end": train_end,
                "train_mask": train_mask,
                "test_mask": test_mask,
            })
        test_start += pd.DateOffset(months=STEP_MONTHS)
    log.info("Folds generated: %d", len(folds))
    for i, f in enumerate(folds, 1):
        log.info("  Fold %2d: test [%s → %s] train_n=%d test_n=%d",
                 i, f["test_start"].date(), f["test_end"].date(),
                 int(f["train_mask"].sum()), int(f["test_mask"].sum()))
    return folds


def train_fold_16_models(train_df: pd.DataFrame, feature_cols: List[str]) -> Dict[str, object]:
    """Returns dict {target_<h>m_<type>: model}."""
    models: Dict[str, object] = {}
    X = train_df[feature_cols].values
    sw = compute_sample_weights(train_df["_ticker"])
    mx_mask = train_df["_ticker"] == "MX"
    n_mx = int(mx_mask.sum())
    X_mx = train_df.loc[mx_mask, feature_cols].values if n_mx >= MIN_MX_SAMPLES else None
    t0 = time.time()
    for tcol in get_target_columns():
        y_gen = train_df[tcol].values
        m_g = train_single_model(X, y_gen, sw, label=f"{tcol}/general")
        if m_g is not None:
            models[f"{tcol}_general"] = m_g
        if X_mx is not None:
            y_mx = train_df.loc[mx_mask, tcol].values
            m_x = train_single_model(X_mx, y_mx, None, label=f"{tcol}/mx_specific")
            if m_x is not None:
                models[f"{tcol}_mx_specific"] = m_x
    log.info("  Trained %d models in %.1fs (MX=%d, mx_specific %s)",
             len(models), time.time() - t0, n_mx,
             "ENABLED" if X_mx is not None else "SKIPPED")
    return models


def simulate_fold(test_df: pd.DataFrame, feature_cols: List[str],
                  models: Dict[str, object], candles: Dict[str, pd.DataFrame]) -> Tuple[List[dict], dict]:
    """Predict + decide + simulate. Returns (trades, stats)."""
    # Predict for all 16 model keys (or those available)
    X = test_df[feature_cols].values
    preds: Dict[str, np.ndarray] = {}
    for name, m in models.items():
        preds[name] = np.maximum(m.predict(X), 0)

    # Build per-row prediction lookup based on USE_MX_SPECIFIC + ticker
    equity = INITIAL_EQUITY
    trades: List[dict] = []
    last_trade_ts: Dict[str, pd.Timestamp] = {}
    current_day = None
    daily_start = equity
    daily_kill = False

    test_df = test_df.sort_values("_dt").reset_index(drop=True)
    n_skip_conf = n_skip_cool = n_skip_open = n_skip_dec = n_skip_nobars = n_skip_simfail = 0

    for i, row in test_df.iterrows():
        ts = row["_dt"]
        ticker = row["_ticker"]
        if current_day is None or ts.date() != current_day:
            current_day = ts.date()
            daily_start = equity
            daily_kill = False
        if daily_kill:
            continue
        if row.get("confidence", 0) < MIN_CONFIDENCE:
            n_skip_conf += 1; continue
        last = last_trade_ts.get(ticker)
        if last and (ts - last).total_seconds() < COOLDOWN_TICKER_SEC:
            n_skip_cool += 1; continue
        if sum(1 for t in trades if t["ts_open"] <= ts and t["ts_close"] > ts) >= MAX_OPEN_POSITIONS:
            n_skip_open += 1; continue

        # Predictions for chosen horizon, taking mx_specific if applicable
        suffix = "mx_specific" if (USE_MX_SPECIFIC and ticker == "MX") else "general"
        key_mfe_l = f"mfe_long_{HORIZON}m_{suffix}"
        key_mae_l = f"mae_long_{HORIZON}m_{suffix}"
        key_mfe_s = f"mfe_short_{HORIZON}m_{suffix}"
        key_mae_s = f"mae_short_{HORIZON}m_{suffix}"
        if any(k not in preds for k in (key_mfe_l, key_mae_l, key_mfe_s, key_mae_s)):
            # fall back to general if mx_specific missing
            key_mfe_l = f"mfe_long_{HORIZON}m_general"
            key_mae_l = f"mae_long_{HORIZON}m_general"
            key_mfe_s = f"mfe_short_{HORIZON}m_general"
            key_mae_s = f"mae_short_{HORIZON}m_general"
            if any(k not in preds for k in (key_mfe_l, key_mae_l, key_mfe_s, key_mae_s)):
                n_skip_dec += 1; continue

        mfe_l = preds[key_mfe_l][i]; mae_l = preds[key_mae_l][i]
        mfe_s = preds[key_mfe_s][i]; mae_s = preds[key_mae_s][i]
        rr_long = mfe_l / max(mae_l, MIN_MAE_PCT)
        rr_short = mfe_s / max(mae_s, MIN_MAE_PCT)
        side = 0; chosen_mfe = chosen_mae = 0.0
        if rr_long >= RR_THRESHOLD and mfe_l >= MIN_MFE_PCT and rr_long >= rr_short:
            side, chosen_mfe, chosen_mae = 1, mfe_l, mae_l
        elif rr_short >= RR_THRESHOLD and mfe_s >= MIN_MFE_PCT and rr_short > rr_long:
            side, chosen_mfe, chosen_mae = -1, mfe_s, mae_s
        if side == 0:
            n_skip_dec += 1; continue
        if ticker not in candles:
            n_skip_nobars += 1; continue

        tr = simulate_trade(candles[ticker], ts, ticker, side,
                            chosen_mfe, chosen_mae, HORIZON, equity)
        if tr is None:
            n_skip_simfail += 1; continue
        equity += tr.net_pnl_rub
        last_trade_ts[ticker] = ts
        trades.append({
            "ticker": tr.ticker, "ts_open": tr.ts_open, "ts_close": tr.ts_close,
            "side": tr.side, "entry": tr.entry, "exit": tr.exit,
            "size_lots": tr.size_lots, "notional_rub": tr.notional_rub,
            "gross_pnl_rub": tr.gross_pnl_rub, "cost_rub": tr.cost_rub,
            "net_pnl_rub": tr.net_pnl_rub, "exit_reason": tr.exit_reason,
            "duration_min": tr.duration_min,
            "pred_mfe_pct": chosen_mfe, "pred_mae_pct": chosen_mae,
        })
        if equity - daily_start < -daily_start * DAILY_KILL_PCT:
            daily_kill = True

    return trades, {
        "n_skip_conf": n_skip_conf, "n_skip_cool": n_skip_cool,
        "n_skip_open": n_skip_open, "n_skip_dec": n_skip_dec,
        "n_skip_nobars": n_skip_nobars, "n_skip_simfail": n_skip_simfail,
    }


def compute_fold_metrics(trades: List[dict]) -> dict:
    if not trades:
        return {"n_trades": 0, "sharpe": 0.0, "median_pnl_rub": 0.0,
                "total_pnl_rub": 0.0, "win_rate": 0.0, "max_dd_rub": 0.0,
                "exit_tp_pct": 0.0, "exit_sl_pct": 0.0, "exit_time_pct": 0.0}
    df = pd.DataFrame(trades)
    n = len(df)
    wins = int((df["net_pnl_rub"] > 0).sum())
    df["_close_dt"] = pd.to_datetime(df["ts_close"])
    df["date"] = df["_close_dt"].dt.date
    daily = df.groupby("date")["net_pnl_rub"].sum()
    sharpe = float((daily.mean() / daily.std()) * np.sqrt(252)) if len(daily) > 1 and daily.std() > 0 else 0.0
    cum = df["net_pnl_rub"].cumsum()
    max_dd = float((cum - cum.cummax()).min()) if len(cum) else 0.0
    ec = df["exit_reason"].value_counts(normalize=True)
    return {
        "n_trades": n, "sharpe": sharpe,
        "win_rate": wins / n,
        "median_pnl_rub": float(df["net_pnl_rub"].median()),
        "total_pnl_rub": float(df["net_pnl_rub"].sum()),
        "max_dd_rub": max_dd,
        "exit_tp_pct": float(ec.get("tp", 0)),
        "exit_sl_pct": float(ec.get("sl", 0)),
        "exit_time_pct": float(ec.get("time", 0)),
    }


def write_report(output_path: Path, per_fold: List[dict], summary: dict, all_trades: List[dict]):
    import xlsxwriter
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = xlsxwriter.Workbook(str(output_path), {"nan_inf_to_errors": True})

    def _write_sheet(name, rows):
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

    _write_sheet("per_fold", per_fold)
    _write_sheet("summary", [summary])
    if all_trades:
        _write_sheet("trades", all_trades)
    wb.close()
    log.info("Excel: %s", output_path)


def merge_features_targets(features_path: Path, targets_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    log.info("Loading features: %s", features_path)
    features = pd.read_parquet(features_path)
    log.info("  features: %d × %d", len(features), len(features.columns))
    log.info("Loading targets: %s", targets_path)
    targets = pd.read_parquet(targets_path)
    log.info("  targets:  %d × %d", len(targets), len(targets.columns))
    targets["id"] = targets["id"].astype(str)
    features["_id"] = features["_id"].astype(str)
    df = features.merge(
        targets.drop(columns=["datetime", "ticker"]),
        left_on="_id", right_on="id", how="inner",
    )
    df["_dt"] = pd.to_datetime(df["_datetime"])
    df = df.sort_values("_dt").reset_index(drop=True)
    log.info("  merged: %d rows", len(df))
    meta = {"_id", "_datetime", "_dt", "_ticker", "id", "ticker", "_entry_price", "_entry_ts"}
    all_targets = set()
    for h in (1, 2, 3, 4, 5, 10, 15, 30, 45, 60):
        for t in ("mfe_long", "mae_long", "mfe_short", "mae_short"):
            all_targets.add(f"{t}_{h}m")
    feature_cols = [c for c in df.columns if c not in meta and c not in all_targets]
    log.info("  feature cols: %d", len(feature_cols))
    return df, feature_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", required=True, help="Features parquet")
    ap.add_argument("--targets", default=str(DEFAULT_TARGETS), help="Targets parquet (Phase 2)")
    ap.add_argument("--prices-dir", default=str(DEFAULT_PRICES_DIR))
    ap.add_argument("--label", required=True,
                    help="Run label (e.g. 'legacy_baseline', 'v6_8b_v2_1_0'). "
                         "Output dir = data/reenrich_phase2/walk_forward/<label>/")
    ap.add_argument("--output-base", default=str(PROJECT_ROOT / "data" / "reenrich_phase2" / "walk_forward"))
    ap.add_argument("--limit-folds", type=int, default=None,
                    help="Process only first N folds (default: all 13). Useful для quick smoke test.")
    args = ap.parse_args()

    output_dir = Path(args.output_base) / args.label
    output_dir.mkdir(parents=True, exist_ok=True)

    df, feature_cols = merge_features_targets(Path(args.features), Path(args.targets))
    folds = build_folds(df)
    if not folds:
        log.error("No folds — check date range")
        return 1
    if args.limit_folds:
        folds = folds[:args.limit_folds]
        log.info("Limited to %d folds (smoke mode)", len(folds))

    # Load candles ONCE
    tickers_seen = sorted(df["_ticker"].unique().tolist())
    candles = load_all_candles(Path(args.prices_dir), tickers_seen)

    per_fold_results: List[dict] = []
    all_trades: List[dict] = []
    for i, fold in enumerate(folds, 1):
        t0 = time.time()
        log.info("=" * 60)
        log.info("FOLD %d/%d — test [%s → %s]", i, len(folds),
                 fold["test_start"].date(), fold["test_end"].date())
        train_df = df[fold["train_mask"]].copy()
        test_df = df[fold["test_mask"]].copy()
        log.info("  train_n=%d  test_n=%d", len(train_df), len(test_df))

        models = train_fold_16_models(train_df, feature_cols)
        trades, skip_stats = simulate_fold(test_df, feature_cols, models, candles)
        m = compute_fold_metrics(trades)
        log.info("  [Fold %d] n_trades=%d sharpe=%.2f total_pnl=%+.0f win=%.1f%%  (%.1fs)",
                 i, m["n_trades"], m["sharpe"], m["total_pnl_rub"], m["win_rate"] * 100,
                 time.time() - t0)

        per_fold_results.append({
            "fold": i,
            "test_start": fold["test_start"].isoformat(),
            "test_end": fold["test_end"].isoformat(),
            "train_n": int(fold["train_mask"].sum()),
            "test_n": int(fold["test_mask"].sum()),
            **m,
            **skip_stats,
        })
        for t in trades:
            t["fold"] = i
        all_trades.extend(trades)

    # Aggregate
    sharpes = [r["sharpe"] for r in per_fold_results]
    pnls = [r["total_pnl_rub"] for r in per_fold_results]
    summary = {
        "label": args.label,
        "n_folds": len(per_fold_results),
        "mean_sharpe": float(np.mean(sharpes)) if sharpes else 0.0,
        "median_sharpe": float(np.median(sharpes)) if sharpes else 0.0,
        "min_sharpe": float(np.min(sharpes)) if sharpes else 0.0,
        "max_sharpe": float(np.max(sharpes)) if sharpes else 0.0,
        "std_sharpe": float(np.std(sharpes)) if sharpes else 0.0,
        "n_folds_positive_pnl": sum(1 for p in pnls if p > 0),
        "n_folds_sharpe_ge_1": sum(1 for s in sharpes if s >= 1.0),
        "total_pnl_rub": float(sum(pnls)),
        "mean_n_trades": float(np.mean([r["n_trades"] for r in per_fold_results])),
    }

    log.info("=" * 60)
    log.info("WALK-FORWARD SUMMARY")
    log.info("=" * 60)
    log.info("  Label: %s", args.label)
    log.info("  Folds: %d", summary["n_folds"])
    log.info("  Mean Sharpe:   %.2f  (Phase 2 baseline = +4.87)", summary["mean_sharpe"])
    log.info("  Median Sharpe: %.2f", summary["median_sharpe"])
    log.info("  Min Sharpe:    %.2f", summary["min_sharpe"])
    log.info("  Std Sharpe:    %.2f", summary["std_sharpe"])
    log.info("  Positive PnL folds: %d/%d", summary["n_folds_positive_pnl"], summary["n_folds"])
    log.info("  Sharpe ≥ +1 folds:  %d/%d", summary["n_folds_sharpe_ge_1"], summary["n_folds"])
    log.info("  Total PnL: %+.0f RUB", summary["total_pnl_rub"])
    log.info("  Mean trades/fold: %.0f", summary["mean_n_trades"])

    excel_path = output_dir / f"walk_forward_{args.label}.xlsx"
    write_report(excel_path, per_fold_results, summary, all_trades)
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df.to_parquet(output_dir / f"walk_forward_{args.label}_trades.parquet", index=False)
        log.info("Trades parquet: %s", output_dir / f"walk_forward_{args.label}_trades.parquet")

    # Acceptance gate
    log.info("")
    log.info("=== Sprint 5.7 ACCEPTANCE GATE ===")
    primary = summary["mean_sharpe"] >= 4.0
    secondary = summary["median_sharpe"] >= 4.0
    all_positive = summary["n_folds_positive_pnl"] == summary["n_folds"]
    most_sharpe = summary["n_folds_sharpe_ge_1"] >= (summary["n_folds"] - 3)
    log.info("  Mean Sharpe ≥ +4:    %.2f  %s", summary["mean_sharpe"], "✓" if primary else "✗")
    log.info("  Median Sharpe ≥ +4:  %.2f  %s", summary["median_sharpe"], "✓" if secondary else "✗")
    log.info("  ALL folds positive:  %d/%d  %s",
             summary["n_folds_positive_pnl"], summary["n_folds"], "✓" if all_positive else "✗")
    log.info("  ≥10 folds Sharpe ≥1: %d/%d  %s",
             summary["n_folds_sharpe_ge_1"], summary["n_folds"], "✓" if most_sharpe else "✗")
    log.info("  OVERALL: %s", "✓ PASS — DEPLOY" if (primary and all_positive) else "✗ FAIL")

    return 0


if __name__ == "__main__":
    sys.exit(main())
