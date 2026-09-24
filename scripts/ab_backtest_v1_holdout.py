r"""
scripts/ab_backtest_v1_holdout.py
==================================
A/B сравнение XGBoost моделей v1_legacy vs v2_70b (v1) на V1 holdout
(2026-01..04, 893 candidate trades в Phase 2 trade frame).

Логика портирована из:
  D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\backtest_mfe.py

Фиксированная стратегия (Phase 2 best combo):
  - horizon = 60 min
  - RR threshold = 2.0
  - mx_specific = True (MX → mx_specific models, остальные → general)
  - MIN_MFE_PCT = 0.15, MIN_MAE_PCT = 0.05
  - TP_FRACTION = 0.7, SL_BUFFER = 1.2
  - Risk = 0.5%, equity = 500k, leverage = 10x, max 3 open, cooldown 60s

Decision per event:
  pred_rr_long  = pred_mfe_long  / max(pred_mae_long,  MIN_MAE_PCT)
  pred_rr_short = pred_mfe_short / max(pred_mae_short, MIN_MAE_PCT)
  if  pred_rr_long  >= 2.0 AND pred_mfe_long  >= 0.15 AND pred_rr_long  >= pred_rr_short  → long
  elif pred_rr_short >= 2.0 AND pred_mfe_short >= 0.15 AND pred_rr_short > pred_rr_long → short
  else: skip

Simulate:
  Bar-by-bar TP/SL/time-stop, real costs (Сбер + slippage), 0.5% risk sizing.

Usage:
  # Legacy arm only (V1 enrichment ещё нет):
  python scripts/ab_backtest_v1_holdout.py --arm legacy

  # Both arms (после V1 70b enrich + rebuild_features --enriched-70b ... --output features_mfe_v1_70b.parquet):
  python scripts/ab_backtest_v1_holdout.py --arm both
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PHASE2_DIR = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe")
DEFAULT_TARGETS = PHASE2_DIR / "targets_mfe.parquet"
DEFAULT_FEATURES_LEGACY = PHASE2_DIR / "features_mfe.parquet"
DEFAULT_FEATURES_70B = PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v1_70b.parquet"
DEFAULT_MODELS_LEGACY = PROJECT_ROOT / "data" / "models" / "predictor" / "v1"           # rolled back 2026-05-28
DEFAULT_MODELS_70B = PROJECT_ROOT / "data" / "models" / "predictor" / "v1_70b_rolling"   # rolled back 2026-05-28
DEFAULT_MODELS_V3 = PROJECT_ROOT / "data" / "models" / "predictor" / "v3"
DEFAULT_MODELS_V5 = PROJECT_ROOT / "data" / "models" / "predictor" / "v5"
DEFAULT_FEATURES_V5 = PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_no_llm.parquet"
DEFAULT_V1_SAMPLE = PROJECT_ROOT / "sprint4" / "sampling" / "data" / "validation_sample.parquet"
DEFAULT_PRICES_DIR = Path(r"D:\quik_sber\newsbot\prices")
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "reenrich_phase2" / "ab_v1_holdout"

# Phase 2 best combo
HORIZON = 60          # минуты
RR_THRESHOLD = 2.0
USE_MX_SPECIFIC = True
MIN_MFE_PCT = 0.15
MIN_MAE_PCT = 0.05
TP_FRACTION = 0.7
SL_BUFFER = 1.2

# Portfolio params (Phase 2)
INITIAL_EQUITY = 500_000.0
LEVERAGE = 10
RISK_PER_TRADE_PCT = 0.005
DAILY_KILL_PCT = 0.02
MAX_OPEN_POSITIONS = 3
COOLDOWN_TICKER_SEC = 60
MIN_CONFIDENCE = 0.55  # event-level confidence filter (CLI overridable via --min-confidence)

# Costs/slippage (Phase 2)
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

V1_START = pd.Timestamp("2026-01-01")
V1_END = pd.Timestamp("2026-05-01")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ab_backtest")


# ============================================================
# Candle loading (Phase 2 reader)
# ============================================================
def total_cost_pct(ticker: str) -> float:
    return COSTS_RT_PCT.get(ticker, 0.10) + SLIPPAGE_RT_PCT.get(ticker, 0.05)


def _read_candles_csv(path: Path) -> Optional[pd.DataFrame]:
    with open(path, encoding="utf-8") as fp:
        first_line = fp.readline().strip()
    sep = ";" if (";" in first_line and "," not in first_line) else ","
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
    if len(df) == 0:
        return None
    for src in ("vol", "volume"):
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
    full = full.set_index("ts")
    return full


def load_all_candles(prices_dir: Path, tickers: List[str]) -> Dict[str, pd.DataFrame]:
    log.info("Loading candles for %d tickers from %s ...", len(tickers), prices_dir)
    out = {}
    for t in tickers:
        df = load_ticker_candles(t, prices_dir)
        if df is not None:
            # slice to V1 window with some padding for entry/exit
            mask = (df.index >= V1_START - pd.Timedelta(days=1)) & (df.index < V1_END + pd.Timedelta(days=1))
            out[t] = df.loc[mask].copy()
    log.info("  loaded: %d/%d tickers", len(out), len(tickers))
    return out


# ============================================================
# Simulate (Phase 2 port)
# ============================================================
@dataclass
class TradeResult:
    ticker: str
    ts_open: pd.Timestamp
    ts_close: pd.Timestamp
    side: int
    entry: float
    exit: float
    sl_price: float
    tp_price: float
    size_lots: int
    notional_rub: float
    gross_pnl_rub: float
    cost_rub: float
    net_pnl_rub: float
    exit_reason: str
    duration_min: float
    pred_mfe_pct: float
    pred_mae_pct: float
    pred_rr: float


def get_entry_price(candles: pd.DataFrame, ts: pd.Timestamp) -> Optional[Tuple[float, pd.Timestamp]]:
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
    if len(window) == 0:
        return None

    exit_price = None
    exit_ts = None
    exit_reason = None
    for ts_bar, row in window.iterrows():
        bar_high = row["high"]
        bar_low = row["low"]
        if side == 1:
            if bar_low <= sl_price:
                exit_price = sl_price
                exit_ts = ts_bar
                exit_reason = "sl"
                break
            if bar_high >= tp_price:
                exit_price = tp_price
                exit_ts = ts_bar
                exit_reason = "tp"
                break
        else:
            if bar_high >= sl_price:
                exit_price = sl_price
                exit_ts = ts_bar
                exit_reason = "sl"
                break
            if bar_low <= tp_price:
                exit_price = tp_price
                exit_ts = ts_bar
                exit_reason = "tp"
                break

    if exit_price is None:
        last_bar = window.iloc[-1]
        exit_price = float(last_bar["close"])
        exit_ts = window.index[-1]
        exit_reason = "time"

    if side == 1:
        gross_pnl_rub = (exit_price - entry_price) * lot_size * n_lots
    else:
        gross_pnl_rub = (entry_price - exit_price) * lot_size * n_lots
    cost_rub = total_notional * total_cost_pct(ticker) / 100
    net_pnl_rub = gross_pnl_rub - cost_rub
    duration_min = (exit_ts - entry_ts).total_seconds() / 60
    pred_rr = pred_mfe_pct / max(pred_mae_pct, MIN_MAE_PCT)

    return TradeResult(
        ticker=ticker, ts_open=entry_ts, ts_close=exit_ts, side=side,
        entry=entry_price, exit=exit_price, sl_price=sl_price, tp_price=tp_price,
        size_lots=n_lots, notional_rub=total_notional,
        gross_pnl_rub=gross_pnl_rub, cost_rub=cost_rub, net_pnl_rub=net_pnl_rub,
        exit_reason=exit_reason, duration_min=duration_min,
        pred_mfe_pct=pred_mfe_pct, pred_mae_pct=pred_mae_pct, pred_rr=pred_rr,
    )


# ============================================================
# Arm runner
# ============================================================
def load_models(models_dir: Path) -> Dict[str, object]:
    needed = []
    for target in ("mfe_long", "mae_long", "mfe_short", "mae_short"):
        for h in (30, 60):
            for mt in ("general", "mx_specific"):
                needed.append(f"{target}_{h}m_{mt}")
    out = {}
    for name in needed:
        p = models_dir / f"{name}.joblib"
        if not p.exists():
            raise FileNotFoundError(f"Missing model: {p}")
        out[name] = joblib.load(p)
    return out


def merge_features_targets(features_path: Path, targets_path: Path) -> Tuple[pd.DataFrame, List[str]]:
    targets = pd.read_parquet(targets_path)
    features = pd.read_parquet(features_path)
    targets["id"] = targets["id"].astype(str)
    features["_id"] = features["_id"].astype(str)
    df = features.merge(
        targets.drop(columns=["datetime", "ticker"]),
        left_on="_id", right_on="id", how="inner",
    )
    df["_dt"] = pd.to_datetime(df["_datetime"])
    df = df.sort_values("_dt").reset_index(drop=True)

    meta_cols = {"_id", "_datetime", "_dt", "_ticker", "id", "ticker",
                 "_entry_price", "_entry_ts"}
    all_targets = set()
    for h in (1, 2, 3, 4, 5, 10, 15, 30, 45, 60):
        for t in ("mfe_long", "mae_long", "mfe_short", "mae_short"):
            all_targets.add(f"{t}_{h}m")
    feature_cols = [c for c in df.columns if c not in meta_cols and c not in all_targets]
    return df, feature_cols


def predict_all(df: pd.DataFrame, feature_cols: List[str],
                models: Dict[str, object]) -> pd.DataFrame:
    """Run all 16 models on full df, returning a DF with pred_<target>_<h>m_<type> columns."""
    X = df[feature_cols].values
    out = pd.DataFrame(index=df.index)
    for name, model in models.items():
        pred = model.predict(X)
        out["pred_" + name] = np.maximum(pred, 0)
    return out


def run_arm(arm_name: str, df: pd.DataFrame, feature_cols: List[str],
            models: Dict[str, object], candles: Dict[str, pd.DataFrame]) -> List[Dict]:
    """Predict + decide + simulate for one arm. Returns trade dicts."""
    log.info("[%s] running predictions on %d rows ...", arm_name, len(df))
    pred_df = predict_all(df, feature_cols, models)
    df = df.join(pred_df)

    # Apply Phase 2 best combo decision
    equity = INITIAL_EQUITY
    trades = []
    last_trade_ts: Dict[str, pd.Timestamp] = {}
    current_day = None
    daily_start_equity = equity
    daily_kill_active = False
    n_skip_conf = n_skip_cool = n_skip_open = n_skip_decision = n_skip_nobars = 0
    n_skip_sim_fail = 0

    df = df.sort_values("_dt").reset_index(drop=True)
    for _, row in df.iterrows():
        ts = row["_dt"]
        ticker = row["_ticker"]

        if current_day is None or ts.date() != current_day:
            current_day = ts.date()
            daily_start_equity = equity
            daily_kill_active = False
        if daily_kill_active:
            continue

        # Skip if confidence col exists AND below threshold. For no-LLM arms (v5) col missing → no filter.
        conf = row.get("confidence")
        if conf is not None and not pd.isna(conf) and conf < MIN_CONFIDENCE:
            n_skip_conf += 1
            continue
        last_ts = last_trade_ts.get(ticker)
        if last_ts and (ts - last_ts).total_seconds() < COOLDOWN_TICKER_SEC:
            n_skip_cool += 1
            continue
        recent = [t for t in trades if t["ts_open"] <= ts and t["ts_close"] > ts]
        if len(recent) >= MAX_OPEN_POSITIONS:
            n_skip_open += 1
            continue

        # MX-specific predictions only for MX ticker
        if USE_MX_SPECIFIC and ticker == "MX":
            mfe_l = row["pred_mfe_long_60m_mx_specific"]
            mae_l = row["pred_mae_long_60m_mx_specific"]
            mfe_s = row["pred_mfe_short_60m_mx_specific"]
            mae_s = row["pred_mae_short_60m_mx_specific"]
        else:
            mfe_l = row["pred_mfe_long_60m_general"]
            mae_l = row["pred_mae_long_60m_general"]
            mfe_s = row["pred_mfe_short_60m_general"]
            mae_s = row["pred_mae_short_60m_general"]

        rr_long = mfe_l / max(mae_l, MIN_MAE_PCT)
        rr_short = mfe_s / max(mae_s, MIN_MAE_PCT)
        side = 0
        chosen_mfe = chosen_mae = 0.0
        if rr_long >= RR_THRESHOLD and mfe_l >= MIN_MFE_PCT and rr_long >= rr_short:
            side = 1
            chosen_mfe, chosen_mae = mfe_l, mae_l
        elif rr_short >= RR_THRESHOLD and mfe_s >= MIN_MFE_PCT and rr_short > rr_long:
            side = -1
            chosen_mfe, chosen_mae = mfe_s, mae_s
        if side == 0:
            n_skip_decision += 1
            continue
        if ticker not in candles:
            n_skip_nobars += 1
            continue

        trade = simulate_trade(candles[ticker], ts, ticker, side,
                               chosen_mfe, chosen_mae, HORIZON, equity)
        if trade is None:
            n_skip_sim_fail += 1
            continue
        equity += trade.net_pnl_rub
        last_trade_ts[ticker] = ts
        trades.append({
            "arm": arm_name,
            "ticker": trade.ticker,
            "ts_open": trade.ts_open,
            "ts_close": trade.ts_close,
            "side": trade.side,
            "entry": trade.entry, "exit": trade.exit,
            "sl_price": trade.sl_price, "tp_price": trade.tp_price,
            "size_lots": trade.size_lots, "notional_rub": trade.notional_rub,
            "gross_pnl_rub": trade.gross_pnl_rub, "cost_rub": trade.cost_rub,
            "net_pnl_rub": trade.net_pnl_rub,
            "exit_reason": trade.exit_reason, "duration_min": trade.duration_min,
            "pred_mfe_pct": trade.pred_mfe_pct, "pred_mae_pct": trade.pred_mae_pct,
            "pred_rr": trade.pred_rr,
        })
        daily_pnl = equity - daily_start_equity
        if daily_pnl < -daily_start_equity * DAILY_KILL_PCT:
            daily_kill_active = True

    log.info("[%s] events processed: %d", arm_name, len(df))
    log.info("[%s]   skipped (confidence<%.2f): %d", arm_name, MIN_CONFIDENCE, n_skip_conf)
    log.info("[%s]   skipped (cooldown):        %d", arm_name, n_skip_cool)
    log.info("[%s]   skipped (max open):        %d", arm_name, n_skip_open)
    log.info("[%s]   skipped (decision filter): %d", arm_name, n_skip_decision)
    log.info("[%s]   skipped (no bars):         %d", arm_name, n_skip_nobars)
    log.info("[%s]   skipped (sim fail):        %d", arm_name, n_skip_sim_fail)
    log.info("[%s] TRADES SIMULATED: %d", arm_name, len(trades))
    return trades


def compute_metrics(trades: List[Dict]) -> Dict:
    if not trades:
        return {
            "n_trades": 0, "win_rate": 0.0, "gross_pnl_rub": 0.0,
            "net_pnl_rub": 0.0, "avg_pnl_rub": 0.0, "max_dd_rub": 0.0,
            "sharpe": 0.0, "final_equity": INITIAL_EQUITY,
            "exit_tp_pct": 0.0, "exit_sl_pct": 0.0, "exit_time_pct": 0.0,
        }
    tdf = pd.DataFrame(trades)
    n = len(tdf)
    wins = int((tdf["net_pnl_rub"] > 0).sum())
    tdf["_close_dt"] = pd.to_datetime(tdf["ts_close"])
    tdf["date"] = tdf["_close_dt"].dt.date
    daily = tdf.groupby("date")["net_pnl_rub"].sum()
    sharpe = float((daily.mean() / daily.std()) * np.sqrt(252)) if len(daily) > 1 and daily.std() > 0 else 0.0
    cum = tdf["net_pnl_rub"].cumsum()
    peak = cum.cummax()
    max_dd = float((cum - peak).min()) if len(cum) > 0 else 0.0
    ec = tdf["exit_reason"].value_counts(normalize=True)
    return {
        "n_trades": n,
        "win_rate": wins / n,
        "gross_pnl_rub": float(tdf["gross_pnl_rub"].sum()),
        "net_pnl_rub": float(tdf["net_pnl_rub"].sum()),
        "avg_pnl_rub": float(tdf["net_pnl_rub"].mean()),
        "max_dd_rub": max_dd,
        "sharpe": sharpe,
        "final_equity": float(INITIAL_EQUITY + tdf["net_pnl_rub"].sum()),
        "exit_tp_pct": float(ec.get("tp", 0)),
        "exit_sl_pct": float(ec.get("sl", 0)),
        "exit_time_pct": float(ec.get("time", 0)),
    }


def filter_to_v1(df: pd.DataFrame, v1_ids: set | None = None) -> pd.DataFrame:
    """Filter to V1 test window (2026-01-01..2026-05-01).
    Если v1_ids передан — также filter к этому id set (для 8k стратифицированного sample).
    Иначе берёт все Phase 2 trade rows в окне (~2,542 events) — больше статистики.
    """
    sub = df[(df["_dt"] >= V1_START) & (df["_dt"] < V1_END)].copy()
    if v1_ids is not None:
        sub = sub[sub["_id"].isin(v1_ids)]
    return sub


def write_report(output_path: Path, summaries: List[Dict], trades_all: List[Dict]) -> None:
    import xlsxwriter
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = xlsxwriter.Workbook(str(output_path), {"nan_inf_to_errors": True})

    ws = wb.add_worksheet("metrics")
    if summaries:
        cols = list(summaries[0].keys())
        for j, c in enumerate(cols):
            ws.write(0, j, c)
        for i, r in enumerate(summaries):
            for j, c in enumerate(cols):
                v = r[c]
                ws.write(i + 1, j, v if isinstance(v, (int, float, str, bool)) else str(v))

    # Comparison sheet: side-by-side всех плеч + pairwise deltas vs первого
    ws2 = wb.add_worksheet("comparison")
    arm_names = [s["arm"] for s in summaries]
    keys_to_show = ["n_trades", "win_rate", "net_pnl_rub", "sharpe", "max_dd_rub",
                    "avg_pnl_rub", "exit_tp_pct", "exit_sl_pct", "exit_time_pct"]
    ws2.write(0, 0, "metric")
    for j, arm in enumerate(arm_names, 1):
        ws2.write(0, j, arm)
    # Pairwise deltas vs first arm (baseline)
    baseline_arm = arm_names[0] if arm_names else None
    delta_col_start = 1 + len(arm_names)
    for j, arm in enumerate(arm_names[1:], 0):
        ws2.write(0, delta_col_start + j, f"Δ {arm} - {baseline_arm}")

    by_arm = {s["arm"]: s for s in summaries}
    for i, k in enumerate(keys_to_show, 1):
        ws2.write(i, 0, k)
        for j, arm in enumerate(arm_names, 1):
            ws2.write(i, j, by_arm[arm].get(k, 0))
        if baseline_arm:
            base_v = by_arm[baseline_arm].get(k, 0)
            for j, arm in enumerate(arm_names[1:], 0):
                ws2.write(i, delta_col_start + j, by_arm[arm].get(k, 0) - base_v)

    if trades_all:
        ws3 = wb.add_worksheet("trades")
        cols = list(trades_all[0].keys())
        for j, c in enumerate(cols):
            ws3.write(0, j, c)
        for i, r in enumerate(trades_all):
            for j, c in enumerate(cols):
                v = r[c]
                if isinstance(v, pd.Timestamp):
                    ws3.write(i + 1, j, v.isoformat())
                elif isinstance(v, (int, float, str, bool)):
                    ws3.write(i + 1, j, v)
                elif v is None:
                    ws3.write(i + 1, j, "")
                else:
                    ws3.write(i + 1, j, str(v))

    wb.close()
    log.info("Excel: %s", output_path)


def main() -> None:
    global MIN_CONFIDENCE, RR_THRESHOLD
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", choices=["legacy", "70b", "v3", "v5", "both", "three-way", "all"], default="three-way")
    ap.add_argument("--features-legacy", default=str(DEFAULT_FEATURES_LEGACY))
    ap.add_argument("--features-70b", default=str(DEFAULT_FEATURES_70B))
    ap.add_argument("--features-v3", default=str(DEFAULT_FEATURES_70B),
                    help="v3 features (default: same as 70b, оба используют full 70k 70b features)")
    ap.add_argument("--features-v5", default=str(DEFAULT_FEATURES_V5),
                    help="v5 features (no LLM cols, 44 features)")
    ap.add_argument("--models-legacy", default=str(DEFAULT_MODELS_LEGACY))
    ap.add_argument("--models-70b", default=str(DEFAULT_MODELS_70B))
    ap.add_argument("--models-v3", default=str(DEFAULT_MODELS_V3))
    ap.add_argument("--models-v5", default=str(DEFAULT_MODELS_V5))
    ap.add_argument("--targets", default=str(DEFAULT_TARGETS))
    ap.add_argument("--v1-sample", default=str(DEFAULT_V1_SAMPLE))
    ap.add_argument("--use-v1-sample-ids", action="store_true",
                    help="Restrict to 8k stratified V1 sample ids (default: use ALL Phase 2 trades in V1 window ~2.5k)")
    ap.add_argument("--prices-dir", default=str(DEFAULT_PRICES_DIR))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    ap.add_argument("--min-confidence", type=float, default=MIN_CONFIDENCE,
                    help=f"Event-level confidence filter (default {MIN_CONFIDENCE}). "
                         "70b калибрует confidence ниже (mean ~0.51 vs 8b ~0.67), "
                         "так что для honest A/B можно попробовать 0.40.")
    ap.add_argument("--rr", type=float, default=RR_THRESHOLD,
                    help=f"RR threshold (default {RR_THRESHOLD}). Phase 2 best by mean Sharpe = 3.0.")
    args = ap.parse_args()
    MIN_CONFIDENCE = args.min_confidence
    RR_THRESHOLD = args.rr
    log.info("MIN_CONFIDENCE = %.2f  RR_THRESHOLD = %.2f", MIN_CONFIDENCE, RR_THRESHOLD)

    if args.arm == "both":
        arms_to_run = ["legacy", "70b"]
    elif args.arm == "three-way":
        arms_to_run = ["legacy", "70b", "v3"]
    elif args.arm == "all":
        arms_to_run = ["legacy", "70b", "v3", "v5"]
    else:
        arms_to_run = [args.arm]
    log.info("Arms: %s", arms_to_run)

    # V1 ids (optional — only used если --use-v1-sample-ids)
    v1_ids: set | None = None
    if args.use_v1_sample_ids:
        import polars as pl
        v1_ids = set(pl.read_parquet(args.v1_sample)["id"].to_list())
        log.info("V1 sample ids: %d (restricting backtest к этому set)", len(v1_ids))
    else:
        log.info("Using ALL Phase 2 trade events in V1 window 2026-01-01..2026-05-01")

    arm_paths = {
        "legacy": (Path(args.features_legacy), Path(args.models_legacy)),
        "70b": (Path(args.features_70b), Path(args.models_70b)),
        "v3": (Path(args.features_v3), Path(args.models_v3)),
        "v5": (Path(args.features_v5), Path(args.models_v5)),
    }

    # Each arm: load its features+models, predict+simulate
    all_summaries = []
    all_trades = []
    tickers_seen = set()

    arms_data = {}  # arm → (df_v1, feature_cols, models)
    for arm in arms_to_run:
        feat_path, mdir = arm_paths[arm]
        if not feat_path.exists():
            log.error("[%s] features not found: %s", arm, feat_path)
            return
        if not mdir.exists():
            log.error("[%s] models dir not found: %s", arm, mdir)
            return
        log.info("[%s] features: %s", arm, feat_path)
        log.info("[%s] models:   %s", arm, mdir)
        df, feature_cols = merge_features_targets(feat_path, Path(args.targets))
        log.info("[%s] merged %d rows × %d features", arm, len(df), len(feature_cols))
        df_v1 = filter_to_v1(df, v1_ids)
        log.info("[%s] after V1 filter: %d rows (%d events)", arm, len(df_v1), df_v1["_id"].nunique())
        if df_v1.empty:
            log.error("[%s] empty V1 subset", arm)
            return
        tickers_seen.update(df_v1["_ticker"].unique().tolist())
        models = load_models(mdir)
        log.info("[%s] loaded %d models", arm, len(models))
        arms_data[arm] = (df_v1, feature_cols, models)

    # Load candles ONCE (shared between arms)
    candles = load_all_candles(Path(args.prices_dir), sorted(tickers_seen))

    # Run
    for arm in arms_to_run:
        df_v1, feature_cols, models = arms_data[arm]
        trades = run_arm(arm, df_v1, feature_cols, models, candles)
        all_trades.extend(trades)
        m = compute_metrics(trades)
        m["arm"] = arm
        all_summaries.append(m)
        log.info("[%s] METRICS: n=%d sharpe=%.2f net_pnl=%+.0f win=%.1f%%",
                 arm, m["n_trades"], m["sharpe"], m["net_pnl_rub"], m["win_rate"] * 100)

    # Output
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    excel = output_dir / "ab_v1_holdout.xlsx"
    write_report(excel, all_summaries, all_trades)
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df.to_parquet(output_dir / "ab_v1_holdout_trades.parquet", index=False)
        log.info("Trades parquet: %s", output_dir / "ab_v1_holdout_trades.parquet")

    log.info("Done. Output: %s", output_dir)
    if len(all_summaries) >= 2:
        log.info("=== A/B SUMMARY ===")
        by = {s["arm"]: s for s in all_summaries}
        for arm in arms_to_run:
            m = by[arm]
            log.info("  %-8s Sharpe=%6.2f  Trades=%4d  NetPnL=%+10.0f  Win=%.1f%%",
                     arm + ":", m["sharpe"], m["n_trades"], m["net_pnl_rub"], m["win_rate"] * 100)


if __name__ == "__main__":
    main()
