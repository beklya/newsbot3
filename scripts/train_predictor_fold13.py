r"""
scripts/train_predictor_fold13.py — Sprint 5 / Commit 5.1
==========================================================

Adapts Phase 2 backtest_mfe.py training pipeline для production Predictor
service. Тренирует ТОЛЬКО Fold 13 (latest) и ТОЛЬКО 30m+60m horizons:
2 horizons × 4 targets (mfe_long, mae_long, mfe_short, mae_short)
× 2 model_types (general, mx_specific) = **16 моделей**.

Output: joblib-сериализованные XGBoost регрессоры в
  data/models/predictor/v1/{horizon}_{target}_{model_type}.joblib

PHASE2 §2.3 hyperparams фиксированы:
  max_depth=4, n_estimators=150, learning_rate=0.05,
  subsample=0.8, colsample_bytree=0.7

Walk-forward Fold 13 ≡ test [2026-01-03, 2026-04-03] (12mo train + 3mo test).

Зачем:
  Phase 2 backtest НЕ сохранял обученные модели — только trade-результаты.
  Predictor service нуждается в XGBoost artifacts для inference на live
  news:enriched events. Sprint 5.1 training step = подготовка артефактов.

Запуск (~30 минут):
  python scripts/train_predictor_fold13.py
  python scripts/train_predictor_fold13.py --verify   # только load + smoke predict
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Project root для совместимости при прямом запуске
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("train_predictor_fold13")

# === Источники Phase 2 (read-only) ===
PHASE2_DIR = Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe")
DEFAULT_TARGETS = PHASE2_DIR / "targets_mfe.parquet"
DEFAULT_FEATURES = PHASE2_DIR / "features_mfe.parquet"

# === Output ===
DEFAULT_MODELS_DIR = _PROJECT_ROOT / "data" / "models" / "predictor" / "v1"

# === Hyperparams (PHASE2 §2.3) ===
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

# === Sprint 5 scope (subset of Phase 2 full 80) ===
HORIZONS = [30, 60]  # minutes — matches MLPredictionPayload.Horizon Literal["30m", "60m"]
TARGETS = ["mfe_long", "mae_long", "mfe_short", "mae_short"]
MODEL_TYPES = ["general", "mx_specific"]

# === Walk-forward (matches PHASE2 §2.3 settings) ===
TRAIN_MONTHS = 12
TEST_MONTHS = 3
STEP_MONTHS = 3
PURGE = pd.Timedelta(minutes=30)
MIN_TRAIN_SAMPLES = 500
MIN_TEST_SAMPLES = 30
MIN_MX_SAMPLES = 500


def get_target_columns() -> List[str]:
    """16 target column names: 2 horizons × 4 targets."""
    return [f"{t}_{H}m" for H in HORIZONS for t in TARGETS]


def merge_features_and_targets(targets_path: Path, features_path: Path) -> tuple[pd.DataFrame, list[str]]:
    """Load and inner-merge by id. Returns (df, feature_cols)."""
    log.info("Loading parquets...")
    targets = pd.read_parquet(targets_path)
    features = pd.read_parquet(features_path)
    log.info("  Targets:  %d × %d", len(targets), len(targets.columns))
    log.info("  Features: %d × %d", len(features), len(features.columns))

    targets["id"] = targets["id"].astype(str)
    features["_id"] = features["_id"].astype(str)

    df = features.merge(
        targets.drop(columns=["datetime", "ticker"]),
        left_on="_id", right_on="id", how="inner",
    )
    df["_dt"] = pd.to_datetime(df["_datetime"])
    df = df.sort_values("_dt").reset_index(drop=True)
    log.info("  Merged:   %d rows", len(df))

    meta_cols = {
        "_id", "_datetime", "_dt", "_ticker", "id", "ticker",
        "_entry_price", "_entry_ts",
    }
    all_phase2_targets = set()
    for H in [1, 2, 3, 4, 5, 10, 15, 30, 45, 60]:
        for t in TARGETS:
            all_phase2_targets.add(f"{t}_{H}m")

    feature_cols = [c for c in df.columns if c not in meta_cols and c not in all_phase2_targets]
    log.info("  Features used: %d, targets used (Sprint 5 subset): %d",
             len(feature_cols), len(get_target_columns()))
    return df, feature_cols


def build_folds(df: pd.DataFrame) -> list[dict]:
    """Reproduce backtest_mfe.py walk-forward exactly. Returns list of fold metadata."""
    first_dt = df["_dt"].iloc[0]
    last_dt = df["_dt"].iloc[-1]
    log.info("Date range: %s → %s", first_dt.date(), last_dt.date())

    folds: list[dict] = []
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


def compute_sample_weights(tickers: pd.Series) -> np.ndarray:
    """Phase 2 §2.3: weight = 1/sqrt(count_per_ticker), normalize mean=1."""
    counts = tickers.value_counts()
    weights = tickers.map(lambda t: 1.0 / np.sqrt(counts[t]))
    weights = weights / weights.mean()
    return weights.values


def train_single_model(
    X_train: np.ndarray,
    y: np.ndarray,
    sample_weight: Optional[np.ndarray],
    label: str,
):
    """Train one XGBoost regressor. NaN target rows dropped."""
    import xgboost as xgb

    mask = ~np.isnan(y)
    n_clean = int(mask.sum())
    if n_clean < 100:
        log.warning("  [%s] too few clean samples (%d) — skipping", label, n_clean)
        return None
    X_clean = X_train[mask]
    y_clean = y[mask]
    w_clean = sample_weight[mask] if sample_weight is not None else None

    model = xgb.XGBRegressor(**XGB_PARAMS)
    model.fit(X_clean, y_clean, sample_weight=w_clean)
    return model


def train_fold_models(
    df: pd.DataFrame, feature_cols: list[str], fold: dict, models_dir: Path,
) -> dict:
    """Train all 16 models for the given fold and dump to disk."""
    import joblib

    train_df = df[fold["train_mask"]].copy()
    log.info("Training fold: test_start=%s train_n=%d",
             fold["test_start"].date(), len(train_df))

    X_train_full = train_df[feature_cols].values

    # === GENERAL (sample-weighted) ===
    sample_weights = compute_sample_weights(train_df["_ticker"])

    # === MX-specific ===
    mx_mask = train_df["_ticker"] == "MX"
    n_mx = int(mx_mask.sum())
    log.info("  Train: total=%d MX=%d (mx_specific %s)",
             len(train_df), n_mx, "ENABLED" if n_mx >= MIN_MX_SAMPLES else "SKIPPED")
    X_train_mx = train_df.loc[mx_mask, feature_cols].values if n_mx >= MIN_MX_SAMPLES else None

    target_cols = get_target_columns()
    saved: dict[str, str] = {}
    t_total = time.time()

    for target_col in target_cols:
        y_general = train_df[target_col].values

        for model_type in MODEL_TYPES:
            t_start = time.time()
            file_path = models_dir / f"{target_col}_{model_type}.joblib"

            if model_type == "general":
                model = train_single_model(
                    X_train_full, y_general, sample_weights,
                    label=f"{target_col}/general",
                )
            else:
                if X_train_mx is None:
                    log.warning("  [%s/mx_specific] skipped — only %d MX samples", target_col, n_mx)
                    continue
                y_mx = train_df.loc[mx_mask, target_col].values
                model = train_single_model(
                    X_train_mx, y_mx, sample_weight=None,
                    label=f"{target_col}/mx_specific",
                )

            if model is None:
                continue

            joblib.dump(model, file_path)
            saved[f"{target_col}_{model_type}"] = str(file_path)
            log.info("  [%s/%s] trained %.1fs → %s",
                     target_col, model_type, time.time() - t_start, file_path.name)

    log.info("Fold training done. Models saved: %d/%d in %.1fs",
             len(saved), len(target_cols) * len(MODEL_TYPES), time.time() - t_total)
    return saved


def verify_models(models_dir: Path, feature_cols: list[str], df: pd.DataFrame, fold: dict) -> None:
    """Smoke test: load every model and predict on first 5 test rows. Sanity-bound predictions."""
    import joblib

    test_df = df[fold["test_mask"]].head(5).copy()
    X_test = test_df[feature_cols].values
    log.info("Verifying %d models on %d test rows...",
             len(get_target_columns()) * len(MODEL_TYPES), len(X_test))

    issues = 0
    for target_col in get_target_columns():
        for model_type in MODEL_TYPES:
            path = models_dir / f"{target_col}_{model_type}.joblib"
            if not path.exists():
                log.warning("  MISSING %s", path.name)
                issues += 1
                continue
            model = joblib.load(path)
            preds = model.predict(X_test)
            # MFE/MAE are %-of-price > 0; reasonable range 0..10%
            if not (np.all(np.isfinite(preds)) and np.all(preds >= -1.0) and np.all(preds < 20.0)):
                log.warning("  SUSPECT %s — preds out of range [%.2f, %.2f]",
                            path.name, preds.min(), preds.max())
                issues += 1
            else:
                log.info("  OK %-40s pred[0..4]=%s", path.name,
                         np.array2string(preds, precision=3, suppress_small=True))

    if issues:
        log.warning("Verify finished with %d issues", issues)
    else:
        log.info("Verify clean ✓")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default=str(DEFAULT_TARGETS))
    parser.add_argument("--features", default=str(DEFAULT_FEATURES))
    parser.add_argument("--out", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--verify", action="store_true",
                        help="Только load + smoke predict (без training)")
    parser.add_argument("--single-fold", action="store_true",
                        help="Skip walk-forward fold building; train on ALL rows of --features. "
                             "Use when features parquet содержит ровно Fold 13 train subset "
                             "(no test rows present). Smoke verify runs on tail 5 train rows.")
    args = parser.parse_args()

    models_dir = Path(args.out)
    models_dir.mkdir(parents=True, exist_ok=True)

    df, feature_cols = merge_features_and_targets(Path(args.targets), Path(args.features))

    if args.single_fold:
        all_mask = pd.Series(True, index=df.index)
        empty_mask = pd.Series(False, index=df.index)
        # Smoke test_mask = last 5 train rows (sanity-only, not real evaluation)
        smoke_mask = empty_mask.copy()
        smoke_mask.iloc[-5:] = True
        fold_13 = {
            "test_start": df["_dt"].max(),
            "test_end": df["_dt"].max(),
            "train_mask": all_mask,
            "test_mask": smoke_mask,
        }
        log.info("Single-fold mode: train on ALL %d rows (no walk-forward)", len(df))
        log.info("  date range: %s → %s",
                 df["_dt"].min().date(), df["_dt"].max().date())
    else:
        folds = build_folds(df)
        if not folds:
            log.error("No folds generated — check data")
            sys.exit(1)
        # Sprint 5.1: train ONLY Fold 13 (the latest)
        fold_13 = folds[-1]
        log.info("Using Fold %d (latest) for training: test [%s → %s]",
                 len(folds), fold_13["test_start"].date(), fold_13["test_end"].date())

    if args.verify:
        verify_models(models_dir, feature_cols, df, fold_13)
        return

    # Also persist feature order (for inference: order matters in XGBoost)
    feature_order_file = models_dir / "feature_order.json"
    import json
    feature_order_file.write_text(json.dumps(feature_cols, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Feature order persisted: %s (%d features)", feature_order_file.name, len(feature_cols))

    # Train + dump
    train_fold_models(df, feature_cols, fold_13, models_dir)

    # Verify
    verify_models(models_dir, feature_cols, df, fold_13)

    log.info("Done. Models in: %s", models_dir)


if __name__ == "__main__":
    main()
