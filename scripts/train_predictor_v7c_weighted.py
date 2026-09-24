r"""Sprint 6.1 — v7c sample-weighted retrain.

Same as train_predictor_fold13.py but assigns Phase 2 events a higher sample
weight than Y6-only events (which have noisier MFE/MAE due to broader corpus).

Phase 2 events have id in `phase2_id_set`.  Y6-only events get weight 1.
Phase 2 events get weight `--phase2-weight` (default 3).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Reuse most of train_predictor_fold13.py
from scripts.train_predictor_fold13 import (  # noqa: E402
    XGB_PARAMS, HORIZONS, TARGETS, MODEL_TYPES,
    TRAIN_MONTHS, TEST_MONTHS, STEP_MONTHS, PURGE,
    MIN_TRAIN_SAMPLES, MIN_TEST_SAMPLES, MIN_MX_SAMPLES,
    get_target_columns, merge_features_and_targets, build_folds,
    compute_sample_weights, train_single_model, verify_models,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("train_v7c")


def train_fold_models_weighted(
    df: pd.DataFrame, feature_cols: list[str], fold: dict, models_dir: Path,
    phase2_ids: set[str], phase2_weight: float,
) -> dict:
    train_df = df[fold["train_mask"]].copy()
    log.info("Training fold: test_start=%s train_n=%d",
             fold["test_start"].date(), len(train_df))

    X_train_full = train_df[feature_cols].values

    # Base weights (1/sqrt(ticker_count))
    base_weights = compute_sample_weights(train_df["_ticker"])
    # Apply Phase 2 boost
    is_phase2 = train_df["_id"].isin(phase2_ids).values
    boost = np.where(is_phase2, phase2_weight, 1.0)
    final_weights = (base_weights * boost) / np.mean(base_weights * boost)
    log.info("  Phase 2 events in train: %d (%.1f%%)  weight x%.1f",
             int(is_phase2.sum()), 100*is_phase2.mean(), phase2_weight)

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
                model = train_single_model(X_train_full, y_general, final_weights,
                                            label=f"{target_col}/general")
            else:
                if X_train_mx is None:
                    log.warning("  [%s/mx_specific] skipped — only %d MX", target_col, n_mx)
                    continue
                y_mx = train_df.loc[mx_mask, target_col].values
                w_mx = final_weights[mx_mask.values]
                model = train_single_model(X_train_mx, y_mx, w_mx,
                                            label=f"{target_col}/mx_specific")
            if model is None:
                continue
            joblib.dump(model, file_path)
            saved[f"{target_col}_{model_type}"] = str(file_path)
            log.info("  [%s/%s] trained %.1fs -> %s",
                     target_col, model_type, time.time() - t_start, file_path.name)

    log.info("Fold done in %.1fs (%d/%d models)",
             time.time() - t_total, len(saved), len(target_cols) * len(MODEL_TYPES))
    return saved


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2.parquet")
    ap.add_argument("--targets", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2.parquet")
    ap.add_argument("--out", type=Path,
                    default=PROJECT_ROOT / "data" / "models" / "predictor" / "v7c_weighted")
    ap.add_argument("--phase2-features", type=Path,
                    default=Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем\Проблема 5 - новое начало\phase2_mfe\features_mfe.parquet"),
                    help="Path used to build phase2_ids set")
    ap.add_argument("--phase2-weight", type=float, default=3.0,
                    help="Multiplier on Phase 2 events vs Y6 (default 3x)")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    log.info("Building Phase 2 id set from %s", args.phase2_features)
    p2 = pd.read_parquet(args.phase2_features)
    phase2_ids = set(p2["_id"].astype(str).unique().tolist())
    log.info("  Phase 2 IDs: %d", len(phase2_ids))

    df, feature_cols = merge_features_and_targets(args.targets, args.features)
    folds = build_folds(df)
    if not folds:
        log.error("no folds")
        return 1

    fold_13 = folds[-1]
    log.info("Using Fold 13 for training: test [%s -> %s]",
             fold_13["test_start"].date(), fold_13["test_end"].date())

    feature_order_file = args.out / "feature_order.json"
    feature_order_file.write_text(
        json.dumps(feature_cols, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Feature order persisted: %s (%d features)",
             feature_order_file.name, len(feature_cols))

    train_fold_models_weighted(df, feature_cols, fold_13, args.out,
                                phase2_ids, args.phase2_weight)
    verify_models(args.out, feature_cols, df, fold_13)
    log.info("Done. Models in: %s", args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
