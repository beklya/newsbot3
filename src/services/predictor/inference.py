"""Inference engine: 67-dim feature vector → 4 predictions × 2 horizons.

Per (horizon, ticker) we get 4 predictions: mfe_long, mae_long, mfe_short, mae_short.
General vs mx_specific routing:
- ticker == "MIX" (canonical of Phase 2 "MX") → mx_specific
- otherwise → general
"""
from __future__ import annotations

import logging
from typing import Dict, List

import numpy as np

from src.contracts.ml_prediction import MLPredictionPerHorizon

from .model_loader import ModelBundle

log = logging.getLogger(__name__)

# MIN_MAE_PCT для R:R деления — Phase 2 PHASE2.md §7.1
MIN_MAE_PCT = 0.05


def _route_model_type(canonical_ticker: str) -> str:
    return "mx_specific" if canonical_ticker == "MIX" else "general"


def predict_all_horizons(
    bundle: ModelBundle,
    feature_vec: np.ndarray,
    canonical_ticker: str,
) -> List[MLPredictionPerHorizon]:
    """Run 8 XGBoost predictions (2 horizons × 4 targets) for the given ticker.

    feature_vec: 1D array of shape (67,). Internally reshaped to (1, 67)
    for XGBoost.
    """
    if feature_vec.ndim == 1:
        X = feature_vec.reshape(1, -1)
    else:
        X = feature_vec

    model_type = _route_model_type(canonical_ticker)
    predictions: List[MLPredictionPerHorizon] = []

    for horizon in bundle.horizons():
        # MFE/MAE поднимаем до 0 — обучали как regression без bound, но
        # отрицательные MFE/MAE бессмысленны (max excursion → ≥ 0).
        mfe_long = max(0.0, float(_predict_one(bundle, horizon, "mfe_long", model_type, X)))
        mae_long = max(0.0, float(_predict_one(bundle, horizon, "mae_long", model_type, X)))
        mfe_short = max(0.0, float(_predict_one(bundle, horizon, "mfe_short", model_type, X)))
        mae_short = max(0.0, float(_predict_one(bundle, horizon, "mae_short", model_type, X)))

        rr_long = mfe_long / max(mae_long, MIN_MAE_PCT)
        rr_short = mfe_short / max(mae_short, MIN_MAE_PCT)

        predictions.append(MLPredictionPerHorizon(
            horizon=horizon,
            predicted_mfe_long_pct=mfe_long,
            predicted_mae_long_pct=mae_long,
            predicted_mfe_short_pct=mfe_short,
            predicted_mae_short_pct=mae_short,
            rr_long=float(rr_long),
            rr_short=float(rr_short),
        ))

    return predictions


def _predict_one(bundle: ModelBundle, horizon: str, target: str, model_type: str, X: np.ndarray) -> float:
    model = bundle.get(horizon, target, model_type)
    if model is None:
        # mx_specific недоступен — fallback на general
        if model_type == "mx_specific":
            model = bundle.get(horizon, target, "general")
        if model is None:
            log.warning("model_missing horizon=%s target=%s model_type=%s — predicting 0", horizon, target, model_type)
            return 0.0
    return float(model.predict(X)[0])
