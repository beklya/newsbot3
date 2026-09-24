"""Tests for inference.predict_all_horizons on a real loaded bundle."""
from __future__ import annotations

import numpy as np
import pytest

from src.services.predictor.inference import predict_all_horizons


def test_predict_returns_two_horizons(real_bundle):
    vec = np.zeros(67, dtype=np.float32)
    preds = predict_all_horizons(real_bundle, vec, canonical_ticker="GAZP")
    horizons = {p.horizon for p in preds}
    assert horizons == {"30m", "60m"}


def test_predict_non_negative_mfe_mae(real_bundle):
    """MFE/MAE clipped to ≥ 0 regardless of model output."""
    vec = np.zeros(67, dtype=np.float32)
    preds = predict_all_horizons(real_bundle, vec, canonical_ticker="GAZP")
    for p in preds:
        assert p.predicted_mfe_long_pct >= 0
        assert p.predicted_mae_long_pct >= 0
        assert p.predicted_mfe_short_pct >= 0
        assert p.predicted_mae_short_pct >= 0


def test_predict_rr_consistent(real_bundle):
    """rr_long = mfe_long / max(mae_long, 0.05)."""
    vec = np.zeros(67, dtype=np.float32)
    preds = predict_all_horizons(real_bundle, vec, canonical_ticker="GAZP")
    for p in preds:
        expected_rr_long = p.predicted_mfe_long_pct / max(p.predicted_mae_long_pct, 0.05)
        assert p.rr_long == pytest.approx(expected_rr_long)


def test_predict_routes_mx_specific_for_mix(real_bundle):
    """Ticker 'MIX' routes to mx_specific; other tickers route to general.

    We verify routing by checking different predictions for same feature_vec
    (general and mx_specific are trained на different data subsets).
    """
    vec = np.ones(67, dtype=np.float32)  # synthetic non-zero
    preds_general = predict_all_horizons(real_bundle, vec, canonical_ticker="GAZP")
    preds_mix = predict_all_horizons(real_bundle, vec, canonical_ticker="MIX")
    diffs = [
        abs(preds_general[i].predicted_mfe_long_pct - preds_mix[i].predicted_mfe_long_pct)
        for i in range(2)
    ]
    # Не критично что отличаются на всех тикерах, но хотя бы одна horizon
    # должна давать разный output (модели обучены на разных подвыборках).
    assert max(diffs) > 0, f"general vs mx_specific identical predictions: {diffs}"
