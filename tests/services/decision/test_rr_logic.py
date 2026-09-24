"""Tests for Phase 2 R:R logic and level computation."""
from __future__ import annotations

from src.services.decision.rr_logic import compute_levels, evaluate_rr


def test_pick_long(prediction_event_factory):
    """rr_long >> rr_short, mfe_long >= MIN_MFE_PCT → side=BUY."""
    ev = prediction_event_factory(
        mfe_long_60m=0.5, mae_long_60m=0.2,    # rr_long = 2.5
        mfe_short_60m=0.1, mae_short_60m=0.3,  # rr_short = 0.33
    )
    rr = evaluate_rr(ev.payload.predictions, horizon_min=60,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side == "BUY"
    assert rr.chosen_mfe_pct == 0.5
    assert rr.rr_ratio == 2.5


def test_pick_short(prediction_event_factory):
    """rr_short > rr_long, mfe_short >= MIN_MFE_PCT → side=SELL."""
    ev = prediction_event_factory(
        mfe_long_60m=0.1, mae_long_60m=0.3,    # rr_long = 0.33
        mfe_short_60m=0.5, mae_short_60m=0.2,  # rr_short = 2.5
    )
    rr = evaluate_rr(ev.payload.predictions, horizon_min=60,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side == "SELL"
    assert rr.chosen_mfe_pct == 0.5


def test_reject_rr_below_threshold(prediction_event_factory):
    """Both sides RR < threshold → skip."""
    ev = prediction_event_factory(
        mfe_long_60m=0.2, mae_long_60m=0.2,    # rr_long = 1.0
        mfe_short_60m=0.15, mae_short_60m=0.2,  # rr_short = 0.75
    )
    rr = evaluate_rr(ev.payload.predictions, horizon_min=60,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side is None
    assert "R:R below" in rr.reject_reason


def test_reject_mfe_too_small(prediction_event_factory):
    """RR is high but absolute MFE < MIN_MFE_PCT → skip."""
    ev = prediction_event_factory(
        mfe_long_60m=0.10, mae_long_60m=0.02,  # rr_long = 5.0 но mfe < 0.15
        mfe_short_60m=0.05, mae_short_60m=0.02,  # rr_short = 2.5 mfe < 0.15
    )
    rr = evaluate_rr(ev.payload.predictions, horizon_min=60,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side is None


def test_pick_30m_horizon(prediction_event_factory):
    """horizon_min=30 → uses 30m predictions, ignores 60m."""
    ev = prediction_event_factory(
        mfe_long_30m=0.5, mae_long_30m=0.2,   # rr 2.5 на 30m
        mfe_short_30m=0.1, mae_short_30m=0.3,
        mfe_long_60m=0.05, mae_long_60m=0.5,  # 60m мусор
        mfe_short_60m=0.05, mae_short_60m=0.5,
    )
    rr = evaluate_rr(ev.payload.predictions, horizon_min=30,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side == "BUY"


def test_no_prediction_for_horizon(prediction_event_factory):
    """horizon_min=15 (нет такого) → skip."""
    ev = prediction_event_factory()
    rr = evaluate_rr(ev.payload.predictions, horizon_min=15,
                     rr_threshold=2.0, min_mfe_pct=0.15, min_mae_pct=0.05)
    assert rr.side is None
    assert "no prediction" in rr.reject_reason


def test_compute_levels_buy():
    """BUY: TP above entry, SL below entry."""
    levels = compute_levels(
        last_close=100.0, side="BUY",
        pred_mfe_pct=0.5, pred_mae_pct=0.2,
        tp_fraction=0.7, sl_buffer=1.2,
        sl_floor_pct=0.0005, tp_floor_pct=0.001,
    )
    # TP = 100 × (1 + 0.7 × 0.005) = 100.35
    # SL = 100 × (1 - 1.2 × 0.002) = 99.76
    assert levels.take_profit > 100.0
    assert levels.stop_loss < 100.0
    assert abs(levels.take_profit - 100.35) < 0.01
    assert abs(levels.stop_loss - 99.76) < 0.01


def test_compute_levels_sell():
    """SELL: TP below entry, SL above entry."""
    levels = compute_levels(
        last_close=100.0, side="SELL",
        pred_mfe_pct=0.5, pred_mae_pct=0.2,
        tp_fraction=0.7, sl_buffer=1.2,
        sl_floor_pct=0.0005, tp_floor_pct=0.001,
    )
    assert levels.take_profit < 100.0
    assert levels.stop_loss > 100.0


def test_compute_levels_floors_kick_in():
    """Tiny predicted MFE/MAE → floors apply."""
    levels = compute_levels(
        last_close=100.0, side="BUY",
        pred_mfe_pct=0.01, pred_mae_pct=0.01,  # too small
        tp_fraction=0.7, sl_buffer=1.2,
        sl_floor_pct=0.0005, tp_floor_pct=0.001,
    )
    # tp_dist = max(0.7 × 0.0001, 0.001) = 0.001 → tp = 100.1
    # sl_dist = max(1.2 × 0.0001, 0.0005) = 0.0005 → sl = 99.95
    assert abs(levels.take_profit - 100.1) < 0.01
    assert abs(levels.stop_loss - 99.95) < 0.01
