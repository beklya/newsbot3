"""Tests for sizing — floor, leverage cap, risk computation."""
from __future__ import annotations

from src.services.decision.sizing import compute_size


def test_normal_sizing():
    """500k equity × 0.5% risk = 2500 руб. SL_dist=0.5 руб per share, lot=10
    → pnl_per_lot_at_sl = 5. n_lots = 500."""
    r = compute_size(
        ticker="GAZP", side="BUY",
        entry_price=100.0, sl_dist_abs=0.5,
        pred_mfe_pct=0.5,
        tp_fraction=0.7,
        equity_rub=500_000.0, risk_per_trade_pct=0.005,
        leverage=10, lot_sizes={"GAZP": 10},
    )
    assert r.quantity == 500
    assert r.capped_by == "risk"


def test_leverage_cap():
    """1 лот = 1M руб (100k × 10). Leverage 10× × 500k = 5M max → 5 лотов max."""
    r = compute_size(
        ticker="LKOH", side="BUY",
        entry_price=100_000.0, sl_dist_abs=0.01,  # тонкий SL → много по риску
        pred_mfe_pct=0.5,
        tp_fraction=0.7,
        equity_rub=500_000.0, risk_per_trade_pct=0.5,  # огромный риск
        leverage=10, lot_sizes={"LKOH": 1},
    )
    # max_notional = 500_000 × 10 = 5_000_000
    # notional per lot = 100_000 × 1 = 100_000
    # max n_lots = 50
    assert r.quantity == 50
    assert r.capped_by == "leverage"


def test_floor_to_one():
    """Дорогой инструмент с tight SL → n_lots_by_risk = 0 → floor to 1."""
    r = compute_size(
        ticker="LKOH", side="BUY",
        entry_price=100_000.0, sl_dist_abs=500.0,  # 500 руб per share × 1 lot = 500 руб риск
        pred_mfe_pct=0.5,
        tp_fraction=0.7,
        equity_rub=500_000.0, risk_per_trade_pct=0.0001,  # 0.01% = 50 руб
        leverage=10, lot_sizes={"LKOH": 1},
    )
    assert r.quantity == 1
    assert r.capped_by == "floor"


def test_expected_pnl():
    """expected_pnl = entry × tp_dist_pct × lot_size × qty."""
    r = compute_size(
        ticker="GAZP", side="BUY",
        entry_price=100.0, sl_dist_abs=0.5,
        pred_mfe_pct=1.0,
        tp_fraction=0.7,
        equity_rub=500_000.0, risk_per_trade_pct=0.005,
        leverage=10, lot_sizes={"GAZP": 10},
    )
    # n_lots = 500. tp_dist_pct = 0.7 × 0.01 = 0.007
    # expected_pnl = 100 × 0.007 × 10 × 500 = 3500
    assert abs(r.expected_pnl_rub - 3500.0) < 1.0


def test_zero_sl_dist_falls_back_to_one():
    r = compute_size(
        ticker="GAZP", side="BUY",
        entry_price=100.0, sl_dist_abs=0.0,
        pred_mfe_pct=0.5, tp_fraction=0.7,
        equity_rub=500_000.0, risk_per_trade_pct=0.005,
        leverage=10, lot_sizes={"GAZP": 10},
    )
    assert r.quantity == 1
