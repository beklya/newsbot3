"""Position sizing — PHASE2 §2.3 formula.

n_lots = risk_rub / (sl_dist_abs × lot_size), capped by leverage 10×.
Sprint 5 floor n_lots ≥ 1 (план 5.2: на низком risk_per_trade=0.5% дорогие
фьючерсы могут давать вычисленный 0 → торгуем 1 лот с warning).
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Dict

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizingResult:
    quantity: int  # final n_lots
    risk_rub: float  # actual риск (может превышать запрошенный из-за floor=1)
    notional_rub: float
    expected_pnl_rub: float  # at TP
    capped_by: str  # "risk", "leverage", "floor" или ""


def compute_size(
    ticker: str,
    side: str,
    entry_price: float,
    sl_dist_abs: float,
    pred_mfe_pct: float,
    tp_fraction: float,
    equity_rub: float,
    risk_per_trade_pct: float,
    leverage: int,
    lot_sizes: Dict[str, int],
) -> SizingResult:
    """Compute n_lots с floor=1, leverage cap, и expected_pnl.

    Returns SizingResult с финальными значениями для TradeSignalPayload.
    """
    lot_size = lot_sizes.get(ticker, 1)
    requested_risk_rub = equity_rub * risk_per_trade_pct

    if sl_dist_abs <= 0:
        log.warning("sizing_zero_sl_dist ticker=%s — falling back to lot=1", ticker)
        n_lots = 1
        capped_by = "floor"
    else:
        # Идеальный размер по риску
        pnl_per_lot_at_sl = sl_dist_abs * lot_size
        n_lots_by_risk = int(requested_risk_rub / pnl_per_lot_at_sl)
        if n_lots_by_risk <= 0:
            n_lots = 1
            capped_by = "floor"
            log.warning(
                "sizing_floor_to_1 ticker=%s requested_risk=%.2f pnl_per_lot_sl=%.2f",
                ticker, requested_risk_rub, pnl_per_lot_at_sl,
            )
        else:
            n_lots = n_lots_by_risk
            capped_by = "risk"

    notional_per_lot = lot_size * entry_price
    total_notional = n_lots * notional_per_lot

    # Leverage cap
    max_notional = equity_rub * leverage
    if total_notional > max_notional:
        n_lots_by_leverage = int(max_notional / notional_per_lot)
        if n_lots_by_leverage < n_lots:
            n_lots = max(1, n_lots_by_leverage)
            capped_by = "leverage"
            total_notional = n_lots * notional_per_lot

    actual_risk_rub = sl_dist_abs * lot_size * n_lots
    tp_dist_pct = (pred_mfe_pct * tp_fraction) / 100
    expected_pnl_rub = entry_price * tp_dist_pct * lot_size * n_lots

    return SizingResult(
        quantity=n_lots,
        risk_rub=actual_risk_rub,
        notional_rub=total_notional,
        expected_pnl_rub=expected_pnl_rub,
        capped_by=capped_by,
    )
