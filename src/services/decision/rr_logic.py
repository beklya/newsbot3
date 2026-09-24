"""Phase 2 R:R decision logic — chooses side from predictions.

PHASE2.md §2.3 / §7.1:
  rr_long  = pred_mfe_long  / max(pred_mae_long,  MIN_MAE_PCT)
  rr_short = pred_mfe_short / max(pred_mae_short, MIN_MAE_PCT)

  if rr_long  >= RR_THRESHOLD and mfe_long  >= MIN_MFE_PCT and rr_long  >= rr_short:
      side = BUY
  elif rr_short >= RR_THRESHOLD and mfe_short >= MIN_MFE_PCT and rr_short > rr_long:
      side = SELL
  else: skip

  TP = entry × (1 ± TP_FRACTION × pred_mfe/100)
  SL = entry × (1 ∓ SL_BUFFER  × pred_mae/100)
  with absolute floors (sl_floor_pct=0.0005, tp_floor_pct=0.001).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.contracts.ml_prediction import MLPredictionPerHorizon


@dataclass(frozen=True)
class RRDecision:
    """Result of R:R analysis: chosen side + sizing inputs."""
    side: Optional[str]  # "BUY", "SELL", or None
    chosen_mfe_pct: float = 0.0
    chosen_mae_pct: float = 0.0
    rr_ratio: float = 0.0
    reject_reason: str = ""


def _pick_horizon(
    predictions: list[MLPredictionPerHorizon], horizon_str: str,
) -> Optional[MLPredictionPerHorizon]:
    for p in predictions:
        if p.horizon == horizon_str:
            return p
    return None


def evaluate_rr(
    predictions: list[MLPredictionPerHorizon],
    horizon_min: int,
    rr_threshold: float,
    min_mfe_pct: float,
    min_mae_pct: float,
) -> RRDecision:
    """Pick best side or reject. Phase 2 §2.3 logic."""
    horizon_str = f"{horizon_min}m"
    pred = _pick_horizon(predictions, horizon_str)
    if pred is None:
        return RRDecision(side=None, reject_reason=f"no prediction for horizon {horizon_str}")

    rr_long = pred.predicted_mfe_long_pct / max(pred.predicted_mae_long_pct, min_mae_pct)
    rr_short = pred.predicted_mfe_short_pct / max(pred.predicted_mae_short_pct, min_mae_pct)

    if (
        rr_long >= rr_threshold
        and pred.predicted_mfe_long_pct >= min_mfe_pct
        and rr_long >= rr_short
    ):
        return RRDecision(
            side="BUY",
            chosen_mfe_pct=pred.predicted_mfe_long_pct,
            chosen_mae_pct=pred.predicted_mae_long_pct,
            rr_ratio=rr_long,
        )

    if (
        rr_short >= rr_threshold
        and pred.predicted_mfe_short_pct >= min_mfe_pct
        and rr_short > rr_long
    ):
        return RRDecision(
            side="SELL",
            chosen_mfe_pct=pred.predicted_mfe_short_pct,
            chosen_mae_pct=pred.predicted_mae_short_pct,
            rr_ratio=rr_short,
        )

    return RRDecision(
        side=None,
        reject_reason=(
            f"R:R below threshold (long={rr_long:.2f} short={rr_short:.2f} "
            f"need ≥{rr_threshold} with mfe ≥ {min_mfe_pct})"
        ),
    )


@dataclass(frozen=True)
class TradeLevels:
    entry_price: float
    stop_loss: float
    take_profit: float
    sl_dist_abs: float  # для sizing


def compute_levels(
    last_close: float,
    side: str,
    pred_mfe_pct: float,
    pred_mae_pct: float,
    tp_fraction: float,
    sl_buffer: float,
    sl_floor_pct: float,
    tp_floor_pct: float,
) -> TradeLevels:
    """Compute SL/TP levels using Phase 2 formula с floors.

    entry_price — последний close из MLPrediction (paper bridge сам fill'нёт next-min open).
    Реальный entry скорректируется в Bridge при fill'е.
    """
    sl_dist_pct = max((pred_mae_pct * sl_buffer) / 100, sl_floor_pct)
    tp_dist_pct = max((pred_mfe_pct * tp_fraction) / 100, tp_floor_pct)

    if side == "BUY":
        sl = last_close * (1 - sl_dist_pct)
        tp = last_close * (1 + tp_dist_pct)
    else:  # SELL
        sl = last_close * (1 + sl_dist_pct)
        tp = last_close * (1 - tp_dist_pct)

    return TradeLevels(
        entry_price=last_close,
        stop_loss=sl,
        take_profit=tp,
        sl_dist_abs=abs(last_close - sl),
    )
