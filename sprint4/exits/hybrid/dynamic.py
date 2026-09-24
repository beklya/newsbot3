"""
sprint4/exits/hybrid/dynamic.py — DynamicHorizonExit для 4.10.

Идея: вместо фиксированного horizon_min=60 (Phase 2 best combo), использовать
LLM-предсказанный expected_timeframe → переопределить horizon на конкретный trade.

Mapping:
  expected_timeframe=instant  → 5  min
  expected_timeframe=short    → 15 min
  expected_timeframe=medium   → 60 min  (= Phase 2 baseline)
  expected_timeframe=slow     → 120 min

Если signal отсутствует или timeframe=None — fallback на Phase 2 horizon (60).

Активируется ТОЛЬКО если reality-check в 4.9 показал correlation ≥ 0.4.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

import pandas as pd

# Imports — same path setup как run_candidates.py
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))  # llm_signal_lookup
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))  # base, baseline

from llm_signal_lookup import LLMSignal  # noqa: E402
from base import Trade, ExitResult, ExitStrategy  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402

# LLM expected_timeframe → horizon_min override
TIMEFRAME_TO_HORIZON_MIN = {
    "instant": 5,
    "short": 15,
    "medium": 60,
    "slow": 120,
}


@dataclass
class DynamicHorizonExit(ExitStrategy):
    """Wraps BaselineFixedTpSl, переопределяя trade.horizon_min из LLM signal."""

    base_strategy: BaselineFixedTpSl
    timeframe_map: dict[str, int] = None

    def __post_init__(self):
        if self.timeframe_map is None:
            self.timeframe_map = TIMEFRAME_TO_HORIZON_MIN.copy()

    @property
    def name(self) -> str:
        return "dynamic_horizon"

    def simulate_with_signal(
        self, trade: Trade, bars: pd.DataFrame, signal: Optional[LLMSignal],
    ) -> ExitResult:
        """Не часть стандартного ExitStrategy interface — требует signal."""
        if signal is None or signal.expected_timeframe is None:
            return self.base_strategy.simulate(trade, bars)

        new_horizon = self.timeframe_map.get(signal.expected_timeframe)
        if new_horizon is None or new_horizon == trade.horizon_min:
            return self.base_strategy.simulate(trade, bars)

        modified_trade = replace(trade, horizon_min=new_horizon)
        return self.base_strategy.simulate(modified_trade, bars)

    def simulate(self, trade: Trade, bars: pd.DataFrame) -> ExitResult:
        """Fallback к baseline без signal — для compat с ExitStrategy ABC."""
        return self.base_strategy.simulate(trade, bars)
