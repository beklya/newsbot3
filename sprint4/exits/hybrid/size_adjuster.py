"""
sprint4/exits/hybrid/size_adjuster.py — pre-simulate size adjustment layer.

SizeAdjuster масштабирует Phase 2 size_lots на основе LLM impact_strength.
Используется в 4.9 candidate C.

Concrete adjusters:
  - IdentitySize: returns trade.size_lots unchanged (baseline)
  - ImpactScale: size_lots × impact_strength, rounded, min 1
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from llm_signal_lookup import LLMSignal


class SizeAdjuster(ABC):
    @abstractmethod
    def adjust(self, base_size_lots: int, signal: Optional[LLMSignal], ticker: str | None = None) -> int:
        ...


@dataclass
class IdentitySize(SizeAdjuster):
    """No-op — возвращает base_size_lots без изменений."""
    def adjust(self, base_size_lots: int, signal: Optional[LLMSignal], ticker: str | None = None) -> int:
        return base_size_lots


@dataclass
class ImpactScale(SizeAdjuster):
    """size_lots ← max(1, round(base × impact_strength)).

    Использует impact_strength ИМЕННО для trade.ticker (per-ticker lookup),
    не top_ticker. Если signal None / нет per-ticker запис / impact None — base.

    Backward compat: если ticker=None (старый вызов), fallback на signal.impact_strength.
    """
    min_lots: int = 1

    def adjust(self, base_size_lots: int, signal: Optional[LLMSignal], ticker: str | None = None) -> int:
        if signal is None:
            return base_size_lots
        impact: float | None = None
        if ticker is not None:
            ts = signal.get_for_ticker(ticker)
            if ts is not None:
                impact = ts.impact_strength
        if impact is None:
            impact = signal.impact_strength
        if impact is None:
            return base_size_lots
        scaled = round(base_size_lots * float(impact))
        return max(self.min_lots, scaled)
