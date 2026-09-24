"""
sprint4/exits/hybrid/trade_filter.py — pre-simulate filter layer.

TradeFilter decides ВКЛЮЧАТЬ ли trade в backtest на основе LLM signal.
Используется в 4.9 candidates B/C/D.

Concrete filters:
  - DirectionFilter: skip если LLM direction != trade.side OR confidence < threshold
  - PerTickerExcludeFilter: skip если trade.ticker в excluded list
  - SequentialFilter: компонует несколько фильтров (ANY rejection = skip)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from llm_signal_lookup import LLMSignal


class TradeFilter(ABC):
    """Decide if a trade should be included given its LLM signal."""

    @abstractmethod
    def include(self, trade_side: str, trade_ticker: str, signal: Optional[LLMSignal]) -> bool:
        ...


@dataclass
class AlwaysInclude(TradeFilter):
    """Baseline — пропускает все trades. Cand A."""
    def include(self, trade_side: str, trade_ticker: str, signal: Optional[LLMSignal]) -> bool:
        return True


@dataclass
class DirectionFilter(TradeFilter):
    """Skip если LLM direction не совпадает с trade.side OR confidence < threshold.

    Phase 2 side: "buy" / "sell"
    LLM direction: "long" / "short" / "neutral"
    """
    min_confidence: float = 0.5
    side_to_direction = {"buy": "long", "sell": "short", "BUY": "long", "SELL": "short"}

    def include(self, trade_side: str, trade_ticker: str, signal: Optional[LLMSignal]) -> bool:
        if signal is None:
            # Нет LLM enrichment — baseline behavior (включаем)
            return True
        # Per-ticker lookup — Phase 2 trade имеет ticker, LLM возвращает массив tickers.
        # Используем direction/confidence ИМЕННО для trade.ticker, не для top_ticker.
        ts = signal.get_for_ticker(trade_ticker)
        if ts is None or ts.direction is None or ts.direction == "neutral":
            # LLM не упомянул этот ticker — не блокируем (baseline behavior),
            # т.к. отсутствие сигнала ≠ negative endorsement.
            return True
        expected = self.side_to_direction.get(trade_side, trade_side)
        if ts.direction != expected:
            return False  # явное несогласие — drop
        if ts.confidence is None or ts.confidence < self.min_confidence:
            return False
        return True


@dataclass
class PerTickerExcludeFilter(TradeFilter):
    """Skip если trade.ticker в excluded list."""
    excluded_tickers: frozenset[str]

    def include(self, trade_side: str, trade_ticker: str, signal: Optional[LLMSignal]) -> bool:
        return trade_ticker not in self.excluded_tickers


@dataclass
class SequentialFilter(TradeFilter):
    """Композиция фильтров — ANY rejection = skip."""
    filters: list[TradeFilter]

    def include(self, trade_side: str, trade_ticker: str, signal: Optional[LLMSignal]) -> bool:
        return all(f.include(trade_side, trade_ticker, signal) for f in self.filters)
