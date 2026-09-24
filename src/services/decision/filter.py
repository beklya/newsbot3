"""DirectionFilter — port of sprint4/exits/hybrid/trade_filter.py for live.

Sprint 4 B-filter rule (winning, **LENIENT** semantics, restored 2026-06-06):

  INCLUDE trade by default.  Only REJECT on EXPLICIT mismatch:
    - LLM mentions this ticker AND direction != trade.side  → REJECT
    - LLM mentions this ticker AND confidence < threshold   → REJECT

  Otherwise INCLUDE:
    - No per-ticker signal in LLM tickers[]  → INCLUDE (absent ≠ veto)
    - direction == "neutral"                 → INCLUDE (no negative endorsement)

  Rationale (from sprint4/exits/hybrid/trade_filter.py:48-54):
    "LLM не упомянул этот ticker — не блокируем (baseline behavior),
     т.к. отсутствие сигнала ≠ negative endorsement."

  Validation: walk_forward_b_filter.py 13 folds reproduce Sharpe 6.42 ONLY with
  this LENIENT variant.  The previous STRICT port (Sprint 5 wired Sep 2026
  → reverted Sprint 6 Jun 2026) gave Sharpe 0 / 0 trades on the same data —
  see docs/B_FILTER_ARCHITECTURE.md "Filter semantics regression" section.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.contracts.enriched_news import EnrichedNewsEvent, TickerImpact


# Side ↔ direction mapping для bridge между TradeSignal (BUY/SELL) и EnrichedNews (long/short)
SIDE_TO_DIRECTION = {"BUY": "long", "SELL": "short"}
DIRECTION_TO_SIDE = {"long": "BUY", "short": "SELL"}


def get_ticker_impact(
    event: EnrichedNewsEvent, ticker: str,
) -> Optional[TickerImpact]:
    """Find TickerImpact for the given canonical ticker. None if not in tickers[].

    Sprint 4.1 validator уже нормализовал ticker в canonical (SI/MIX/YDEX/GLDRUB).
    Simple exact match достаточно.
    """
    for t in event.payload.tickers:
        if t.ticker == ticker:
            return t
    return None


@dataclass(frozen=True)
class FilterDecision:
    """Outcome of direction filter: include или reject + reason для logging."""
    include: bool
    reject_reason: str = ""


def apply_direction_filter(
    event: EnrichedNewsEvent,
    ticker: str,
    side: str,
    min_confidence: float = 0.5,
) -> FilterDecision:
    """B_direction_filter rule.

    Args:
        event: enriched news context
        ticker: canonical ticker (already validated)
        side: TradeSignal side: "BUY" or "SELL"
        min_confidence: threshold для confidence drop

    Returns:
        FilterDecision(include=True) или FilterDecision(include=False, reason=...)
    """
    expected_direction = SIDE_TO_DIRECTION.get(side)
    if expected_direction is None:
        return FilterDecision(False, f"unknown side: {side}")

    ti = get_ticker_impact(event, ticker)
    if ti is None:
        # Sprint 4 design: absence ≠ veto.  INCLUDE.
        # (LLM may have legitimately not mentioned the ticker in this news,
        # but XGBoost decided to trade it based on price/technical features.
        # Lack of mention is not a "no" — only an explicit "no" is a no.)
        return FilterDecision(True)

    if ti.direction == "neutral":
        # Sprint 4 design: neutral ≠ negative endorsement.  INCLUDE.
        return FilterDecision(True)

    # Below: LLM explicitly endorses a direction.  Must match.
    if ti.direction != expected_direction:
        return FilterDecision(
            False,
            f"LLM direction={ti.direction} != expected {expected_direction} for side {side}",
        )

    if ti.confidence < min_confidence:
        return FilterDecision(
            False,
            f"confidence {ti.confidence:.2f} < threshold {min_confidence:.2f}",
        )

    return FilterDecision(True)
