"""Sprint 6.1 regression guard — prod filter.py ≡ sprint4 walk-forward reference.

Walk-forward Sharpe 6.42 (docs/B_FILTER_ARCHITECTURE.md) was produced by
sprint4/exits/hybrid/trade_filter.py::DirectionFilter (LENIENT semantics).
Production code lives in src/services/decision/filter.py; after the Sprint 6.1
revert it should mirror sprint4 1:1.

This test enumerates the cartesian product of edge cases that distinguish
LENIENT from STRICT (missing signal, missing ticker, neutral, None confidence,
side aliases) and asserts the two filters return identical include/reject
decisions for EVERY case.

If this test fails, walk-forward Sharpe is no longer a valid prediction of
prod behavior — investigate the divergence before deploying.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest

# Make sprint4 imports work from the test
PROJECT_ROOT = Path(__file__).resolve().parents[3]
for _p in (PROJECT_ROOT / "sprint4" / "exits",
           PROJECT_ROOT / "sprint4" / "exits" / "hybrid"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from llm_signal_lookup import LLMSignal, TickerSignal  # noqa: E402
from trade_filter import DirectionFilter  # noqa: E402

from src.services.decision.filter import apply_direction_filter  # noqa: E402


# ---------------------------------------------------------------------------
# Prod-filter adapter — mirrors scripts/walk_forward_b_filter.py::ProdFilterAdapter
# kept inline here so the test is self-contained and runs without importing
# the walk-forward module.
# ---------------------------------------------------------------------------
_SIDE_NORMALIZE = {
    "long": "BUY", "BUY": "BUY", "buy": "BUY",
    "short": "SELL", "SELL": "SELL", "sell": "SELL",
}


def _prod_include(trade_side: str, trade_ticker: str,
                  signal: Optional[LLMSignal],
                  min_confidence: float = 0.5) -> bool:
    prod_side = _SIDE_NORMALIZE.get(trade_side)
    if prod_side is None:
        return False
    if signal is None:
        fake_event = SimpleNamespace(payload=SimpleNamespace(tickers=[]))
    else:
        tickers_shim = [
            SimpleNamespace(
                ticker=ts.ticker,
                direction=ts.direction if ts.direction is not None else "neutral",
                confidence=ts.confidence if ts.confidence is not None else 0.0,
            )
            for ts in signal.tickers
        ]
        fake_event = SimpleNamespace(payload=SimpleNamespace(tickers=tickers_shim))
    return bool(apply_direction_filter(
        fake_event, trade_ticker, prod_side, min_confidence,
    ).include)


def _sprint4_include(trade_side: str, trade_ticker: str,
                     signal: Optional[LLMSignal],
                     min_confidence: float = 0.5) -> bool:
    return DirectionFilter(min_confidence=min_confidence).include(
        trade_side, trade_ticker, signal,
    )


# ---------------------------------------------------------------------------
# Test case generator — covers every interesting branch
# ---------------------------------------------------------------------------
_SIDES = ["BUY", "SELL", "buy", "sell"]
_TICKERS = ["GAZP", "LKOH", "MIX"]   # GAZP — in signal, LKOH — not, MIX — sometimes
_DIRECTIONS = ["long", "short", "neutral", None]
_CONFIDENCES = [None, 0.0, 0.49, 0.5, 0.7]


def _build_signal(ticker_dirs_confs: list[tuple[str, Optional[str], Optional[float]]]) -> LLMSignal:
    """Build an LLMSignal with the given per-ticker entries."""
    tickers = tuple(
        TickerSignal(ticker=t, direction=d, confidence=c,
                     impact_strength=0.5, sentiment="neutral")
        for t, d, c in ticker_dirs_confs
    )
    return LLMSignal(
        news_id="test_news",
        is_financial=True,
        category="corporate",
        urgency="medium",
        is_actionable=True,
        expected_timeframe="hours",
        top_ticker=ticker_dirs_confs[0][0] if ticker_dirs_confs else None,
        direction=ticker_dirs_confs[0][1] if ticker_dirs_confs else None,
        confidence=ticker_dirs_confs[0][2] if ticker_dirs_confs else None,
        impact_strength=0.5,
        sentiment="neutral",
        sell_the_news_flag=False,
        tickers=tickers,
    )


def _gen_cases():
    """Generate every (signal, ticker, side, min_confidence) case worth checking."""
    cases = []

    # Case family 1: signal is None — both must INCLUDE.
    for side in _SIDES:
        for tk in _TICKERS:
            cases.append(("signal_none", None, tk, side, 0.5))

    # Case family 2: signal is empty (no tickers at all).
    empty_sig = _build_signal([])
    for side in _SIDES:
        for tk in _TICKERS:
            cases.append(("empty_tickers", empty_sig, tk, side, 0.5))

    # Case family 3: signal mentions GAZP only.
    for direc in _DIRECTIONS:
        for conf in _CONFIDENCES:
            sig = _build_signal([("GAZP", direc, conf)])
            label = f"gazp_{direc}_{conf}"
            for side in _SIDES:
                # GAZP is mentioned — test direction match & confidence
                cases.append((label + "_for_GAZP", sig, "GAZP", side, 0.5))
                # LKOH not mentioned — LENIENT path
                cases.append((label + "_for_LKOH", sig, "LKOH", side, 0.5))

    # Case family 4: signal mentions multiple tickers.
    multi_sig = _build_signal([
        ("GAZP", "long", 0.7),
        ("MIX", "short", 0.6),
        ("LKOH", "neutral", 0.9),
    ])
    for side in _SIDES:
        for tk in ["GAZP", "LKOH", "MIX", "SBER"]:  # SBER not present
            cases.append((f"multi_for_{tk}", multi_sig, tk, side, 0.5))

    # Case family 5: confidence threshold sweep.
    for thr in [0.4, 0.5, 0.6, 0.8]:
        sig = _build_signal([("GAZP", "long", 0.55)])
        cases.append((f"thr_{thr}", sig, "GAZP", "BUY", thr))

    return cases


_CASES = _gen_cases()


@pytest.mark.parametrize(
    "label,signal,ticker,side,min_conf",
    _CASES,
    ids=[c[0] for c in _CASES],
)
def test_prod_filter_matches_sprint4_reference(label, signal, ticker, side, min_conf):
    """Every (signal, ticker, side, threshold) case: prod include == sprint4 include."""
    prod = _prod_include(side, ticker, signal, min_conf)
    ref = _sprint4_include(side, ticker, signal, min_conf)
    assert prod == ref, (
        f"DIVERGENCE [{label}] side={side} ticker={ticker} "
        f"min_conf={min_conf}: prod={prod} != sprint4_ref={ref}. "
        f"Walk-forward Sharpe 6.42 no longer predicts prod behavior."
    )


def test_case_count_sanity():
    """Smoke check that the generator produces enough cases to be meaningful."""
    assert len(_CASES) >= 100, f"only {len(_CASES)} cases — generator looks broken"


def test_includes_a_known_divergent_case_for_strict():
    """Sanity: confirm the dataset contains a case where STRICT semantics
    would diverge from LENIENT — otherwise this test wouldn't detect a
    regression to STRICT.
    """
    # No-signal case: LENIENT says INCLUDE, STRICT says REJECT.
    found_no_signal = any(c[1] is None for c in _CASES)
    # Neutral-direction case: LENIENT says INCLUDE, STRICT says REJECT.
    found_neutral = False
    for _, sig, tk, _, _ in _CASES:
        if sig is None:
            continue
        ts = sig.get_for_ticker(tk)
        if ts is not None and ts.direction == "neutral":
            found_neutral = True
            break
    assert found_no_signal, "case generator missing signal=None family"
    assert found_neutral, "case generator missing neutral-direction family"
