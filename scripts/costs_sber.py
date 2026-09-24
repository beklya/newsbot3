"""Sber «Самостоятельный» cost model — single source of truth for backtests.

v1 (Sprint 6.3): flat per-class ROUND_TRIP_COST_PCT — kept below for backward
compatibility / reproduction of SPRINT_6_3_DONE numbers.

v2 (Sprint 6.4 Phase 0, 2026-06-11): full tariff (tariff_self.pdf, 14.07.2025,
extracted to data/walk_forward/tariff_self_text.txt) + MOEX fees + Phase 2
slippage + overnight funding.  Key corrections vs v1:

  1. Stock brokerage is TIERED on the DAY's total turnover (п.1.1,
     internet-trading): ≤1M 0.060% / ≤50M 0.035% / >50M 0.018% per leg.
     One round-trip with our typical notional (600–900k) already puts the day
     into the 0.035% tier → brokerage must be computed per DAY, not per trade.
  2. GLDRUB is a precious metal (п.2.2): 0.6% per leg = 1.2% RT — NOT the
     0.40% currency rate v1 assumed.  Structurally dead.
  3. Overnight funding (Sber margin rates, user-confirmed 2026-06-11):
     long borrow 20%/год (0.055%/день, ≤10M), short 13%/год (0.035%/день),
     + 0.0045% per transfer.  Longs on own funds carry for free; shorts always
     pay (borrowed securities).
  4. Slippage per-LEG by execution type (v2.1): TP exit = limit → ×0,
     SL exit = stop-market → ×2, entry/time = market → ×1.
  5. Phone orders are more expensive (0.30% ≤250k) — we only use internet
     trading (бесплатно, п.8.2).  Депозитарка/счёт — бесплатно (п.2.1 доп.услуг).

Architecture note for harnesses: simulate trades GROSS (cost_rub=0), store
notional + entry/exit dates + side, then apply this model VECTORIZED at
aggregation time (brokerage needs the day's selected-trade turnover, which
depends on the sweep combo).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------
STOCK_TICKERS = [
    "SBER", "GAZP", "LKOH", "YDEX", "ROSN", "GMKN",
    "NVTK", "TATN", "MGNT", "MTSS", "PLZL", "VTBR",
]
FUTURES_TICKERS = ["SI", "MIX", "BR", "NG"]
CURRENCY_TICKERS = ["USDRUB", "CNY", "GLDRUB"]  # v1 grouping (GLDRUB → metal in v2)

ASSET_CLASS: dict[str, str] = {
    **{t: "stock" for t in STOCK_TICKERS},
    **{t: "futures" for t in FUTURES_TICKERS},
    "USDRUB": "currency", "CNY": "currency",
    "GLDRUB": "metal",  # п.2.2 драгметаллы — 0.6% per leg
}

# Whitelist presets for sweeps
WHITELIST_STRATEGIES: dict[str, frozenset[str]] = {
    "full": frozenset(STOCK_TICKERS + FUTURES_TICKERS + CURRENCY_TICKERS),   # 19
    "stocks_only": frozenset(STOCK_TICKERS),                                  # 12
    "stocks_futures": frozenset(STOCK_TICKERS + FUTURES_TICKERS),             # 16
}

# ---------------------------------------------------------------------------
# v2 — full tariff
# ---------------------------------------------------------------------------
# Brokerage per LEG as fraction of leg notional, tiered by the DAY's total
# turnover in RUB (cumulative across all trades that day; the reached tier's
# rate applies to the whole day's turnover — standard Sber interpretation).
STOCK_BROKERAGE_TIERS: list[tuple[float, float]] = [   # (turnover ≤, rate/leg)
    (1_000_000.0, 0.00060),
    (50_000_000.0, 0.00035),
    (math.inf, 0.00018),
]
CURRENCY_BROKERAGE_TIERS: list[tuple[float, float]] = [  # п.2.1
    (100_000_000.0, 0.0020),
    (math.inf, 0.0002),
]
FUTURES_BROKERAGE_LEG = 0.00015   # п.3: 0.015% от стоимости контракта
METAL_BROKERAGE_LEG = 0.0060      # п.2.2: 0.6% (!) — GLDRUB
FORCED_CLOSE_PER_CONTRACT_RUB = 10.0  # п.3 принудительное закрытие (не используем)

# MOEX exchange fees per LEG (not in the Sber tariff; added on top).
MOEX_FEE_LEG: dict[str, float] = {
    "stock": 0.0001,       # ~0.01% TQBR taker
    "currency": 0.000015,  # USDRUB ~0.0015%; CNY биржевая не взимается — упрощённо same
    "metal": 0.0,          # «Комиссия бирж по сделкам с драгметаллами не взимается»
    "futures": 0.00001,    # FORTS фикс ₽/контракт ≈ ~0.001% от стоимости — грубая оценка
}

# Slippage — PER-LEG model by execution type (v2.1, 2026-06-11 user review):
# the old flat RT% over-charged TP exits (limit order at the level → maker,
# ~zero slippage) and under-charged SL exits (stop-market through the level).
# Base = half-spread+impact per leg (Phase 2 empirical RT / 2).
SLIPPAGE_LEG_BASE: dict[str, float] = {
    "stock": 0.00025, "currency": 0.0005, "metal": 0.0005, "futures": 0.00025,
}
# Exit-leg multiplier by exit reason; entry leg always ×1 (market at next-min open).
SLIPPAGE_EXIT_MULT: dict[str, float] = {
    "tp": 0.0,    # limit at the level — fills when touched, maker
    "sl": 2.0,    # stop-market punches through the level
    "time": 1.0,  # market order
    "kill": 1.0,
}

# Overnight funding of UNCOVERED positions (Sber margin rates, user-confirmed
# 2026-06-11): long borrow 20%/год ≤10M (19.5% above), short borrow 13%/год,
# plus 0.0045% per transfer (сделка переноса).
FUNDING_LONG_ANNUAL = 0.20       # 0.055%/день, заём ≤10M на покупку
FUNDING_LONG_ANNUAL_OVER_10M = 0.195
FUNDING_SHORT_ANNUAL = 0.13      # 0.035%/день, заём бумаг для шорта
SPECREPO_FEE_PER_TRANSFER = 0.000045  # 0.0045% за сделку переноса, per night


@dataclass(frozen=True)
class CostBreakdown:
    brokerage_rub: float
    moex_rub: float
    slippage_rub: float
    funding_rub: float

    @property
    def total_rub(self) -> float:
        return self.brokerage_rub + self.moex_rub + self.slippage_rub + self.funding_rub


def brokerage_leg_rate(asset_class: str, day_turnover_rub: float) -> float:
    """Per-leg brokerage rate given the day's TOTAL turnover (both legs of all trades)."""
    if asset_class == "futures":
        return FUTURES_BROKERAGE_LEG
    if asset_class == "metal":
        return METAL_BROKERAGE_LEG
    tiers = STOCK_BROKERAGE_TIERS if asset_class == "stock" else CURRENCY_BROKERAGE_TIERS
    for cap, rate in tiers:
        if day_turnover_rub <= cap:
            return rate
    return tiers[-1][1]


def slippage_rt_cost_rub(ticker: str, notional_rub: float,
                         exit_reason: str = "time") -> float:
    """Entry leg (market) + exit leg weighted by execution type."""
    cls = ASSET_CLASS.get(ticker, "stock")
    base = SLIPPAGE_LEG_BASE[cls]
    mult = SLIPPAGE_EXIT_MULT.get(exit_reason, 1.0)
    return notional_rub * base * (1.0 + mult)


def funding_rt_cost_rub(ticker: str, notional_rub: float, holding_nights: int,
                        side: str, borrowed_share_long: float = 0.0) -> float:
    """Overnight carry. Shorts: borrowed securities → full notional pays 13%/год.
    Longs: only the borrowed share pays 20%/год (0.0 = own funds)."""
    if holding_nights <= 0:
        return 0.0
    if side == "SELL":
        share, annual = 1.0, FUNDING_SHORT_ANNUAL
    else:
        share = borrowed_share_long
        annual = (FUNDING_LONG_ANNUAL if notional_rub * share <= 10_000_000
                  else FUNDING_LONG_ANNUAL_OVER_10M)
    if share <= 0.0:
        return 0.0
    per_night = annual / 365.0 + SPECREPO_FEE_PER_TRANSFER
    return notional_rub * share * per_night * holding_nights


def round_trip_cost_rub(ticker: str, notional_rub: float, *,
                        day_turnover_rub: float | None = None,
                        holding_nights: int = 0, side: str = "BUY",
                        borrowed_share_long: float = 0.0,
                        exit_reason: str = "time") -> CostBreakdown:
    """Full RT cost of one trade.  `day_turnover_rub` = the day's TOTAL turnover
    (defaults to this trade's own 2×notional — i.e. the only trade that day)."""
    cls = ASSET_CLASS.get(ticker, "stock")
    turnover = day_turnover_rub if day_turnover_rub is not None else 2 * notional_rub
    brok = 2 * notional_rub * brokerage_leg_rate(cls, turnover)
    moex = 2 * notional_rub * MOEX_FEE_LEG[cls]
    slip = slippage_rt_cost_rub(ticker, notional_rub, exit_reason)
    fund = funding_rt_cost_rub(ticker, notional_rub, holding_nights, side,
                               borrowed_share_long)
    return CostBreakdown(brok, moex, slip, fund)


def apply_costs_v2(trades, equity_rub: float = 500_000.0):
    """Vectorized v2 costs over a SELECTED trade set (pandas DataFrame).

    Required columns: ticker, side, notional_rub, exit_reason, entry_ts,
    ts_close, gross_pnl.  Returns a copy with cost component columns and
    net_pnl = gross_pnl − total cost.

    Brokerage is tiered on each DAY's total turnover of the selected set,
    per market section (stocks TQBR and currencies CETS tiered; futures and
    metals flat) — so it must be computed on the final selection, never
    per-trade in isolation.  Both legs land on their own dates (T+1 exits
    contribute to the exit day's turnover).
    """
    import numpy as np
    import pandas as pd

    t = trades.copy()
    cls = t["ticker"].map(ASSET_CLASS).fillna("stock")
    n = t["notional_rub"].astype(float)
    t["entry_date"] = pd.to_datetime(t["entry_ts"]).dt.normalize()
    t["exit_date"] = pd.to_datetime(t["ts_close"], errors="coerce").dt.normalize()
    t["holding_nights"] = (
        (t["exit_date"] - t["entry_date"]).dt.days.fillna(0).astype(int).clip(lower=0)
    )

    t["cost_moex_rub"] = 2 * n * cls.map(MOEX_FEE_LEG).astype(float)
    mult = t["exit_reason"].map(SLIPPAGE_EXIT_MULT).fillna(1.0)
    t["cost_slippage_rub"] = n * cls.map(SLIPPAGE_LEG_BASE).astype(float) * (1.0 + mult)

    is_short = t["side"].eq("SELL").values
    borrowed_share = np.where(is_short, 1.0,
                              np.maximum(0.0, n.values - equity_rub) / n.values)
    annual = np.where(is_short, FUNDING_SHORT_ANNUAL, FUNDING_LONG_ANNUAL)
    # Futures are margin instruments — no money/securities borrowing either
    # side, variation margin settles daily → no overnight funding cost.
    is_margin_instr = (cls == "futures").values
    t["cost_funding_rub"] = np.where(
        is_margin_instr, 0.0,
        n.values * borrowed_share
        * (annual / 365.0 + SPECREPO_FEE_PER_TRANSFER)
        * t["holding_nights"].values)

    # --- brokerage: leg-level day×section turnover, tier on the whole day ---
    legs = pd.concat([
        pd.DataFrame({"i": t.index, "date": t["entry_date"].values,
                      "cls": cls.values, "n": n.values}),
        pd.DataFrame({"i": t.index, "date": t["exit_date"].values,
                      "cls": cls.values, "n": n.values}),
    ], ignore_index=True)
    turn = legs.groupby(["date", "cls"])["n"].transform("sum").values
    cls_v = legs["cls"].values
    # NB: keep in sync with *_BROKERAGE_TIERS / *_BROKERAGE_LEG above
    rate = np.where(
        cls_v == "futures", FUTURES_BROKERAGE_LEG,
        np.where(
            cls_v == "metal", METAL_BROKERAGE_LEG,
            np.where(
                cls_v == "currency",
                np.where(turn <= 100_000_000.0, 0.0020, 0.0002),
                np.where(turn <= 1_000_000.0, 0.00060,
                         np.where(turn <= 50_000_000.0, 0.00035, 0.00018)),
            ),
        ),
    )
    legs["fee"] = legs["n"] * rate
    t["cost_brokerage_rub"] = (
        legs.groupby("i")["fee"].sum().reindex(t.index).fillna(0.0)
    )

    t["cost_total_rub"] = (t["cost_brokerage_rub"] + t["cost_moex_rub"]
                           + t["cost_slippage_rub"] + t["cost_funding_rub"])
    t["net_pnl"] = t["gross_pnl"] - t["cost_total_rub"]
    return t


# ---------------------------------------------------------------------------
# v1 legacy API (Sprint 6.3 reproduction) — DO NOT use for new work
# ---------------------------------------------------------------------------
ROUND_TRIP_COST_PCT: dict[str, float] = {
    **{t: 0.0019 for t in STOCK_TICKERS},     # стоки ~0.19% RT (v1: 0.060%/leg tier)
    **{t: 0.0050 for t in CURRENCY_TICKERS},  # валюты ~0.50% RT (v1: GLDRUB как валюта)
    **{t: 0.0008 for t in FUTURES_TICKERS},   # фьючерсы ~0.08% RT
}
DEFAULT_RT_COST_PCT = 0.002


def rt_cost_pct(ticker: str) -> float:
    """v1 legacy: flat round-trip cost as fraction of notional."""
    return ROUND_TRIP_COST_PCT.get(ticker, DEFAULT_RT_COST_PCT)
