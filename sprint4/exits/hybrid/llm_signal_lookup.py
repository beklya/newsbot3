"""
sprint4/exits/hybrid/llm_signal_lookup.py — accessor для LLM enrichment per anchor news_id.

Используется 4.9/4.10 backtest для подачи в TradeFilter/SizeAdjuster/DynamicHorizon.

Использование:
    lookup = LLMSignalLookup.from_parquet("path/to/c1_8b_v1_0_0.parquet")
    sig = lookup.get(news_id="abc123def456")
    if sig is None:
        # no enrichment — skip or treat as neutral
    elif sig.confidence >= 0.5 and sig.direction == "long":
        ...
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import polars as pl


@dataclass(frozen=True)
class TickerSignal:
    """Per-(news, ticker) предсказание из LLM."""
    ticker: str
    direction: str | None       # "long" | "short" | "neutral"
    confidence: float | None
    impact_strength: float | None
    sentiment: str | None


@dataclass(frozen=True)
class LLMSignal:
    """Per-news enrichment signal — flattened.

    Использование:
      sig = lookup.get(news_id="abc...")
      # Top-ticker view (default — обратная совместимость)
      sig.direction, sig.confidence  # для top_ticker по impact_strength
      # Per-ticker view (Phase 2 trade имеет специфический ticker)
      ts = sig.get_for_ticker("MX")  # → TickerSignal | None
    """
    news_id: str
    is_financial: bool | None
    category: str | None
    urgency: str | None
    is_actionable: bool | None
    expected_timeframe: str | None
    # Top-1 ticker info (по impact_strength desc) — для downstream summary
    top_ticker: str | None
    direction: str | None       # "long" | "short" | "neutral" | None — top ticker
    confidence: float | None    # 0..1 — top ticker
    impact_strength: float | None
    sentiment: str | None
    sell_the_news_flag: bool    # sentiment ≠ direction polarity
    # Полный список tickers — позволяет TradeFilter лукапить per Phase 2 ticker
    tickers: tuple[TickerSignal, ...] = ()

    def get_for_ticker(self, ticker: str) -> Optional[TickerSignal]:
        """Lookup signal для конкретного ticker. Сравнение exact (canonical names).

        Phase 2 trade ticker — canonical (SBER, MIX, SI, ...).
        Legacy LLM names (Si, MX, YNDX, GOLD) тоже учитываем через registry-normalize.
        """
        # Lazy import чтобы не тянуть src при импорте модуля
        from src.contracts.instruments import try_normalize_ticker
        target = try_normalize_ticker(ticker) or ticker
        for ts in self.tickers:
            stored = try_normalize_ticker(ts.ticker) or ts.ticker
            if stored == target:
                return ts
        return None


class LLMSignalLookup:
    """Indexed-by-event_id accessor для enrichment результатов."""

    def __init__(self, signals: dict[str, LLMSignal]):
        self._signals = signals

    @property
    def size(self) -> int:
        return len(self._signals)

    def get(self, news_id: str) -> Optional[LLMSignal]:
        return self._signals.get(news_id)

    def has(self, news_id: str) -> bool:
        return news_id in self._signals

    @classmethod
    def from_parquet(cls, path: Path) -> "LLMSignalLookup":
        df = pl.read_parquet(str(path))
        df = df.filter(pl.col("is_enriched") & pl.col("enrich_error").is_null())

        # Extract top-1 ticker from tickers list (by impact_strength)
        def _top_ticker_row(row: dict) -> dict:
            tickers = row.get("tickers") or []
            if not tickers:
                return {
                    "top_ticker": None, "direction": None, "confidence": None,
                    "impact_strength": None, "sentiment": None, "sell_the_news_flag": False,
                }
            # Sort by impact_strength desc, fallback to confidence
            best = max(
                tickers,
                key=lambda t: (
                    (t.get("impact_strength") or 0.0),
                    (t.get("confidence") or 0.0),
                ),
            )
            sent = best.get("sentiment")
            direc = best.get("direction")
            sell_flag = (
                (sent == "positive" and direc == "short")
                or (sent == "negative" and direc == "long")
            )
            return {
                "top_ticker": best.get("ticker"),
                "direction": direc,
                "confidence": float(best.get("confidence")) if best.get("confidence") is not None else None,
                "impact_strength": float(best.get("impact_strength")) if best.get("impact_strength") is not None else None,
                "sentiment": sent,
                "sell_the_news_flag": sell_flag,
            }

        sig_map: dict[str, LLMSignal] = {}
        for row in df.iter_rows(named=True):
            top = _top_ticker_row(row)
            # Build per-ticker map (для get_for_ticker())
            ticker_list = row.get("tickers") or []
            per_ticker_tuple: tuple[TickerSignal, ...] = tuple(
                TickerSignal(
                    ticker=t.get("ticker") or "",
                    direction=t.get("direction"),
                    confidence=float(t.get("confidence")) if t.get("confidence") is not None else None,
                    impact_strength=float(t.get("impact_strength")) if t.get("impact_strength") is not None else None,
                    sentiment=t.get("sentiment"),
                )
                for t in ticker_list
                if isinstance(t, dict) and t.get("ticker")
            )
            sig_map[row["id"]] = LLMSignal(
                news_id=row["id"],
                is_financial=row.get("is_financial"),
                category=row.get("category"),
                urgency=row.get("urgency"),
                is_actionable=row.get("is_actionable"),
                expected_timeframe=row.get("expected_timeframe"),
                top_ticker=top["top_ticker"],
                direction=top["direction"],
                confidence=top["confidence"],
                impact_strength=top["impact_strength"],
                sentiment=top["sentiment"],
                sell_the_news_flag=top["sell_the_news_flag"],
                tickers=per_ticker_tuple,
            )
        return cls(sig_map)
