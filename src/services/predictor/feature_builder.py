"""Port of Phase 2 features_mfe.py extract_features() for live inference.

Ключевые отличия от Phase 2:
1. Inputs — EnrichedNewsEvent + TickerImpact (вместо legacy news_pool JSON rec).
2. Sentiment mapping: positive→bullish, negative→bearish, neutral→neutral
   (Sprint 3+ EnrichedNewsEvent использует positive/negative/neutral;
   Phase 2 trained on bullish/bearish/neutral).
3. Category one-hot: 8 значений из новой схемы покрывают 7 из 12 Phase 2
   значений (geopolitics, macro, cbr, corporate, commodity, currency, other).
   Sprint 3+ "market" не имеет эквивалента в Phase 2 — все cat_* зануляются
   для "market". Это известный schema drift, документирован в SPRINT4.
4. Text features (text_length/n_numbers/n_percent/has_quotes) считаются от
   EnrichedNewsPayload.summary (max 300 chars). Phase 2 считал от full_text
   (~252 median chars). Распределение смещено вниз, но features составляют
   только 5/67 = 7.4% веса — приемлемая proxy для MVP.
   Sprint 6 backlog: lookup raw_event_id → full text.
5. price_driven, causal — отсутствуют в новой схеме → константа 0.0.

Output: dict[str, float] совпадает с feature_order.json по ключам.
Конвертация в np.ndarray делается в inference.py.
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import datetime
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

from src.contracts.enriched_news import EnrichedNewsEvent, TickerImpact

from .candle_cache import CandleCache
from .news_history import NewsHistory

log = logging.getLogger(__name__)


# === Phase 2 feature axes (matches features_mfe.py) ===
ALL_CATEGORIES = [
    "geopolitics", "macro", "cbr", "corporate", "commodity",
    "currency", "sanctions", "earnings", "dividends", "ma",
    "regulation", "other",
]
ALL_URGENCIES = ["high", "medium", "low"]
ALL_SENTIMENTS = ["bullish", "bearish", "neutral"]

# === Schema mapping new EnrichedNews → Phase 2 ===
_SENTIMENT_MAP = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}


# ============================================================
# Technical indicators (verbatim port of features_mfe.py)
# ============================================================

def _return(candles: Optional[pd.DataFrame], ts: pd.Timestamp, minutes: int) -> Optional[float]:
    if candles is None or len(candles) == 0:
        return None
    ts_now = ts.floor("min")
    ts_prev = ts_now - pd.Timedelta(minutes=minutes)
    idx_now = candles.index.searchsorted(ts_now)
    idx_prev = candles.index.searchsorted(ts_prev)
    if idx_now >= len(candles) or idx_prev >= len(candles) or idx_now == 0 or idx_prev == 0:
        return None
    p_now = candles["close"].iat[idx_now - 1]
    p_prev = candles["close"].iat[idx_prev - 1]
    if p_prev <= 0:
        return None
    return float((p_now / p_prev - 1) * 100)


def _atr(candles: pd.DataFrame, ts: pd.Timestamp, window_min: int) -> Optional[float]:
    end_ts = ts.floor("min")
    start_ts = end_ts - pd.Timedelta(minutes=window_min)
    w = candles.loc[start_ts:end_ts]
    if len(w) < 3:
        return None
    tr = (w["high"] - w["low"]).mean()
    p_avg = w["close"].mean()
    if p_avg <= 0:
        return None
    return float(tr / p_avg * 100)


def _rsi(candles: pd.DataFrame, ts: pd.Timestamp, period: int) -> float:
    end_ts = ts.floor("min")
    start_ts = end_ts - pd.Timedelta(minutes=period * 3)
    w = candles.loc[start_ts:end_ts]["close"]
    if len(w) < period + 1:
        return 50.0
    delta = w.diff().dropna()
    gain = delta.where(delta > 0, 0).rolling(period).mean().iloc[-1]
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean().iloc[-1]
    if loss == 0 or pd.isna(loss):
        return 100.0 if gain > 0 else 50.0
    rs = gain / loss
    return float(100 - 100 / (1 + rs))


def _bollinger_position(candles: pd.DataFrame, ts: pd.Timestamp, period: int = 20) -> float:
    end_ts = ts.floor("min")
    start_ts = end_ts - pd.Timedelta(minutes=period * 5)
    w = candles.loc[start_ts:end_ts]["close"]
    if len(w) < period:
        return 0.5
    ma = w.rolling(period).mean().iloc[-1]
    std = w.rolling(period).std().iloc[-1]
    if pd.isna(ma) or pd.isna(std) or std == 0:
        return 0.5
    upper = ma + 2 * std
    lower = ma - 2 * std
    cur = w.iloc[-1]
    pos = (cur - lower) / (upper - lower)
    return float(np.clip(pos, 0, 1))


def _distance_to_extremes(candles: pd.DataFrame, ts: pd.Timestamp, days: int = 5) -> Dict[str, float]:
    end_ts = ts.floor("min")
    start_ts = end_ts - pd.Timedelta(days=days)
    w = candles.loc[start_ts:end_ts]
    if len(w) == 0:
        return {"dist_to_high_5d": 0.0, "dist_to_low_5d": 0.0}
    cur = w["close"].iloc[-1]
    high = w["high"].max()
    low = w["low"].min()
    if cur <= 0:
        return {"dist_to_high_5d": 0.0, "dist_to_low_5d": 0.0}
    return {
        "dist_to_high_5d": float((high - cur) / cur * 100),
        "dist_to_low_5d": float((cur - low) / cur * 100),
    }


def _volume_intensity(candles: pd.DataFrame, ts: pd.Timestamp, window: int = 5) -> float:
    end_ts = ts.floor("min")
    start_ts = end_ts - pd.Timedelta(minutes=window)
    recent = candles.loc[start_ts:end_ts, "volume"].sum()
    if recent <= 0:
        return 1.0
    long_start = end_ts - pd.Timedelta(days=20)
    long_w = candles.loc[long_start:end_ts, "volume"]
    if len(long_w) == 0:
        return 1.0
    avg_per_min = long_w.sum() / len(long_w)
    if avg_per_min <= 0:
        return 1.0
    expected = avg_per_min * window
    return float(recent / expected)


# ============================================================
# Cross-asset shortcuts.
# Phase 2 features_mfe.py использует ticker строки {"BR","USDRUB","MX","GOLD"}.
# Эти строки — Phase 2 legacy. Маппим в canonical имена для CandleCache.
# ============================================================
_CROSS_ASSET_PHASE2_TO_CANONICAL = {"BR": "BR", "USDRUB": "USDRUB", "MX": "MIX", "GOLD": "GLDRUB"}


# ============================================================
# Main feature extraction
# ============================================================

def build_features(
    event: EnrichedNewsEvent,
    ticker_impact: TickerImpact,
    candles: CandleCache,
    history: NewsHistory,
) -> Dict[str, float]:
    """Build 67-feature vector for (event, single ticker).

    Returns ordered dict (Python 3.7+ preserves insertion order). Order
    matches Phase 2 features_mfe.py extract_features() — sanity-checked
    by inference.py против feature_order.json.

    Raises:
        ValueError if event.produced_at не parseable (defensive — should
        never happen with valid pydantic envelope).
    """
    a = event.payload  # alias
    ticker = ticker_impact.ticker  # canonical (validator уже нормализовал)

    # Reference time for feature window. Sprint 6: prefer payload.tg_published_at
    # (original Telegram time) if set — needed for honest historical replay.
    # Fallback на envelope.produced_at для backward compat со старыми events.
    # Phase 2 candle data в naive MSK. Конвертируем: take UTC, +3h, strip tz.
    ref_ts_str = a.tg_published_at or event.produced_at
    ts_utc = pd.to_datetime(ref_ts_str)
    if ts_utc.tzinfo is not None:
        ts = (ts_utc.tz_convert("UTC") + pd.Timedelta(hours=3)).tz_localize(None)
    else:
        ts = ts_utc

    f: Dict[str, float] = {}

    # === A. LLM features (12 dims) ===
    sentiment_legacy = _SENTIMENT_MAP.get(ticker_impact.sentiment, "neutral")
    for s in ALL_SENTIMENTS:
        f[f"sent_{s}"] = 1.0 if sentiment_legacy == s else 0.0

    f["confidence"] = float(ticker_impact.confidence)  # уже 0..1, как Phase 2 после /100

    urg = a.urgency
    for u in ALL_URGENCIES:
        f[f"urg_{u}"] = 1.0 if urg == u else 0.0

    cat = a.category  # 8 values; sanctions/earnings/dividends/ma/regulation/market не входят в Phase 2 → 0
    for c in ALL_CATEGORIES:
        f[f"cat_{c}"] = 1.0 if cat == c else 0.0

    # price_driven / causal — нет в новой схеме. is_actionable частично перекрывает.
    f["price_driven"] = 0.0
    f["causal"] = 1.0 if a.is_actionable else 0.0
    f["n_tickers_aff"] = float(len(a.tickers))

    # === A1. Sprint 6.1 Y4 — 10 new 70B-only LLM features ===
    # Phase 2 corpus (Ollama 8B v2 prompt) did NOT emit these fields.  Current
    # 70B prompt v1.0.0 does.  Adding them gives XGBoost signal that the
    # legacy training distribution physically could not represent — which is
    # the structural fix for distribution shift documented in
    # docs/B_FILTER_ARCHITECTURE.md.
    #
    # Train data: rebuild_features_with_70b.py emits these from full_70k_70b.parquet.
    # Inference: this section.  feature_order.json controls vectorize order.
    f["is_actionable_int"] = 1.0 if a.is_actionable else 0.0
    f["is_financial_int"] = 1.0 if a.is_financial else 0.0
    tf = (a.expected_timeframe or "medium").lower()
    for x in ("instant", "fast", "medium", "slow"):
        f[f"tf_{x}"] = 1.0 if tf == x else 0.0
    f["impact_strength"] = float(ticker_impact.impact_strength or 0.0)
    direction = (ticker_impact.direction or "neutral").lower()
    for d in ("long", "short", "neutral"):
        f[f"dir_{d}"] = 1.0 if direction == d else 0.0
    sent = (ticker_impact.sentiment or "neutral").lower()
    sell_flag = (sent == "positive" and direction == "short") or \
                (sent == "negative" and direction == "long")
    f["sell_the_news"] = 1.0 if sell_flag else 0.0

    # === B. Text features (5 dims) — proxy from summary ===
    text = a.summary or ""
    f["text_length"] = float(len(text))
    f["headline_length"] = 0.0  # нет headline в EnrichedNewsPayload
    f["n_numbers"] = float(sum(1 for ch in text if ch.isdigit()))
    f["n_percent"] = float(text.count("%"))
    f["has_quotes"] = 1.0 if ('"' in text or "«" in text) else 0.0

    # === C. Market context — на тикере (25 dims) ===
    c_main = candles.get(ticker)
    if c_main is not None and len(c_main) > 0:
        for mins in [5, 15, 30, 60, 240]:
            ret = _return(c_main, ts, mins) or 0.0
            f[f"ret_{mins}m_pre"] = ret
            f[f"abs_ret_{mins}m_pre"] = abs(ret)
        for mins in [5, 15, 30, 60, 240]:
            f[f"atr_{mins}m_pct"] = _atr(c_main, ts, mins) or 0.0

        atr_15 = f["atr_15m_pct"]
        atr_240 = f["atr_240m_pct"]
        f["atr_ratio_15_240"] = (atr_15 / atr_240) if atr_240 > 0 else 1.0

        f["rsi_14"] = _rsi(c_main, ts, 14)
        f["rsi_30"] = _rsi(c_main, ts, 30)
        f["bb_position"] = _bollinger_position(c_main, ts, 20)
        f.update(_distance_to_extremes(c_main, ts, days=5))
        f["vol_5m_intensity"] = _volume_intensity(c_main, ts, 5)
        f["vol_15m_intensity"] = _volume_intensity(c_main, ts, 15)
    else:
        # Тикера нет в свечах — заполняем neutral defaults как Phase 2
        for mins in [5, 15, 30, 60, 240]:
            f[f"ret_{mins}m_pre"] = 0.0
            f[f"abs_ret_{mins}m_pre"] = 0.0
            f[f"atr_{mins}m_pct"] = 0.0
        f["atr_ratio_15_240"] = 1.0
        f["rsi_14"] = 50.0
        f["rsi_30"] = 50.0
        f["bb_position"] = 0.5
        f["dist_to_high_5d"] = 0.0
        f["dist_to_low_5d"] = 0.0
        f["vol_5m_intensity"] = 1.0
        f["vol_15m_intensity"] = 1.0

    # === D. Cross-asset (7 dims) ===
    for label, mins in (("BR_15m", 15), ("BR_60m", 60),
                        ("USDRUB_15m", 15), ("USDRUB_60m", 60),
                        ("MX_15m", 15), ("MX_60m", 60),
                        ("GOLD_60m", 60)):
        phase2_ticker = label.split("_")[0]
        canonical = _CROSS_ASSET_PHASE2_TO_CANONICAL[phase2_ticker]
        f[f"ret_{label}"] = _return(candles.get(canonical), ts, mins) or 0.0

    # === E. Temporal (7 dims) ===
    f["hour"] = float(ts.hour)
    f["dow"] = float(ts.weekday())
    f["minute_of_day"] = float(ts.hour * 60 + ts.minute)
    f["is_morning"] = 1.0 if 10 <= ts.hour < 12 else 0.0
    f["is_evening"] = 1.0 if 19 <= ts.hour < 24 else 0.0
    f["is_first_30m"] = 1.0 if (ts.hour == 10 and ts.minute < 30) else 0.0

    if ts.weekday() < 5 and 10 <= ts.hour < 19:
        close_ts = ts.replace(hour=18, minute=50, second=0, microsecond=0)
        f["minutes_to_close"] = max(0, (close_ts - ts).total_seconds() / 60)
    else:
        f["minutes_to_close"] = -1.0

    # === F. News history per ticker (3 dims) ===
    n24, cum_sent, last_min = 0, 0.0, 1440.0
    for prev_ev in history.for_ticker(ticker, now=ts_utc):
        # produced_at parseable assumed (NewsHistory строит из валидных EnrichedNewsEvent)
        prev_ts = pd.to_datetime(prev_ev.produced_at)
        if prev_ts.tzinfo is not None:
            prev_ts = (prev_ts.tz_convert("UTC") + pd.Timedelta(hours=3)).tz_localize(None)
        delta = (ts - prev_ts).total_seconds()
        if delta <= 0 or delta > 86400:
            continue
        # Find this ticker's sentiment в prev_ev
        prev_ticker = next((t for t in prev_ev.payload.tickers if t.ticker == ticker), None)
        if prev_ticker is None:
            continue
        n24 += 1
        prev_sent_legacy = _SENTIMENT_MAP.get(prev_ticker.sentiment, "neutral")
        sv = (1 if prev_sent_legacy == "bullish"
              else -1 if prev_sent_legacy == "bearish" else 0)
        cum_sent += sv * float(prev_ticker.confidence)
        m = delta / 60
        if m < last_min:
            last_min = m

    f["news_count_24h"] = float(n24)
    f["cum_sentiment_24h"] = cum_sent
    f["time_since_last_min"] = last_min

    return f


def vectorize(feature_dict: Dict[str, float], feature_order: list[str]) -> np.ndarray:
    """Convert dict → np.ndarray по канонической колоночному порядку.

    Если в dict отсутствуют ожидаемые ключи — заполнить 0.0 и log warning.
    Это защита от schema drift при первой загрузке после bump'а порядка.
    """
    n = len(feature_order)
    out = np.zeros(n, dtype=np.float32)
    missing: list[str] = []
    for i, key in enumerate(feature_order):
        if key in feature_dict:
            out[i] = float(feature_dict[key])
        else:
            missing.append(key)
    if missing:
        log.warning("vectorize_missing_features count=%d sample=%s",
                    len(missing), missing[:5])
    return out
