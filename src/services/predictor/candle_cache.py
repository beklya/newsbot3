"""Backwards-compat re-export. Real definition moved to src/infra/candles.py
в Sprint 5.3 (shared between Predictor + Bridge).

Tests и downstream imports продолжают работать через этот thin shim.
"""
from src.infra.candles import CSV_PREFIX_FOR_CANONICAL, CandleCache  # noqa: F401
