"""Sprint 6.1 — Replay last-2-weeks VPS enriched events through LIVE Decision +
paper-fill simulation on historical prices/.

Loads enriched events pulled by `pull_enriched_from_vps.py`, reconstructs
EnrichedNewsEvent objects, runs them through:

    Predictor (live src/services/predictor) -> ml predictions
        -> Decision (live src/services/decision, post-Sprint-6.1 LENIENT filter)
            -> TradeSignalEvent (EXECUTE / REJECT)
                -> sprint4 BaselineFixedTpSl over PricesCache (paper fill)

All Redis interactions are stubbed in-memory — this is a pure offline backtest.
The point: feed our REAL collected events through the REAL fixed code with
historical prices that are now backfilled through 2026-06-05, and see whether
the documented walk-forward expectations (Sharpe ≈ 5, filter rate ≈ 5%) hold
on live-collected enrichment.

Usage:
    python scripts/replay_vps_window_backtest.py \
        --input data/replay/enriched_vps_window.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits"))
sys.path.insert(0, str(PROJECT_ROOT / "sprint4" / "exits" / "hybrid"))

# ---- Production-code imports ----
from src.contracts.enriched_news import EnrichedNewsEvent  # noqa: E402
from src.contracts.ml_prediction import MLPredictionEvent  # noqa: E402
from src.contracts.trade_signal import TradeSignalEvent  # noqa: E402

from src.services.predictor.pipeline import PredictorPipeline  # noqa: E402
from src.services.predictor.config import PredictorSettings  # noqa: E402
from src.services.predictor.model_loader import load_bundle  # noqa: E402
from src.services.predictor.news_history import NewsHistory  # noqa: E402
from src.services.predictor.metrics import PredictorMetrics  # noqa: E402
from src.infra.candles import CandleCache  # noqa: E402

from src.services.decision.pipeline import DecisionPipeline  # noqa: E402
from src.services.decision.config import DecisionSettings  # noqa: E402
from src.services.decision.metrics import DecisionMetrics  # noqa: E402

# ---- Sprint 4 paper-simulator ----
from base import Trade as Sprint4Trade  # noqa: E402
from baseline import BaselineFixedTpSl  # noqa: E402
from instruments import get_lot_size  # noqa: E402
from prices_cache import PricesCache  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("replay")
log.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# In-memory stubs replacing Redis-backed infra
# ---------------------------------------------------------------------------
class FakePublisher:
    """Collects publishes in-memory instead of XADD."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.collected: list[Any] = []
        # Some downstream code reads .redis.xadd directly; stub minimally.
        self.redis = _FakeRedisStub()
        self.stream = name

    async def publish(self, event: Any) -> None:
        self.collected.append(event)


class _FakeRedisStub:
    async def xadd(self, *args, **kwargs) -> None:
        pass


class FakeIdempotencyGuard:
    """Always claim — replay runs each event exactly once anyway."""
    async def claim(self, *, scope: str, key: str) -> bool:
        return True


class FakeEnrichmentCache:
    """Dict-backed lookup populated as we process EnrichedNewsEvents."""

    def __init__(self) -> None:
        self._store: dict[str, EnrichedNewsEvent] = {}

    def put(self, event: EnrichedNewsEvent) -> None:
        self._store[event.event_id] = event

    async def get(self, event_id: str) -> Optional[EnrichedNewsEvent]:
        return self._store.get(event_id)


class FakeRiskManager:
    """No risk gating — we WANT all surviving EXECUTE to flow through so we can
    measure pure filter+rr-decision behaviour. Bridge-side risk effects are
    measured separately by the paper-fill loop (cooldown, daily-kill).
    """

    def __init__(self) -> None:
        self._open: set[str] = set()
        self._cooldown: set[str] = set()
        self._daily_pnl: float = 0.0

    async def open_positions_count(self) -> int:
        return 0  # never gate — let everything pass to paper-fill stage

    async def is_cooldown_active(self, ticker: str) -> bool:
        return False  # not gated; paper-fill simulates its own cooldown

    async def is_daily_kill_triggered(self, when=None) -> bool:
        return False

    async def daily_pnl_pct(self, when=None) -> float:
        return 0.0


# ---------------------------------------------------------------------------
# Event reconstruction from JSONL
# ---------------------------------------------------------------------------
def load_enriched_events(jsonl_path: Path) -> list[EnrichedNewsEvent]:
    out: list[EnrichedNewsEvent] = []
    skipped = 0
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                env_dict = rec["envelope"]
                ev = EnrichedNewsEvent.model_validate(env_dict)
                out.append(ev)
            except Exception as e:
                skipped += 1
                if skipped <= 3:
                    log.warning("skip_jsonl_line err=%s", e)
    if skipped:
        log.warning("skipped %d lines on load", skipped)
    log.info("loaded %d EnrichedNewsEvents from %s", len(out), jsonl_path)
    return out


def event_news_time_utc(ev: EnrichedNewsEvent) -> datetime:
    """Best news_time as UTC datetime. Prefer payload.tg_published_at."""
    raw = ev.payload.tg_published_at or ev.produced_at
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Paper-fill simulation (sprint4 BaselineFixedTpSl)
# ---------------------------------------------------------------------------
MSK = timezone(timedelta(hours=3))

# Sprint 6.3 — honest per-asset-class round-trip costs (Sber «Самостоятельный»
# tariff + MOEX fees + Phase 2 empirical slippage). See scripts/costs_sber.py.
from scripts.costs_sber import ROUND_TRIP_COST_PCT, DEFAULT_RT_COST_PCT  # noqa: E402


def utc_to_msk_naive(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(MSK).replace(tzinfo=None)


def signal_to_sprint4_trade(
    signal: TradeSignalEvent, fold_idx: int,
    flat_cost_rub: Optional[float] = None,
) -> Optional[Sprint4Trade]:
    """Map our TradeSignal to a sprint4 Trade for BaselineFixedTpSl.simulate.

    Sprint4 Trade is frozen — has many extra fields we don't need but must fill
    with plausible defaults.
    """
    p = signal.payload
    if p.action != "EXECUTE":
        return None
    # news_time → MSK naive (sprint4 PricesCache convention)
    nt_raw = p.news_time or signal.produced_at
    nt_utc = datetime.fromisoformat(str(nt_raw).replace("Z", "+00:00"))
    if nt_utc.tzinfo is None:
        nt_utc = nt_utc.replace(tzinfo=timezone.utc)
    ts_open_msk = utc_to_msk_naive(nt_utc)

    side_int = 1 if p.side == "BUY" else -1
    horizon_min = int(str(p.horizon).rstrip("m")) if p.horizon else 60
    # Sprint 6.3 — honest round-trip cost from notional (was: flat 2.0₽, which
    # understated real Sber costs by ~200×; see SPRINT_6_3 brief 2026-06-09).
    # notional_rub = entry × lot_size × n_lots, same base as Bridge PaperExecutor.
    if flat_cost_rub is not None:
        cost_rub = flat_cost_rub
    else:
        notional_rub = (p.entry_price or 0.0) * get_lot_size(p.ticker) * (p.quantity or 1)
        cost_rub = notional_rub * ROUND_TRIP_COST_PCT.get(p.ticker, DEFAULT_RT_COST_PCT)
    return Sprint4Trade(
        ticker=p.ticker,
        fold=fold_idx,
        horizon_min=horizon_min,
        rr_threshold=p.rr_ratio or 2.0,
        model_type="general",
        ts_open=ts_open_msk,
        side=side_int,
        entry=p.entry_price or 0.0,
        size_lots=p.quantity or 1,
        sl_price=p.stop_loss or 0.0,
        tp_price=p.take_profit or 0.0,
        pred_mfe_pct=0.0,  # not used by BaselineFixedTpSl
        pred_mae_pct=0.0,
        ts_close_phase2=ts_open_msk + timedelta(minutes=horizon_min),
        exit_price_phase2=0.0,
        exit_reason_phase2="unknown",
        net_pnl_rub_phase2=0.0,
        cost_rub=cost_rub,
    )


def simulate_paper_fills(
    signals: list[TradeSignalEvent],
    cache: PricesCache,
    flat_cost_rub: Optional[float] = None,
) -> list[dict]:
    """Run BaselineFixedTpSl over each EXECUTE signal.

    Returns one row per simulated trade with realized PnL / R / exit_reason.
    Skips signals with no candle coverage.
    """
    strategy = BaselineFixedTpSl()
    rows: list[dict] = []
    skipped_no_bars = 0
    for sig in signals:
        t = signal_to_sprint4_trade(sig, fold_idx=0, flat_cost_rub=flat_cost_rub)
        if t is None:
            continue
        try:
            bars = cache.get_bars(
                t.ticker,
                t.ts_open,
                t.ts_open + timedelta(minutes=int(t.horizon_min * 1.5) + 2),
            )
        except FileNotFoundError:
            skipped_no_bars += 1
            continue
        if bars is None or len(bars) == 0:
            skipped_no_bars += 1
            continue
        try:
            res = strategy.simulate(t, bars)
        except Exception as e:
            log.warning("paper_sim_fail ticker=%s err=%s", t.ticker, e)
            continue
        if res is None:
            skipped_no_bars += 1
            continue
        rows.append({
            "ts_open": t.ts_open,
            "ts_close": res.ts_close,
            "ticker": t.ticker,
            "side": "BUY" if t.side == 1 else "SELL",
            "exit_reason": res.exit_reason,
            "realized_r": res.realized_r,
            "realized_pnl": res.realized_pnl,
            "duration_min": res.duration_min,
            "rr": sig.payload.rr_ratio,
            "quantity": t.size_lots,
            "notional_rub": round(t.entry * t.lot_size * t.size_lots, 2),
            "cost_rub": round(t.cost_rub, 2),
        })
    if skipped_no_bars:
        log.info("paper-sim skipped %d signals (no bars at ts_open)", skipped_no_bars)
    return rows


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def aggregate(metrics_p: PredictorMetrics, metrics_d: DecisionMetrics,
              trade_rows: list[dict]) -> dict:
    counters_p = dict(metrics_p._counters)  # noqa: SLF001
    counters_d = dict(metrics_d._counters)  # noqa: SLF001
    n_exec = counters_d.get("signals_execute", 0)
    n_rej = counters_d.get("signals_reject", 0)
    events_in = counters_d.get("events_in", 0)
    rr_rej = counters_d.get("rejects.rr_below_threshold", 0)
    dir_rej = counters_d.get("rejects.direction_filter", 0)
    market_rej = counters_d.get("rejects.market_closed", 0)
    stale_rej = counters_d.get("rejects.stale_features", 0)

    post_rr_universe = max(events_in - rr_rej - market_rej - stale_rej, 0)
    filter_rate = dir_rej / post_rr_universe if post_rr_universe else 0.0
    execute_rate = n_exec / events_in if events_in else 0.0

    # PnL stats
    n_trades = len(trade_rows)
    if n_trades > 0:
        df = pd.DataFrame(trade_rows)
        pnl = df["realized_pnl"].values
        total_pnl = float(pnl.sum())
        median_pnl = float(np.median(pnl))
        win_rate = float((pnl > 0).mean())
        df["close_dt"] = pd.to_datetime([r["ts_close"] for r in trade_rows], errors="coerce")
        df["date"] = df["close_dt"].dt.date
        daily = df.groupby("date")["realized_pnl"].sum()
        sharpe = (
            float((daily.mean() / daily.std()) * np.sqrt(252))
            if len(daily) > 1 and daily.std() > 0
            else 0.0
        )
        cum = pd.Series(pnl).cumsum()
        max_dd = float((cum - cum.cummax()).min())
        by_exit = Counter(df["exit_reason"]).most_common()
        total_cost = float(df["cost_rub"].sum()) if "cost_rub" in df else 0.0
        mean_cost = float(df["cost_rub"].mean()) if "cost_rub" in df else 0.0
    else:
        total_pnl = median_pnl = sharpe = max_dd = 0.0
        win_rate = 0.0
        by_exit = []
        total_cost = mean_cost = 0.0

    return {
        "predictor_events_in": counters_p.get("events_in", 0),
        "predictor_predictions_out": counters_p.get("predictions_out", 0),
        "predictor_skip_non_financial": counters_p.get("events_skipped_non_financial", 0),
        "predictor_skip_no_tickers": counters_p.get("events_skipped_no_tickers", 0),
        "predictor_off_whitelist": counters_p.get("tickers_skipped_off_whitelist", 0),
        "predictor_missing_data": counters_p.get("errors.missing_market_data", 0),
        "predictor_stale_news": counters_p.get("errors.stale_candles_at_news_time", 0),
        "decision_events_in": events_in,
        "decision_rejects_rr": rr_rej,
        "decision_rejects_market": market_rej,
        "decision_rejects_stale": stale_rej,
        "decision_rejects_direction": dir_rej,
        "decision_rejects_other": n_rej - rr_rej - market_rej - stale_rej - dir_rej,
        "decision_signals_execute": n_exec,
        "decision_signals_reject": n_rej,
        "filter_rate_pct": round(filter_rate * 100, 2),
        "execute_rate_pct": round(execute_rate * 100, 2),
        "paper_trades": n_trades,
        "paper_total_pnl_rub": round(total_pnl, 2),
        "paper_total_cost_rub": round(total_cost, 2),
        "paper_mean_cost_rub": round(mean_cost, 2),
        "paper_median_pnl_rub": round(median_pnl, 2),
        "paper_win_rate_pct": round(win_rate * 100, 2),
        "paper_sharpe_daily_annualized": round(sharpe, 2),
        "paper_max_drawdown_rub": round(max_dd, 2),
        "paper_exit_reason_breakdown": by_exit,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def amain() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "enriched_vps_window.jsonl")
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "vps_window_backtest_report.json")
    ap.add_argument("--trades-out", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "vps_window_trades.csv")
    ap.add_argument("--no-filter", action="store_true",
                    help="A1: bypass B_filter — every post-R:R signal becomes EXECUTE. "
                         "Measures XGBoost-alone PnL on the same data.")
    ap.add_argument("--strict-filter", action="store_true",
                    help="A4: use STRICT B_filter (reject if no ticker / neutral) "
                         "via monkey-patch of apply_direction_filter.")
    ap.add_argument("--models-dir", type=Path, default=None,
                    help="Override XGBoost models dir (e.g. data/models/predictor/v1_70b_rolling). "
                         "Defaults to PredictorSettings.models_dir (v1 legacy).")
    ap.add_argument("--blacklist", default="",
                    help="Comma-separated tickers to skip BEFORE Predictor "
                         "(e.g. GAZP,NG,VTBR). Useful for Sprint 6.1 anti-select drill-down.")
    ap.add_argument("--direction-min-conf", type=float, default=None,
                    help="Override direction_filter_min_confidence (default 0.5).")
    ap.add_argument("--rr-threshold", type=float, default=None,
                    help="Override rr_threshold (default 2.0).")
    ap.add_argument("--min-mfe-pct", type=float, default=None,
                    help="Override min_mfe_pct (default 0.15).")
    ap.add_argument("--legacy-flat-cost", type=float, default=None, metavar="RUB",
                    help="Use a flat per-trade cost in RUB instead of the honest "
                         "Sber notional-based model (pass 2.0 to reproduce "
                         "Sprint 6.1/6.2 numbers).")
    args = ap.parse_args()

    if args.no_filter and args.strict_filter:
        log.error("--no-filter and --strict-filter are mutually exclusive")
        return 2

    # --- Filter variant wiring ---
    # apply_direction_filter is imported at module scope in decision.pipeline.
    # We monkey-patch the symbol used by DecisionPipeline.process to switch
    # semantics without touching production code.
    if args.no_filter or args.strict_filter:
        from src.services.decision import pipeline as _decision_pipeline_mod
        from src.services.decision.filter import FilterDecision

        if args.no_filter:
            def _always_include(event, ticker, side, min_confidence=0.5):
                return FilterDecision(True)
            _decision_pipeline_mod.apply_direction_filter = _always_include  # type: ignore[attr-defined]
            log.info("FILTER MODE: ALWAYS_INCLUDE (no B_filter)")
        else:
            # STRICT — mirrors the pre-Sprint-6.1 broken prod
            def _strict_filter(event, ticker, side, min_confidence=0.5):
                from src.services.decision.filter import SIDE_TO_DIRECTION, get_ticker_impact
                expected = SIDE_TO_DIRECTION.get(side)
                if expected is None:
                    return FilterDecision(False, f"unknown side {side}")
                ti = get_ticker_impact(event, ticker)
                if ti is None:
                    return FilterDecision(False, "STRICT: ticker not in LLM tickers[]")
                if ti.direction == "neutral":
                    return FilterDecision(False, "STRICT: LLM direction=neutral")
                if ti.direction != expected:
                    return FilterDecision(False, f"STRICT: direction={ti.direction} != {expected}")
                if ti.confidence < min_confidence:
                    return FilterDecision(False, f"STRICT: confidence {ti.confidence}")
                return FilterDecision(True)
            _decision_pipeline_mod.apply_direction_filter = _strict_filter  # type: ignore[attr-defined]
            log.info("FILTER MODE: STRICT (old pre-Sprint-6.1 prod)")
    else:
        log.info("FILTER MODE: LENIENT (current prod, post-revert)")

    if not args.input.exists():
        log.error("input not found: %s", args.input)
        return 1

    # 1. Load events
    events = load_enriched_events(args.input)
    if not events:
        log.error("no events loaded")
        return 1
    events.sort(key=event_news_time_utc)
    log.info("event window: %s -> %s",
             event_news_time_utc(events[0]).isoformat(),
             event_news_time_utc(events[-1]).isoformat())

    # 2. Build live infrastructure
    log.info("Loading CandleCache from prices/...")
    pred_settings = PredictorSettings()
    if args.models_dir:
        pred_settings = pred_settings.model_copy(update={"models_dir": args.models_dir})
        log.info("MODELS DIR OVERRIDE: %s", args.models_dir)
    candle_cache = CandleCache(pred_settings.prices_dir)
    candle_cache.load_all()

    log.info("Loading XGBoost models from %s...", pred_settings.models_dir)
    bundle = load_bundle(pred_settings.models_dir)

    news_history = NewsHistory(
        lookback_hours=pred_settings.news_history_lookback_hours,
        per_ticker_maxlen=pred_settings.news_history_per_ticker_maxlen,
    )

    metrics_p = PredictorMetrics()
    metrics_d = DecisionMetrics()
    pred_pub = FakePublisher("ml:predictions")
    pred_dlq = FakePublisher("ml:predictions:dlq")
    decision_pub = FakePublisher("trade:signals")
    enrichment_cache = FakeEnrichmentCache()
    risk = FakeRiskManager()
    idem = FakeIdempotencyGuard()

    predictor = PredictorPipeline(
        bundle=bundle,
        candles=candle_cache,
        history=news_history,
        idem=idem,
        publisher_main=pred_pub,
        publisher_dlq=pred_dlq,
        metrics=metrics_p,
        settings=pred_settings,
    )

    decision_settings = DecisionSettings()
    if args.direction_min_conf is not None:
        decision_settings = decision_settings.model_copy(
            update={"direction_filter_min_confidence": args.direction_min_conf})
        log.info("direction_filter_min_confidence OVERRIDE: %.2f", args.direction_min_conf)
    if args.rr_threshold is not None:
        decision_settings = decision_settings.model_copy(
            update={"rr_threshold": args.rr_threshold})
        log.info("rr_threshold OVERRIDE: %.2f", args.rr_threshold)
    if args.min_mfe_pct is not None:
        decision_settings = decision_settings.model_copy(
            update={"min_mfe_pct": args.min_mfe_pct})
        log.info("min_mfe_pct OVERRIDE: %.3f", args.min_mfe_pct)
    decision = DecisionPipeline(
        settings=decision_settings,
        enrichment_cache=enrichment_cache,
        risk_manager=risk,
        idem=idem,
        publisher=decision_pub,
        metrics=metrics_d,
    )

    log.info("DecisionSettings.direction_filter_min_confidence=%.2f horizon=%d",
             decision_settings.direction_filter_min_confidence,
             decision_settings.horizon_min)

    # 3. Drive replay
    blacklist = {t.strip().upper() for t in args.blacklist.split(",") if t.strip()}
    if blacklist:
        log.info("BLACKLIST tickers: %s", sorted(blacklist))
    log.info("Replaying %d events through Predictor → Decision ...", len(events))
    progress_step = max(len(events) // 20, 1)
    skipped_by_blacklist = 0
    for i, ev in enumerate(events):
        # Filter out blacklisted tickers before they reach Predictor — emulates
        # whitelist tweak without touching the live PredictorSettings.
        if blacklist and any(t.ticker.upper() in blacklist for t in ev.payload.tickers):
            new_impacts = [t for t in ev.payload.tickers if t.ticker.upper() not in blacklist]
            skipped_in_ev = len(ev.payload.tickers) - len(new_impacts)
            if not new_impacts:
                skipped_by_blacklist += skipped_in_ev
                # Still cache the event so EnrichmentCache lookups won't miss;
                # but Predictor will see empty tickers[] and skip naturally.
                ev = ev.model_copy(update={
                    "payload": ev.payload.model_copy(update={"tickers": []})
                })
            else:
                skipped_by_blacklist += skipped_in_ev
                ev = ev.model_copy(update={
                    "payload": ev.payload.model_copy(update={"tickers": new_impacts})
                })
        enrichment_cache.put(ev)
        pre_n_pred = len(pred_pub.collected)
        await predictor.process(ev)
        new_preds = pred_pub.collected[pre_n_pred:]
        for pred in new_preds:
            if isinstance(pred, MLPredictionEvent):
                await decision.process(pred)
        if (i + 1) % progress_step == 0:
            log.info("  ... %d/%d events  preds_total=%d signals_total=%d",
                     i + 1, len(events),
                     len(pred_pub.collected), len(decision_pub.collected))
    if blacklist:
        log.info("Blacklist removed %d ticker-impacts before Predictor", skipped_by_blacklist)

    log.info("Replay done.  preds=%d signals=%d", len(pred_pub.collected),
             len(decision_pub.collected))

    # 4. Paper-fill simulation
    execute_signals = [
        s for s in decision_pub.collected
        if isinstance(s, TradeSignalEvent) and s.payload.action == "EXECUTE"
    ]
    log.info("Paper-simulating %d EXECUTE signals via sprint4 BaselineFixedTpSl ...",
             len(execute_signals))
    prices_cache = PricesCache()
    prices_cache.warmup()
    if args.legacy_flat_cost is not None:
        log.info("COST MODE: LEGACY FLAT %.2f₽/trade", args.legacy_flat_cost)
    else:
        log.info("COST MODE: HONEST Sber notional-based (stocks 0.19%% / "
                 "currencies 0.50%% / futures 0.08%% RT)")
    trade_rows = simulate_paper_fills(execute_signals, prices_cache,
                                      flat_cost_rub=args.legacy_flat_cost)
    log.info("Paper sim: %d filled trades", len(trade_rows))

    # 5. Aggregate + write
    summary = aggregate(metrics_p, metrics_d, trade_rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, default=str, ensure_ascii=False),
                            encoding="utf-8")
    log.info("Wrote %s", args.output)

    if trade_rows:
        pd.DataFrame(trade_rows).to_csv(args.trades_out, index=False, encoding="utf-8")
        log.info("Wrote %s", args.trades_out)

    # 6. Console summary
    print("\n" + "=" * 70)
    print("VPS REPLAY BACKTEST — Sprint 6.1 LENIENT filter")
    print("=" * 70)
    for k, v in summary.items():
        print(f"  {k:36s} {v}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(amain()))
