"""
Sprint 4 / Commit 4.0 - Sanity Check (REVISION 4)

REV4 fixes:
  - bars range = [ts_open, ts_open + 2*horizon] -- ВКЛЮЧАЯ entry-минуту
  - baseline создаётся в default Phase 2 mode (entry_bar_inclusive=True,
    after_time_stop_bar_inclusive=True)

  Это должно дать ~0% diff vs Phase 2 baseline.
"""

from __future__ import annotations

import logging
import sys
import time
from collections import Counter
from datetime import timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from base import Trade
from baseline import BaselineFixedTpSl
from prices_cache import PricesCache
from trades_loader import load_trades_best_combo


def run_sanity() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    log = logging.getLogger("sanity")

    print("=" * 70)
    print("  Sprint 4 / Commit 4.0 - Sanity Check (REV4: Phase 2 reproducibility)")
    print("=" * 70)
    trades = load_trades_best_combo()
    print(f"\nLoaded {len(trades)} trades from best combo")

    cache = PricesCache()
    unique_tickers = sorted({t.ticker for t in trades})
    print(f"Warming up cache for {len(unique_tickers)} tickers")
    cache.warmup(unique_tickers)

    # Phase 2 mode (default)
    strategy = BaselineFixedTpSl()
    print(f"\nStrategy: {strategy.name}")
    print(f"  entry_bar_inclusive: {strategy.entry_bar_inclusive}")
    print(f"  after_time_stop_bar_inclusive: {strategy.after_time_stop_bar_inclusive}")

    print(f"\nSimulating {len(trades)} trades...")
    started = time.time()

    results = []
    n_no_bars = 0
    for i, trade in enumerate(trades):
        try:
            # *** REV4: окно ВКЛЮЧАЕТ entry-минуту (ts_open) ***
            bars = cache.get_bars(
                trade.ticker,
                trade.ts_open,  # !!! не +1 минута, а ровно ts_open !!!
                trade.ts_open + timedelta(minutes=trade.horizon_min * 2),
                inclusive="both",
            )
        except FileNotFoundError as e:
            log.warning("No CSV for %s: %s", trade.ticker, e)
            n_no_bars += 1
            continue

        if bars.empty:
            n_no_bars += 1
            continue

        result = strategy.simulate(trade, bars)
        results.append((trade, result))

        if (i + 1) % 500 == 0:
            elapsed = time.time() - started
            rate = (i + 1) / elapsed
            eta = (len(trades) - i - 1) / rate
            print(f"  {i+1}/{len(trades)}  rate={rate:.0f} t/s  ETA={eta:.0f}s")

    elapsed = time.time() - started
    print(f"\nDone in {elapsed:.1f}s ({len(results)} simulated, {n_no_bars} skipped)")

    print()
    print("=" * 70)
    print("  Results vs Phase 2")
    print("=" * 70)

    sim_total_pnl = sum(r.realized_pnl for _, r in results)
    phase2_total_pnl = sum(t.net_pnl_rub_phase2 for t, _ in results)
    pnl_diff = sim_total_pnl - phase2_total_pnl
    pnl_diff_pct = 100 * pnl_diff / abs(phase2_total_pnl) if phase2_total_pnl != 0 else 0

    print(f"  Total PnL (simulated):  {sim_total_pnl:>15,.2f}")
    print(f"  Total PnL (Phase 2):    {phase2_total_pnl:>15,.2f}")
    print(f"  Diff:                   {pnl_diff:>+15,.2f}  ({pnl_diff_pct:+.2f}%)")

    sim_dist = Counter(r.exit_reason for _, r in results)
    p2_dist = Counter(t.exit_reason_phase2 for t, _ in results)
    print(f"\n  Exit reason distribution:")
    print(f"  {'reason':<10s} {'sim':>14s} {'phase2':>14s} {'diff':>10s}")
    all_reasons = sorted(set(sim_dist.keys()) | set(p2_dist.keys()))
    for r in all_reasons:
        s = sim_dist.get(r, 0)
        p = p2_dist.get(r, 0)
        s_pct = 100 * s / len(results) if results else 0
        p_pct = 100 * p / len(results) if results else 0
        print(f"  {r:<10s} {s:>6d} ({s_pct:>5.1f}%) {p:>6d} ({p_pct:>5.1f}%) "
              f"{s_pct - p_pct:>+5.1f}%")

    n_disagree = sum(1 for t, r in results if r.exit_reason != t.exit_reason_phase2)
    print(f"\n  Exit reason disagreement: {n_disagree}/{len(results)} "
          f"({100*n_disagree/len(results):.1f}%)")

    print(f"\n  Per-ticker total PnL:")
    print(f"  {'ticker':<8s} {'n':>5s} {'sim_pnl':>15s} {'p2_pnl':>15s} "
          f"{'diff':>15s} {'diff%':>8s}")
    per_ticker = {}
    for t, r in results:
        per_ticker.setdefault(t.ticker, {"n": 0, "sim": 0.0, "p2": 0.0})
        per_ticker[t.ticker]["n"] += 1
        per_ticker[t.ticker]["sim"] += r.realized_pnl
        per_ticker[t.ticker]["p2"] += t.net_pnl_rub_phase2
    for tk in sorted(per_ticker.keys()):
        info = per_ticker[tk]
        diff = info["sim"] - info["p2"]
        diff_pct = 100 * diff / abs(info["p2"]) if info["p2"] != 0 else 0
        marker = ""
        if abs(diff_pct) > 10:
            marker = " (!)"
        elif abs(diff_pct) > 5:
            marker = " (.)"
        print(f"  {tk:<8s} {info['n']:>5d} {info['sim']:>+15,.0f} "
              f"{info['p2']:>+15,.0f} {diff:>+15,.0f} {diff_pct:>+7.1f}%{marker}")

    print()
    print("=" * 70)
    print("  Acceptance verdict")
    print("=" * 70)

    ok = True
    if abs(pnl_diff_pct) > 1.0:
        print(f"  [FAIL] Total PnL diff = {pnl_diff_pct:+.2f}% > 1%")
        ok = False
    elif abs(pnl_diff_pct) > 0.1:
        print(f"  [WARN] Total PnL diff = {pnl_diff_pct:+.2f}% (within 1% but >0.1%)")
    else:
        print(f"  [OK]   Total PnL diff = {pnl_diff_pct:+.4f}% (essentially identical)")

    max_reason_diff = max(
        abs(100 * sim_dist.get(r, 0) / len(results) - 100 * p2_dist.get(r, 0) / len(results))
        for r in all_reasons
    ) if results else 0
    if max_reason_diff > 5:
        print(f"  [WARN] Max exit_reason diff = {max_reason_diff:.1f} p.p. > 5")
        ok = False
    else:
        print(f"  [OK]   Max exit_reason diff = {max_reason_diff:.1f} p.p.")

    if ok:
        print(f"\n  SANITY CHECK PASSED")
        print(f"  -> Ready to implement 4 remaining strategies")
    else:
        print(f"\n  SANITY CHECK FAILED")


if __name__ == "__main__":
    run_sanity()
