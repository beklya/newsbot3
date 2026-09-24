r"""Sprint 6.1 — compare Groq 70B (VPS prod) vs DI 70B (Y5) on identical events.

Joins VPS-cached enrichment events to Y5 DI enrichment via text_hash (pulled
from VPS news:raw through tunnel).  For each match, compares:
  - is_financial / is_actionable booleans
  - category, urgency, expected_timeframe enums
  - tickers count + per-ticker direction/sentiment/confidence/impact_strength
  - sell_the_news flag distribution

Prints distribution comparison + per-event diff rates.

Usage:
    python scripts/compare_groq_vs_di.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("compare")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def pull_raw_text_hashes(redis_url: str) -> dict[str, str]:
    """raw_event_id -> text_hash from VPS news:raw stream."""
    r = redis.Redis.from_url(redis_url, decode_responses=False,
                              socket_connect_timeout=5, socket_timeout=30)
    mapping: dict[str, str] = {}
    cursor = "-"
    last_id = None
    while True:
        batch = r.xrange("news:raw", min=cursor, max="+", count=1000)
        if not batch:
            break
        new_in_batch = 0
        for sid, fields in batch:
            entry_id = sid.decode()
            if last_id is not None and entry_id == last_id:
                continue
            last_id = entry_id
            new_in_batch += 1
            raw = fields.get(b"data")
            if not raw:
                continue
            try:
                env = json.loads(raw)
            except json.JSONDecodeError:
                continue
            eid = env.get("event_id")
            th = env.get("payload", {}).get("text_hash")
            if eid and th:
                mapping[eid] = th
        cursor = last_id
        if new_in_batch == 0 or len(batch) < 1000:
            break
    return mapping


def load_groq_enrichments(jsonl_path: Path,
                           raw_to_hash: dict[str, str]) -> dict[str, dict]:
    """text_hash -> enrichment dict (Groq side, from VPS replay JSONL)."""
    out: dict[str, dict] = {}
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            env = rec["envelope"]
            p = env["payload"]
            raw_id = p.get("raw_event_id")
            th = raw_to_hash.get(raw_id)
            if not th:
                continue
            out[th] = {
                "provider": "groq",
                "is_financial": bool(p.get("is_financial") or False),
                "is_actionable": bool(p.get("is_actionable") or False),
                "category": p.get("category") or "other",
                "urgency": p.get("urgency") or "low",
                "expected_timeframe": p.get("expected_timeframe") or "medium",
                "tickers": [
                    {
                        "ticker": t.get("ticker"),
                        "direction": (t.get("direction") or "neutral").lower(),
                        "sentiment": (t.get("sentiment") or "neutral").lower(),
                        "confidence": float(t.get("confidence") or 0.0),
                        "impact_strength": float(t.get("impact_strength") or 0.0),
                    }
                    for t in (p.get("tickers") or [])
                    if isinstance(t, dict)
                ],
            }
    return out


def load_di_enrichments(parquet_path: Path) -> dict[str, dict]:
    """text_hash -> enrichment dict (DI side, from Y5 parquet)."""
    df = pl.read_parquet(parquet_path).filter(pl.col("is_enriched"))
    out: dict[str, dict] = {}
    for row in df.iter_rows(named=True):
        th = row.get("text_hash")
        if not th:
            continue
        out[th] = {
            "provider": "di",
            "is_financial": bool(row.get("is_financial") or False),
            "is_actionable": bool(row.get("is_actionable") or False),
            "category": row.get("category") or "other",
            "urgency": row.get("urgency") or "low",
            "expected_timeframe": row.get("expected_timeframe") or "medium",
            "tickers": [
                {
                    "ticker": t.get("ticker"),
                    "direction": (t.get("direction") or "neutral").lower(),
                    "sentiment": (t.get("sentiment") or "neutral").lower(),
                    "confidence": float(t.get("confidence") or 0.0),
                    "impact_strength": float(t.get("impact_strength") or 0.0),
                }
                for t in (row.get("tickers") or [])
                if isinstance(t, dict)
            ],
        }
    return out


def compare(groq: dict[str, dict], di: dict[str, dict]) -> None:
    common = set(groq) & set(di)
    log.info("MATCH: %d events in both providers", len(common))
    log.info("  Groq only: %d  DI only: %d",
             len(set(groq) - common), len(set(di) - common))

    # === Event-level booleans/enums ===
    def pct(b: bool) -> float:
        return 100 * b
    g_fin = sum(1 for k in common if groq[k]["is_financial"]) / len(common) * 100
    d_fin = sum(1 for k in common if di[k]["is_financial"]) / len(common) * 100
    g_act = sum(1 for k in common if groq[k]["is_actionable"]) / len(common) * 100
    d_act = sum(1 for k in common if di[k]["is_actionable"]) / len(common) * 100
    fin_disagree = sum(1 for k in common if groq[k]["is_financial"] != di[k]["is_financial"])
    act_disagree = sum(1 for k in common if groq[k]["is_actionable"] != di[k]["is_actionable"])

    print()
    print("=" * 70)
    print("EVENT-LEVEL DISTRIBUTION")
    print("=" * 70)
    print(f"{'Field':<30} {'Groq %':>10} {'DI %':>10} {'Diff':>8} {'disagree %':>12}")
    print("-" * 70)
    print(f"{'is_financial=True':<30} {g_fin:>10.1f} {d_fin:>10.1f} {d_fin-g_fin:>+8.1f} "
          f"{100*fin_disagree/len(common):>12.1f}")
    print(f"{'is_actionable=True':<30} {g_act:>10.1f} {d_act:>10.1f} {d_act-g_act:>+8.1f} "
          f"{100*act_disagree/len(common):>12.1f}")

    # Category
    print()
    print(f"{'Category':<30} {'Groq %':>10} {'DI %':>10} {'Diff':>8}")
    print("-" * 70)
    g_cats = Counter(groq[k]["category"] for k in common)
    d_cats = Counter(di[k]["category"] for k in common)
    all_cats = sorted(set(g_cats) | set(d_cats))
    for c in all_cats:
        gp = g_cats.get(c, 0) / len(common) * 100
        dp = d_cats.get(c, 0) / len(common) * 100
        print(f"  {c:<28} {gp:>10.1f} {dp:>10.1f} {dp-gp:>+8.1f}")

    # Urgency
    print()
    print(f"{'Urgency':<30} {'Groq %':>10} {'DI %':>10} {'Diff':>8}")
    print("-" * 70)
    g_urg = Counter(groq[k]["urgency"] for k in common)
    d_urg = Counter(di[k]["urgency"] for k in common)
    for u in ["high", "medium", "low"]:
        gp = g_urg.get(u, 0) / len(common) * 100
        dp = d_urg.get(u, 0) / len(common) * 100
        print(f"  {u:<28} {gp:>10.1f} {dp:>10.1f} {dp-gp:>+8.1f}")

    # Expected timeframe
    print()
    print(f"{'Expected timeframe':<30} {'Groq %':>10} {'DI %':>10} {'Diff':>8}")
    print("-" * 70)
    g_tf = Counter(groq[k]["expected_timeframe"] for k in common)
    d_tf = Counter(di[k]["expected_timeframe"] for k in common)
    for t in sorted(set(g_tf) | set(d_tf)):
        gp = g_tf.get(t, 0) / len(common) * 100
        dp = d_tf.get(t, 0) / len(common) * 100
        print(f"  {t:<28} {gp:>10.1f} {dp:>10.1f} {dp-gp:>+8.1f}")

    # === Ticker-level ===
    g_n = sum(len(groq[k]["tickers"]) for k in common)
    d_n = sum(len(di[k]["tickers"]) for k in common)
    print()
    print("=" * 70)
    print("TICKER-LEVEL DISTRIBUTION")
    print("=" * 70)
    print(f"Total ticker-impacts:  Groq={g_n}  DI={d_n}  ratio DI/Groq={d_n/g_n:.2f}")

    # Average tickers per event with at least 1 ticker
    g_with = sum(1 for k in common if groq[k]["tickers"])
    d_with = sum(1 for k in common if di[k]["tickers"])
    print(f"Events with &gt;=1 ticker: Groq={g_with} ({100*g_with/len(common):.1f}%)  "
          f"DI={d_with} ({100*d_with/len(common):.1f}%)")

    # Direction distribution
    print()
    print(f"{'Direction':<30} {'Groq %':>10} {'DI %':>10} {'Diff':>8}")
    print("-" * 70)
    g_dir: Counter = Counter()
    d_dir: Counter = Counter()
    for k in common:
        for t in groq[k]["tickers"]:
            g_dir[t["direction"]] += 1
        for t in di[k]["tickers"]:
            d_dir[t["direction"]] += 1
    g_dir_tot = sum(g_dir.values()) or 1
    d_dir_tot = sum(d_dir.values()) or 1
    for d in ["long", "short", "neutral"]:
        gp = g_dir.get(d, 0) / g_dir_tot * 100
        dp = d_dir.get(d, 0) / d_dir_tot * 100
        print(f"  {d:<28} {gp:>10.1f} {dp:>10.1f} {dp-gp:>+8.1f}")

    # Confidence stats
    print()
    print(f"{'Confidence stats':<30} {'Groq':>10} {'DI':>10}")
    print("-" * 70)
    g_confs = [t["confidence"] for k in common for t in groq[k]["tickers"]]
    d_confs = [t["confidence"] for k in common for t in di[k]["tickers"]]

    def stats(s):
        if not s:
            return (0, 0, 0, 0)
        s_sorted = sorted(s)
        n = len(s)
        return (
            sum(s) / n,
            s_sorted[n // 2],
            sum(1 for x in s if x >= 0.5) / n * 100,
            sum(1 for x in s if x >= 0.7) / n * 100,
        )
    g_mean, g_med, g_ge05, g_ge07 = stats(g_confs)
    d_mean, d_med, d_ge05, d_ge07 = stats(d_confs)
    print(f"  {'mean':<28} {g_mean:>10.3f} {d_mean:>10.3f}")
    print(f"  {'median':<28} {g_med:>10.3f} {d_med:>10.3f}")
    print(f"  {'% conf &gt;= 0.5':<28} {g_ge05:>10.1f} {d_ge05:>10.1f}")
    print(f"  {'% conf &gt;= 0.7':<28} {g_ge07:>10.1f} {d_ge07:>10.1f}")

    # Per-event direction agreement on COMMON tickers
    print()
    print("=" * 70)
    print("PER-EVENT × TICKER AGREEMENT")
    print("=" * 70)
    n_pairs = 0
    n_dir_match = 0
    n_dir_disagree = 0
    n_only_groq = 0
    n_only_di = 0
    for k in common:
        g_by = {t["ticker"]: t for t in groq[k]["tickers"]}
        d_by = {t["ticker"]: t for t in di[k]["tickers"]}
        all_ticks = set(g_by) | set(d_by)
        for ticker in all_ticks:
            n_pairs += 1
            in_g = ticker in g_by
            in_d = ticker in d_by
            if in_g and in_d:
                if g_by[ticker]["direction"] == d_by[ticker]["direction"]:
                    n_dir_match += 1
                else:
                    n_dir_disagree += 1
            elif in_g:
                n_only_groq += 1
            else:
                n_only_di += 1
    print(f"Total ticker-mentions (union):  {n_pairs}")
    print(f"  Both providers mentioned + direction MATCH:    {n_dir_match}  ({100*n_dir_match/n_pairs:.1f}%)")
    print(f"  Both providers mentioned + direction DISAGREE: {n_dir_disagree}  ({100*n_dir_disagree/n_pairs:.1f}%)")
    print(f"  Only Groq mentioned:                          {n_only_groq}  ({100*n_only_groq/n_pairs:.1f}%)")
    print(f"  Only DI mentioned:                            {n_only_di}  ({100*n_only_di/n_pairs:.1f}%)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vps-jsonl", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "enriched_vps_window.jsonl")
    ap.add_argument("--di-parquet", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "y5_gap_70b_v1_0_0.parquet")
    ap.add_argument("--redis-url", default="redis://127.0.0.1:6380")
    args = ap.parse_args()

    log.info("Pulling raw_event_id -> text_hash from VPS tunnel ...")
    raw_to_hash = pull_raw_text_hashes(args.redis_url)
    log.info("  %d raw events", len(raw_to_hash))

    log.info("Loading Groq enrichments from %s ...", args.vps_jsonl)
    groq = load_groq_enrichments(args.vps_jsonl, raw_to_hash)
    log.info("  %d Groq events with text_hash", len(groq))

    log.info("Loading DI enrichments from %s ...", args.di_parquet)
    di = load_di_enrichments(args.di_parquet)
    log.info("  %d DI events", len(di))

    compare(groq, di)
    return 0


if __name__ == "__main__":
    sys.exit(main())
