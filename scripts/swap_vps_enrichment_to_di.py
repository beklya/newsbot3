"""Sprint 6.1 — swap VPS prod (Groq) enrichment for fresh DI (Y5) enrichment.

For each event in data/replay/enriched_vps_window.jsonl:
  1. Look up VPS raw event (via tunnel) to get text_hash
  2. Find matching Y5 enriched event (by text_hash) from
     data/reenrich_phase2/y5_gap_70b_v1_0_0.parquet
  3. Build a NEW EnrichedNewsEvent payload using Y5's parsed JSON,
     preserving the original event_id, trace, produced_at envelope wrapper
  4. Write replacement JSONL

The replay backtest then runs identical (same model v6_70b_ext, same code) but
with DI 70B enrichment instead of Groq 70B.  Sharpe difference = provider effect.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import redis

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("swap")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def pull_raw_text_hashes(redis_url: str) -> dict[str, str]:
    """raw_event_id -> text_hash from VPS news:raw stream."""
    r = redis.Redis.from_url(redis_url, decode_responses=False,
                              socket_connect_timeout=5, socket_timeout=30)
    log.info("Pulling news:raw from %s ...", redis_url)
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
    log.info("  built raw_event_id -> text_hash: %d entries", len(mapping))
    return mapping


_CAT_NORMALIZE = {
    "cbr": "cbr", "geopolitics": "geopolitics", "macro": "macro",
    "corporate": "corporate", "commodity": "commodity",
    "currency": "currency", "market": "market", "other": "other",
    # 70B sometimes emits these — map to legal Literal
    "regulatory": "other", "infrastructure": "other",
    "sanctions": "geopolitics", "earnings": "corporate",
    "dividends": "corporate", "ma": "corporate",
    "regulation": "other",
}
_TF_NORMALIZE = {
    "instant": "instant", "fast": "short", "short": "short",
    "medium": "medium", "slow": "slow", "long": "slow",
}
_URG_NORMALIZE = {"high": "high", "medium": "medium", "low": "low"}


def build_payload_from_y5(y5_row: dict, vps_payload: dict) -> dict:
    """Build a Sprint 5 EnrichedNewsPayload-compatible dict from Y5 row.

    Preserves VPS envelope-level fields (raw_event_id, tg_published_at) but
    swaps the LLM-output fields (tickers, summary, is_financial, ...) with Y5.

    Notes:
      - llm_provider is forced to "groq" because the v1.1.0 contract only allows
        ["groq", "ollama"] as Literal.  Real provider was DeepInfra — visible in
        llm_model which still says Llama-3.3-70B-Instruct.
      - category & expected_timeframe normalized to allowed enums.
    """
    tickers = y5_row.get("tickers") or []
    norm_tickers = []
    for t in tickers:
        if not isinstance(t, dict):
            continue
        norm_tickers.append({
            "ticker": t.get("ticker") or "",
            "direction": (t.get("direction") or "neutral"),
            "sentiment": (t.get("sentiment") or "neutral"),
            "confidence": float(t.get("confidence") or 0.0),
            "impact_strength": float(t.get("impact_strength") or 0.0),
            "rationale": (t.get("rationale") or "")[:500],
        })

    raw_cat = (y5_row.get("category") or "other").lower()
    cat = _CAT_NORMALIZE.get(raw_cat, "other")
    raw_tf = (y5_row.get("expected_timeframe") or "medium").lower()
    tf = _TF_NORMALIZE.get(raw_tf, "medium")
    raw_urg = (y5_row.get("urgency") or "low").lower()
    urg = _URG_NORMALIZE.get(raw_urg, "low")

    return {
        "raw_event_id": vps_payload["raw_event_id"],
        "tg_published_at": vps_payload["tg_published_at"],
        # llm_provider Literal=["groq","ollama"]; mark DI via llm_model
        "llm_provider": "groq",
        "llm_model": y5_row.get("enrich_model") or "meta-llama/Llama-3.3-70B-Instruct",
        "llm_latency_ms": float(y5_row.get("enrich_latency_ms") or 0.0),
        "llm_input_tokens": int(y5_row.get("enrich_input_tokens") or 0),
        "llm_output_tokens": int(y5_row.get("enrich_output_tokens") or 0),
        "prompt_version": y5_row.get("enrich_prompt_version") or "1.0.0",
        "is_financial": bool(y5_row.get("is_financial") or False),
        "tickers": norm_tickers,
        "summary": (y5_row.get("summary") or "")[:300],
        "expected_timeframe": tf,
        "urgency": urg,
        "category": cat,
        "is_actionable": bool(y5_row.get("is_actionable") or False),
        "llm_raw_response": (y5_row.get("enrich_raw_response") or "")[:10000],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vps-jsonl", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "enriched_vps_window.jsonl")
    ap.add_argument("--y5-parquet", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "y5_gap_70b_v1_0_0.parquet")
    ap.add_argument("--redis-url", default="redis://127.0.0.1:6380")
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "replay" / "enriched_vps_window_di_swapped.jsonl")
    args = ap.parse_args()

    # 1. raw_event_id -> text_hash (from VPS news:raw via tunnel)
    raw_map = pull_raw_text_hashes(args.redis_url)

    # 2. text_hash -> Y5 row (from Y5 parquet)
    log.info("Loading Y5 parquet: %s", args.y5_parquet)
    y5_df = pl.read_parquet(args.y5_parquet).filter(pl.col("is_enriched"))
    log.info("  Y5 enriched rows: %d", y5_df.height)
    y5_by_hash: dict[str, dict] = {}
    for row in y5_df.iter_rows(named=True):
        th = row.get("text_hash")
        if th:
            y5_by_hash[th] = row
    log.info("  Y5 text_hash index: %d", len(y5_by_hash))

    # 3. Iterate VPS jsonl, swap payload
    n_seen = n_swapped = n_no_raw = n_no_y5 = 0
    with args.vps_jsonl.open(encoding="utf-8") as fin, \
         args.output.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            env = rec["envelope"]
            payload = env["payload"]
            raw_id = payload.get("raw_event_id")
            n_seen += 1
            th = raw_map.get(raw_id)
            if th is None:
                n_no_raw += 1
                # Keep original
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue
            y5_row = y5_by_hash.get(th)
            if y5_row is None:
                n_no_y5 += 1
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                continue
            new_payload = build_payload_from_y5(y5_row, payload)
            env["payload"] = new_payload
            rec["envelope"] = env
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n_swapped += 1

    log.info("=== swap summary ===")
    log.info("  seen:                 %d", n_seen)
    log.info("  swapped (Y5 fresh):   %d  (%.1f%%)", n_swapped, 100*n_swapped/max(n_seen,1))
    log.info("  no raw_id->text_hash: %d (kept original)", n_no_raw)
    log.info("  text_hash not in Y5:  %d (kept original)", n_no_y5)
    log.info("Wrote: %s", args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
