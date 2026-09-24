r"""Sprint 6.1 — filter v2 corpus to is_actionable=True events only.

Inner-join features_mfe_v2.parquet + targets_mfe_v2.parquet with the enrichment
corpus on `_id`, keeping only events where is_actionable=True.

Output:
    features_mfe_v2_actionable.parquet
    targets_mfe_v2_actionable.parquet
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("filter_actionable")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2.parquet")
    ap.add_argument("--targets", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2.parquet")
    ap.add_argument("--phase2-enriched", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "full_70k_70b.parquet")
    ap.add_argument("--y6-enriched", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "y6_corpus_70b.parquet")
    ap.add_argument("--features-out", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2_actionable.parquet")
    ap.add_argument("--targets-out", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2_actionable.parquet")
    args = ap.parse_args()

    log.info("Building actionable_ids set from BOTH enrichment parquets...")
    actionable_ids: set[str] = set()
    for path in (args.phase2_enriched, args.y6_enriched):
        if not path.exists():
            log.warning("  missing: %s", path)
            continue
        df = pl.read_parquet(path).filter(pl.col("is_enriched") & pl.col("is_actionable"))
        ids = set(df["id"].cast(pl.Utf8).to_list())
        log.info("  %s -> %d actionable", path.name, len(ids))
        actionable_ids |= ids
    log.info("  TOTAL actionable IDs: %d", len(actionable_ids))

    log.info("Loading features: %s", args.features)
    feat = pl.read_parquet(args.features).with_columns(pl.col("_id").cast(pl.Utf8))
    log.info("  pre-filter: %d rows", feat.height)
    feat = feat.filter(pl.col("_id").is_in(actionable_ids))
    log.info("  post-filter: %d rows (%.1f%%)", feat.height,
             100*feat.height/max(1, len(actionable_ids)))

    log.info("Loading targets: %s", args.targets)
    tgt = pl.read_parquet(args.targets).with_columns(pl.col("id").cast(pl.Utf8))
    log.info("  pre-filter: %d rows", tgt.height)
    tgt = tgt.filter(pl.col("id").is_in(actionable_ids))
    log.info("  post-filter: %d rows", tgt.height)

    args.features_out.parent.mkdir(parents=True, exist_ok=True)
    feat.write_parquet(args.features_out)
    log.info("Saved %s", args.features_out)
    tgt.write_parquet(args.targets_out)
    log.info("Saved %s", args.targets_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
