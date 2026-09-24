r"""Sprint 6.1 — combine Phase 2 corpus + Y6 new events into v2 train parquet.

Inputs:
  - data/reenrich_phase2/features_mfe_70b_ext.parquet  (Phase 2 corpus, 70k × 81)
  - data/reenrich_phase2/features_mfe_y6_70b_ext.parquet (Y6 new, 67k × 81)
  - newsbot2/.../targets_mfe.parquet  (Phase 2 targets, ~70k × 45)
  - data/reenrich_phase2/targets_mfe_y6.parquet (Y6 targets, 67k × 13)

Outputs:
  - data/reenrich_phase2/features_mfe_v2.parquet  (concatenated + deduped)
  - data/reenrich_phase2/targets_mfe_v2.parquet   (concatenated + deduped)

Dedup: when (_id, _ticker) overlaps between Phase 2 and Y6, KEEP Phase 2
version (the curated one with multiple horizons in targets).
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("combine")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase2-features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_70b_ext.parquet")
    ap.add_argument("--y6-features", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_y6_70b_ext.parquet")
    ap.add_argument("--phase2-targets", type=Path,
                    default=Path(r"D:\quik_sber\newsbot\newsbot2\решение проблем"
                                  r"\Проблема 5 - новое начало\phase2_mfe\targets_mfe.parquet"))
    ap.add_argument("--y6-targets", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_y6.parquet")
    ap.add_argument("--features-out", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "features_mfe_v2.parquet")
    ap.add_argument("--targets-out", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" / "targets_mfe_v2.parquet")
    args = ap.parse_args()

    # === Features ===
    log.info("Loading Phase 2 features: %s", args.phase2_features)
    p2_feat = pl.read_parquet(args.phase2_features)
    log.info("  %d rows × %d cols", p2_feat.height, p2_feat.width)

    log.info("Loading Y6 features: %s", args.y6_features)
    y6_feat = pl.read_parquet(args.y6_features)
    log.info("  %d rows × %d cols", y6_feat.height, y6_feat.width)

    # Align columns — both have same schema after rebuild_features_with_70b_ext +
    # build_features_for_y6_corpus. Use Phase 2 column order as authoritative.
    common_cols = [c for c in p2_feat.columns if c in y6_feat.columns]
    p2_only = [c for c in p2_feat.columns if c not in y6_feat.columns]
    y6_only = [c for c in y6_feat.columns if c not in p2_feat.columns]
    if p2_only:
        log.warning("  Phase 2 has cols not in Y6 (filled with 0): %s", p2_only)
    if y6_only:
        log.warning("  Y6 has cols not in Phase 2 (dropped): %s", y6_only)

    p2_subset = p2_feat.select(common_cols)
    y6_subset = y6_feat.select(common_cols)
    # Cast _id to string in both
    p2_subset = p2_subset.with_columns(pl.col("_id").cast(pl.Utf8))
    y6_subset = y6_subset.with_columns(pl.col("_id").cast(pl.Utf8))

    # Concat — Phase 2 FIRST so dedup keeps it
    merged_feat = pl.concat([p2_subset, y6_subset], how="vertical_relaxed")
    log.info("  pre-dedup: %d rows", merged_feat.height)
    # Dedup on (_id, _ticker)
    merged_feat = merged_feat.unique(subset=["_id", "_ticker"], keep="first")
    log.info("  post-dedup: %d rows", merged_feat.height)

    args.features_out.parent.mkdir(parents=True, exist_ok=True)
    merged_feat.write_parquet(args.features_out)
    log.info("Saved %s (%d rows × %d cols)", args.features_out,
             merged_feat.height, merged_feat.width)

    # === Targets ===
    log.info("")
    log.info("Loading Phase 2 targets: %s", args.phase2_targets)
    p2_tgt = pl.read_parquet(args.phase2_targets)
    log.info("  %d rows × %d cols", p2_tgt.height, p2_tgt.width)
    log.info("Loading Y6 targets: %s", args.y6_targets)
    y6_tgt = pl.read_parquet(args.y6_targets)
    log.info("  %d rows × %d cols", y6_tgt.height, y6_tgt.width)

    # Y6 targets have id/datetime/ticker meta + 30m/60m mfe/mae.  Phase 2 has more horizons.
    # Use Y6 column subset (id/datetime/ticker + 30m/60m × {mfe,mae} × {long,short})
    tgt_cols = ["id", "datetime", "ticker",
                "mfe_long_30m", "mae_long_30m", "mfe_short_30m", "mae_short_30m",
                "mfe_long_60m", "mae_long_60m", "mfe_short_60m", "mae_short_60m",
                "_entry_price", "_entry_ts"]
    p2_tgt_keep = [c for c in tgt_cols if c in p2_tgt.columns]
    y6_tgt_keep = [c for c in tgt_cols if c in y6_tgt.columns]
    log.info("  Phase 2 target cols kept: %d", len(p2_tgt_keep))
    log.info("  Y6 target cols kept:      %d", len(y6_tgt_keep))

    p2_tgt_sub = p2_tgt.select(p2_tgt_keep).with_columns(pl.col("id").cast(pl.Utf8))
    y6_tgt_sub = y6_tgt.select(y6_tgt_keep).with_columns(pl.col("id").cast(pl.Utf8))

    merged_tgt = pl.concat([p2_tgt_sub, y6_tgt_sub], how="vertical_relaxed")
    log.info("  pre-dedup: %d rows", merged_tgt.height)
    merged_tgt = merged_tgt.unique(subset=["id", "ticker"], keep="first")
    log.info("  post-dedup: %d rows", merged_tgt.height)

    args.targets_out.parent.mkdir(parents=True, exist_ok=True)
    merged_tgt.write_parquet(args.targets_out)
    log.info("Saved %s (%d rows × %d cols)", args.targets_out,
             merged_tgt.height, merged_tgt.width)
    return 0


if __name__ == "__main__":
    sys.exit(main())
