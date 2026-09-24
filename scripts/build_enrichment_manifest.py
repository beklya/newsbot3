"""Sprint 6.1 — build unified manifest of all enrichments.

Scans all jsonl checkpoint files + parquet enrichment caches across newsbot3
project.  For each unique event id, records which (model, prompt_version) pairs
have enriched it, and where the artefacts live.

Verifies that file naming conventions match the enrich_prompt_version field
inside each row — flags any file that has mixed prompt versions.

Output:
    data/reenrich_phase2/enrichment_manifest.parquet
      Columns: id, models[], prompt_versions[], file_paths[]
    data/reenrich_phase2/enrichment_manifest_summary.json
      Coverage stats per (model, prompt) and any contamination detected
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("manifest")

# canonical model-id normalizer
MODEL_NORMALIZE = {
    "llama_3_3_70b_versatile": "llama-3.3-70B",
    "llama_3_3_70b": "llama-3.3-70B",
    "meta-llama/Llama-3.3-70B-Instruct": "llama-3.3-70B",
    "llama-3.3-70b-versatile": "llama-3.3-70B",
    "llama_3_1_8b_instant": "llama-3.1-8B",
    "llama_3_1_8b": "llama-3.1-8B",
    "meta-llama/Meta-Llama-3.1-8B-Instruct": "llama-3.1-8B",
    "meta-llama/Llama-3.1-8B-Instruct": "llama-3.1-8B",
    "llama-3.1-8b-instant": "llama-3.1-8B",
}


def parse_filename(path: Path) -> tuple[str | None, str | None]:
    """Infer (model, prompt_version) from filename like
    checkpoint_llama_3_3_70b_versatile_v1_0_0.jsonl"""
    name = path.stem
    if not name.startswith("checkpoint_"):
        return None, None
    rest = name[len("checkpoint_"):]
    # Match version pattern v\d_\d_\d
    m = re.search(r"_v(\d+_\d+_\d+)(?:_|$)", rest)
    if not m:
        return None, None
    version = m.group(1).replace("_", ".")
    model_token = rest[:m.start()]
    model = MODEL_NORMALIZE.get(model_token, model_token)
    return model, version


def scan_jsonl(path: Path, manifest: dict, contamination: dict,
               filename_model: str, filename_version: str) -> tuple[int, int]:
    """Walk a checkpoint jsonl.  Returns (rows_seen, rows_with_id)."""
    n_seen = n_id = 0
    inner_models: set[str] = set()
    inner_versions: set[str] = set()
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            n_seen += 1
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            eid = r.get("id")
            if not eid:
                continue
            n_id += 1
            inner_model = MODEL_NORMALIZE.get(
                r.get("enrich_model", "") or "", r.get("enrich_model", "") or ""
            )
            inner_version = r.get("enrich_prompt_version") or ""
            if inner_model:
                inner_models.add(inner_model)
            if inner_version:
                inner_versions.add(inner_version)
            manifest[eid].add((filename_model, filename_version, str(path)))
    # Contamination: file claims prompt V but row has V'
    if filename_version and inner_versions:
        if inner_versions != {filename_version}:
            contamination[str(path)] = {
                "filename_says": filename_version,
                "rows_actually": sorted(inner_versions),
            }
    if filename_model and inner_models:
        if filename_model not in inner_models and len(inner_models) > 0:
            # only warn if filename normalizes to a value that isn't in actuals
            normalized = MODEL_NORMALIZE.get(filename_model, filename_model)
            if normalized not in inner_models:
                contamination.setdefault(str(path), {})["model_mismatch"] = {
                    "filename_normalized": normalized,
                    "rows_actually": sorted(inner_models),
                }
    return n_seen, n_id


def scan_parquet(path: Path, manifest: dict) -> tuple[int, int]:
    """Some parquet files (full_70k_70b.parquet etc) also carry enrichment per id."""
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        log.warning("  cannot read parquet %s: %s", path, e)
        return 0, 0
    if "id" not in df.columns:
        return df.height, 0
    n_id = 0
    has_model = "enrich_model" in df.columns
    has_ver = "enrich_prompt_version" in df.columns
    for row in df.select([c for c in ("id", "enrich_model", "enrich_prompt_version")
                          if c in df.columns]).iter_rows(named=True):
        eid = row.get("id")
        if not eid:
            continue
        n_id += 1
        m = MODEL_NORMALIZE.get(row.get("enrich_model", "") or "",
                                row.get("enrich_model", "") or "")
        v = row.get("enrich_prompt_version") or ""
        if m or v:
            manifest[eid].add((m or "?", v or "?", str(path)))
    return df.height, n_id


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path,
                    default=PROJECT_ROOT)
    ap.add_argument("--output", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" /
                             "enrichment_manifest.parquet")
    ap.add_argument("--summary", type=Path,
                    default=PROJECT_ROOT / "data" / "reenrich_phase2" /
                             "enrichment_manifest_summary.json")
    args = ap.parse_args()

    manifest: dict[str, set] = defaultdict(set)  # id -> {(model, version, file)}
    contamination: dict[str, dict] = {}

    log.info("Scanning jsonl checkpoints...")
    jsonl_files = list(args.root.rglob("checkpoint_*.jsonl"))
    log.info("  %d candidate checkpoint files", len(jsonl_files))
    for p in sorted(jsonl_files):
        rel = p.relative_to(args.root)
        model, version = parse_filename(p)
        n_seen, n_id = scan_jsonl(p, manifest, contamination,
                                    model or "?", version or "?")
        log.info("  %-90s model=%-15s ver=%-7s rows=%d ids=%d",
                 str(rel)[:90], model or "?", version or "?", n_seen, n_id)

    log.info("Scanning known parquet enrichments...")
    parquet_candidates = [
        args.root / "data" / "reenrich_phase2" / "full_70k_70b.parquet",
        args.root / "data" / "reenrich_phase2" / "fold13_rolling_12mo_70b.parquet",
    ]
    for p in parquet_candidates:
        if p.exists():
            n_rows, n_id = scan_parquet(p, manifest)
            log.info("  %-90s rows=%d ids=%d",
                     str(p.relative_to(args.root))[:90], n_rows, n_id)

    log.info("")
    log.info("=== Coverage summary ===")
    by_pair: dict[tuple[str, str], set[str]] = defaultdict(set)
    for eid, entries in manifest.items():
        for (m, v, _f) in entries:
            by_pair[(m, v)].add(eid)
    log.info("(model, prompt_version)  unique_ids")
    for (m, v), ids in sorted(by_pair.items()):
        log.info("  %-20s %-8s  %d", m, v, len(ids))
    log.info("Total unique IDs across all enrichments: %d", len(manifest))

    if contamination:
        log.warning("")
        log.warning("=== Contamination detected (filename ≠ row content) ===")
        for fp, det in contamination.items():
            log.warning("  %s: %s", fp, det)
    else:
        log.info("")
        log.info("No filename↔content prompt-version contamination detected ✓")

    # Cross-model coverage — events enriched by both 70B and 8B
    by_id_models: dict[str, set[str]] = defaultdict(set)
    by_id_versions: dict[str, set[str]] = defaultdict(set)
    for eid, entries in manifest.items():
        for (m, v, _f) in entries:
            by_id_models[eid].add(m)
            by_id_versions[eid].add(v)
    multi_model = sum(1 for s in by_id_models.values() if len(s) > 1)
    multi_version = sum(1 for s in by_id_versions.values() if len(s) > 1)
    log.info("")
    log.info("Cross-enrichment:")
    log.info("  events enriched by 2+ models:    %d", multi_model)
    log.info("  events enriched on 2+ prompt vrs: %d", multi_version)

    # Write parquet manifest
    rows = []
    for eid, entries in manifest.items():
        models = sorted({m for (m, _v, _f) in entries})
        versions = sorted({v for (_m, v, _f) in entries})
        files = sorted({f for (_m, _v, f) in entries})
        rows.append({
            "id": eid,
            "models": models,
            "prompt_versions": versions,
            "files": files,
            "n_artifacts": len(entries),
        })
    df = pl.DataFrame(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.output)
    log.info("")
    log.info("Wrote manifest: %s (%d rows)", args.output, df.height)

    # Summary JSON
    summary = {
        "total_unique_ids": len(manifest),
        "by_model_prompt": {f"{m}|{v}": len(ids)
                             for (m, v), ids in sorted(by_pair.items())},
        "events_with_2plus_models": multi_model,
        "events_with_2plus_prompts": multi_version,
        "contamination": contamination,
    }
    args.summary.write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                              encoding="utf-8")
    log.info("Summary: %s", args.summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
