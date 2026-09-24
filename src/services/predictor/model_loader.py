"""Load 16 XGBoost models + feature_order from disk on startup."""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import joblib

log = logging.getLogger(__name__)

HORIZONS = ["30m", "60m"]
TARGETS = ["mfe_long", "mae_long", "mfe_short", "mae_short"]
MODEL_TYPES = ["general", "mx_specific"]


class ModelBundle:
    """Holds all loaded models keyed by (horizon, target, model_type)."""

    def __init__(self) -> None:
        self.models: Dict[Tuple[str, str, str], object] = {}
        self.feature_order: List[str] = []
        self.fingerprint: str = ""

    def horizons(self) -> List[str]:
        return HORIZONS

    def get(self, horizon: str, target: str, model_type: str):
        return self.models.get((horizon, target, model_type))


def load_bundle(models_dir: Path) -> ModelBundle:
    """Load feature_order.json and 16 joblib models. Fail fast if any missing."""
    bundle = ModelBundle()

    feature_order_path = models_dir / "feature_order.json"
    if not feature_order_path.exists():
        raise FileNotFoundError(
            f"feature_order.json not found at {feature_order_path}\n"
            f"Run: python scripts/train_predictor_fold13.py"
        )
    bundle.feature_order = json.loads(feature_order_path.read_text(encoding="utf-8"))
    log.info("Loaded feature_order: %d features", len(bundle.feature_order))

    fingerprint_parts: List[str] = []
    for horizon_min in (30, 60):
        horizon = f"{horizon_min}m"
        for target in TARGETS:
            for model_type in MODEL_TYPES:
                path = models_dir / f"{target}_{horizon}_{model_type}.joblib"
                if not path.exists():
                    raise FileNotFoundError(
                        f"Model missing: {path.name}\n"
                        f"Expected 16 files in {models_dir}; "
                        f"run scripts/train_predictor_fold13.py"
                    )
                model = joblib.load(path)
                bundle.models[(horizon, target, model_type)] = model
                # Hash file content for fingerprint
                with open(path, "rb") as fp:
                    fingerprint_parts.append(hashlib.sha256(fp.read()).hexdigest()[:8])
                log.info("  loaded %s", path.name)

    bundle.fingerprint = hashlib.sha256(
        "|".join(fingerprint_parts).encode("utf-8")
    ).hexdigest()[:16]
    log.info(
        "ModelBundle ready: %d models, feature_count=%d, fingerprint=%s",
        len(bundle.models), len(bundle.feature_order), bundle.fingerprint,
    )
    return bundle
