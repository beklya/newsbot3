"""Tests for model_loader.load_bundle — real on-disk artifacts."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.services.predictor.model_loader import HORIZONS, MODEL_TYPES, TARGETS, load_bundle


def test_load_bundle_real():
    project_root = Path(__file__).resolve().parents[3]
    bundle = load_bundle(project_root / "data" / "models" / "predictor" / "v1")
    assert len(bundle.feature_order) == 67
    assert len(bundle.models) == 16
    assert len(bundle.fingerprint) == 16  # SHA256[:16]

    expected_keys = {
        (h, t, m) for h in HORIZONS for t in TARGETS for m in MODEL_TYPES
    }
    assert set(bundle.models.keys()) == expected_keys


def test_load_bundle_missing_dir(tmp_path: Path):
    with pytest.raises(FileNotFoundError, match="feature_order.json"):
        load_bundle(tmp_path)


def test_load_bundle_partial(tmp_path: Path):
    """Если есть feature_order но нет моделей — fail fast."""
    (tmp_path / "feature_order.json").write_text("[]", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="Model missing"):
        load_bundle(tmp_path)
