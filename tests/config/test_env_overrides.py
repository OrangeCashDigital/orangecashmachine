"""tests/config/test_env_overrides.py — smoke tests migrados a layers/env_override.py."""

import os
import pytest
from omegaconf import OmegaConf
from core.config.layers.env_override import apply_env_overrides


@pytest.fixture(autouse=True)
def clean_ocm_env(monkeypatch):
    """Elimina todas las OCM_* del entorno antes de cada test."""
    for key in list(os.environ):
        if key.startswith("OCM_"):
            monkeypatch.delenv(key, raising=False)


def test_env_override_applies_correctly(monkeypatch):
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__START_DATE", "2040-01-01T00:00:00Z")
    cfg = OmegaConf.create({"pipeline": {"historical": {"start_date": "2020-01-01T00:00:00Z"}}})
    result, count = apply_env_overrides(cfg)
    assert result["pipeline"]["historical"]["start_date"] == "2040-01-01T00:00:00Z"
    assert count == 1


def test_no_override_when_env_missing(monkeypatch):
    monkeypatch.delenv("OCM_PIPELINE__HISTORICAL__START_DATE", raising=False)
    cfg = OmegaConf.create({"pipeline": {"historical": {"start_date": "2020-01-01T00:00:00Z"}}})
    result, count = apply_env_overrides(cfg)
    assert result["pipeline"]["historical"]["start_date"] == "2020-01-01T00:00:00Z"
    assert count == 0


def test_nested_creation(monkeypatch):
    """Override sobre clave inexistente — OmegaConf.merge crea la estructura."""
    monkeypatch.setenv("OCM_OBSERVABILITY__METRICS__PORT", "9999")
    cfg = OmegaConf.create({})
    result, count = apply_env_overrides(cfg)
    assert result["observability"]["metrics"]["port"] == "9999"  # str — L4 coerciona a int
    assert count == 1
