"""tests/config/test_env_overrides.py — smoke tests con env= explícito (sin acoplamiento global)."""

from omegaconf import OmegaConf
from ocm_platform.config.layers.env_override import apply_env_overrides


def test_env_override_applies_correctly():
    cfg = OmegaConf.create({"pipeline": {"historical": {"start_date": "2020-01-01T00:00:00Z"}}})
    result, count = apply_env_overrides(
        cfg,
        env={"OCM_PIPELINE__HISTORICAL__START_DATE": "2040-01-01T00:00:00Z"},
    )
    assert result["pipeline"]["historical"]["start_date"] == "2040-01-01T00:00:00Z"
    assert count == 1


def test_no_override_when_env_empty():
    cfg = OmegaConf.create({"pipeline": {"historical": {"start_date": "2020-01-01T00:00:00Z"}}})
    result, count = apply_env_overrides(cfg, env={})
    assert result["pipeline"]["historical"]["start_date"] == "2020-01-01T00:00:00Z"
    assert count == 0


def test_nested_creation():
    """Override sobre clave inexistente — OmegaConf.merge crea la estructura."""
    cfg = OmegaConf.create({})
    result, count = apply_env_overrides(cfg, env={"OCM_OBSERVABILITY__METRICS__PORT": "9999"})
    assert result["observability"]["metrics"]["port"] == "9999"  # str — L4 coerciona a int
    assert count == 1
