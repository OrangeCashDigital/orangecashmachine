"""tests/core/config/loader/test_env_overrides.py

Tests migrados a layers/env_override.py (nueva API).

API nueva:
    apply_env_overrides(DictConfig) -> (DictConfig, int)

Cambios respecto a versión anterior:
    - Input: DictConfig (OmegaConf), no plain dict
    - Output: (DictConfig, int) — acceso via result[0]["key"]
    - Coerción int/float eliminada de L2 — responsabilidad de Pydantic L4.
      test_max_concurrent_coercion eliminado: "8" permanece str aquí;
      Pydantic lo coerciona a int al construir AppConfig (SSOT del schema).
"""

import pytest
from omegaconf import OmegaConf
from core.config.layers.env_override import apply_env_overrides


@pytest.fixture(autouse=True)
def clean_ocm_env(monkeypatch):
    """Elimina todas las OCM_* del entorno antes de cada test.

    Evita que variables del shell (e.g. OCM_STORAGE__DATA_LAKE__PATH)
    contaminen el count de overrides y produzcan falsos negativos.
    """
    import os
    for key in list(os.environ):
        if key.startswith("OCM_"):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture
def base_cfg():
    return OmegaConf.create({
        "pipeline": {
            "historical": {
                "fetch_all_history": False,
                "start_date": "2024-01-01T00:00:00Z",
                "max_concurrent_tasks": 2,
            },
            "realtime": {
                "snapshot_interval_seconds": 60,
            },
        },
        "observability": {
            "logging": {"level": "INFO"},
        },
    })


def test_fetch_all_history_override(monkeypatch, base_cfg):
    """OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY=true debe pisar el False del YAML."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY", "true")
    result, count = apply_env_overrides(base_cfg)
    assert result["pipeline"]["historical"]["fetch_all_history"] is True
    assert count == 1


def test_fetch_all_history_false(monkeypatch, base_cfg):
    """OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY=false debe mantener False."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__FETCH_ALL_HISTORY", "false")
    result, count = apply_env_overrides(base_cfg)
    assert result["pipeline"]["historical"]["fetch_all_history"] is False
    assert count == 1


def test_start_date_override(monkeypatch, base_cfg):
    """OCM_PIPELINE__HISTORICAL__START_DATE debe pisar el start_date del YAML."""
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__START_DATE", "2017-01-01T00:00:00Z")
    result, count = apply_env_overrides(base_cfg)
    assert result["pipeline"]["historical"]["start_date"] == "2017-01-01T00:00:00Z"
    assert count == 1


def test_int_stays_str_in_l2(monkeypatch, base_cfg):
    """L2 NO coerciona int/float — responsabilidad exclusiva de Pydantic L4.

    "8" permanece str en el DictConfig. AppConfig (L4) lo convierte a int
    al construir el modelo. Este test documenta el contrato explícitamente.
    """
    monkeypatch.setenv("OCM_PIPELINE__HISTORICAL__MAX_CONCURRENT_TASKS", "8")
    result, count = apply_env_overrides(base_cfg)
    assert result["pipeline"]["historical"]["max_concurrent_tasks"] == "8"
    assert isinstance(result["pipeline"]["historical"]["max_concurrent_tasks"], str)
    assert count == 1


def test_no_overrides_leaves_config_intact(base_cfg):
    """Sin env vars OCM_*, el config no debe cambiar y count=0."""
    result, count = apply_env_overrides(base_cfg)
    assert result["pipeline"]["historical"]["fetch_all_history"] is False
    assert count == 0


def test_log_level_override(monkeypatch, base_cfg):
    """OCM_OBSERVABILITY__LOGGING__LEVEL debe pisar el nivel de logging."""
    monkeypatch.setenv("OCM_OBSERVABILITY__LOGGING__LEVEL", "DEBUG")
    result, count = apply_env_overrides(base_cfg)
    assert result["observability"]["logging"]["level"] == "DEBUG"
    assert count == 1


def test_malformed_key_skipped(monkeypatch, base_cfg):
    """OCM_FOO (sin __) debe ser ignorado con warning, no romper el pipeline."""
    monkeypatch.setenv("OCM_FOO", "bar")
    result, count = apply_env_overrides(base_cfg)
    assert count == 0  # no aplicado
