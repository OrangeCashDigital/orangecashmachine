"""
tests/core/config/test_snapshot_contract.py
============================================

Contrato de integración: load_appconfig_from_hydra y load_appconfig_standalone
deben comportarse consistentemente respecto a write_snapshot + run_id.

Principio: "Same config → same behavior" — ambos flows deben tener
las mismas garantías de auditoría o fallar de forma idéntica y explícita.
"""
from __future__ import annotations

import pytest
from omegaconf import OmegaConf
from unittest.mock import patch, MagicMock

from ocm_platform.config.hydra_loader import load_appconfig_from_hydra


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_cfg():
    """DictConfig mínimo que pasa L1-L5 del pipeline."""
    return OmegaConf.create({
        "exchanges": {"bybit": {"enabled": True, "api_key": "test", "api_secret": "test"}},
        "pipeline": {
            "historical": {"start_date": "auto", "timeframes": ["1m"]},
            "resample":   {"targets": ["5m"], "source_tf": "1m"},
            "realtime":   {},
        },
        "environment": {"name": "test"},
        "safety": {"dry_run": True},
    })


# ---------------------------------------------------------------------------
# Contrato fail-fast: write_snapshot=True sin run_id → ValueError inmediato
# ---------------------------------------------------------------------------

def test_write_snapshot_without_run_id_raises(minimal_cfg):
    """write_snapshot=True sin run_id debe fallar de forma explícita.

    Contrato: nunca omitir snapshot silenciosamente cuando se pidió auditoría.
    """
    with pytest.raises(ValueError, match="write_snapshot=True requiere run_id"):
        load_appconfig_from_hydra(minimal_cfg, env="test", write_snapshot=True)


# ---------------------------------------------------------------------------
# Contrato correcto: write_snapshot=False sin run_id → ok (deliberado)
# ---------------------------------------------------------------------------

def test_write_snapshot_false_without_run_id_ok(minimal_cfg):
    """write_snapshot=False + run_id=None es intencional — no debe lanzar."""
    config = load_appconfig_from_hydra(minimal_cfg, env="test", write_snapshot=False)
    assert config is not None


# ---------------------------------------------------------------------------
# Contrato correcto: write_snapshot=True con run_id → snapshot escrito
# ---------------------------------------------------------------------------

def test_write_snapshot_with_run_id_calls_writer(minimal_cfg):
    """write_snapshot=True + run_id válido debe llamar write_config_snapshot."""
    with patch("core.config.hydra_loader.write_config_snapshot") as mock_write:
        load_appconfig_from_hydra(
            minimal_cfg,
            env="test",
            run_id="test-run-001",
            write_snapshot=True,
        )
        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args
        assert call_kwargs.kwargs["run_id"] == "test-run-001"
        assert call_kwargs.kwargs["env"] == "test"


# ---------------------------------------------------------------------------
# Contrato standalone: run_id propagado → mismo comportamiento que hydra path
# ---------------------------------------------------------------------------

def test_standalone_propagates_run_id_to_snapshot(tmp_path, minimal_cfg):
    """load_appconfig_standalone con run_id debe propagar al snapshot writer.

    Verifica que el standalone path tiene el mismo contrato de auditoría
    que el hydra path — sin configuration drift entre flows.
    """
    import yaml
    from ocm_platform.config.hydra_loader import load_appconfig_standalone

    # Construir config_dir mínimo en tmp_path
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "base.yaml").write_text(yaml.dump({
        "exchanges": {"bybit": {"enabled": True, "api_key": "test", "api_secret": "test"}},
        "pipeline": {
            "historical": {"start_date": "auto", "timeframes": ["1m"]},
            "resample":   {"targets": ["5m"], "source_tf": "1m"},
            "realtime":   {},
        },
        "environment": {"name": "test"},
        "safety": {"dry_run": True},
    }))
    (config_dir / "env").mkdir()

    with patch("core.config.hydra_loader.write_config_snapshot") as mock_write:
        load_appconfig_standalone(
            env="test",
            config_dir=config_dir,
            run_id="standalone-run-001",
            write_snapshot=True,
        )
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs["run_id"] == "standalone-run-001"
