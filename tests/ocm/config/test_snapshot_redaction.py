# -*- coding: utf-8 -*-
"""
tests/ocm/config/test_snapshot_redaction.py
============================================

Guard R4 / B-04 (H-06): el snapshot de config JAMÁS contiene el valor de un
SecretStr. Pydantic v2 ya redacta SecretStr en model_dump(mode="json"); este
test fija ese contrato para que una regresión (campo tipado como str, versión
de pydantic, etc.) rompa en CI.
"""

from __future__ import annotations

from omegaconf import OmegaConf

from ocm.config.hydra_loader import load_appconfig_from_hydra
from ocm.config.loader.snapshot import write_config_snapshot

_SECRET = "super-secret-api-value-9911"


def _minimal_cfg() -> OmegaConf:
    return OmegaConf.create(
        {
            "exchanges": {
                "bybit": {
                    "enabled": True,
                    "api_key": "pub-key",
                    "api_secret": _SECRET,
                    "api_password": "pwd-secret-5522",
                }
            },
            "pipeline": {"historical": {"start_date": "auto", "timeframes": ["1m"]}},
            "environment": {"name": "test"},
            "safety": {"dry_run": True},
        }
    )


def _write_snapshot(cfg, tmp_path):
    config = load_appconfig_from_hydra(cfg, env="test", run_id="r", write_snapshot=False)
    run_id = "redact-run-001"
    path = write_config_snapshot(
        config,
        run_id=run_id,
        config_hash="a" * 32,
        env="test",
        snapshot_dir=tmp_path,
    )
    assert path is not None
    return config, path


def test_snapshot_does_not_contain_secret_value(tmp_path) -> None:
    _, path = _write_snapshot(_minimal_cfg(), tmp_path)
    text = path.read_text()
    assert _SECRET not in text, "el valor del api_secret NO debe aparecer en el snapshot"


def test_snapshot_preserves_non_secret_fields(tmp_path) -> None:
    _, path = _write_snapshot(_minimal_cfg(), tmp_path)
    assert "bybit" in path.read_text()
