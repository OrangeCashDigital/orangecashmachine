# -*- coding: utf-8 -*-
"""
tests/app/test_cfg_job_guard.py
=================================

Guard R4 / B-04 (H-06): `--cfg` (Hydra dump de config) queda bloqueado en el
entorno de producción — Hydra imprime el DictConfig OmegaConf pre-Pydantic SIN
redactar SecretStr, exponiendo credenciales a stdout.
"""

from __future__ import annotations

import pytest
from app.cli.main import _reject_cfg_job_in_production


def test_rejects_cfg_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["ocm", "--cfg", "job"])
    monkeypatch.setenv("OCM_ENV", "production")
    with pytest.raises(SystemExit) as e:
        _reject_cfg_job_in_production()
    assert e.value.code == 2


def test_allows_cfg_in_development(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["ocm", "--cfg", "job"])
    monkeypatch.delenv("OCM_ENV", raising=False)  # defaults to development
    _reject_cfg_job_in_production()  # no debe lanzar


def test_allows_normal_run_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["ocm", "pipeline=ohlcv"])
    monkeypatch.setenv("OCM_ENV", "production")
    _reject_cfg_job_in_production()  # sin --cfg, nunca bloquea
