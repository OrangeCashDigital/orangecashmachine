"""
tests/architecture/test_deploy_artifacts.py — Validación de artefactos de deploy (B-57/B-59).

Verifica que la solución de deploy verificado (ADR-0037) y la unidad systemd del
streaming (ADR-0022) existen y son sintácticamente válidas:

  - deploy/scripts/deploy_ocm.sh existe, es ejecutable y pasa `bash -n`
  - deploy/systemd/ocm-streaming.service existe y su sintaxis es válida
    (systemd-analyze verify — evidencia mecánica real, no simulación)
  - la unit referencia el entrypoint `streaming` registrado en pyproject
  - .github/workflows/ocm-cd.yml ya no es un placeholder
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
DEPLOY_SH = ROOT / "deploy" / "scripts" / "deploy_ocm.sh"
SYSTEMD_UNIT = ROOT / "deploy" / "systemd" / "ocm-streaming.service"
CD_WORKFLOW = ROOT / ".github" / "workflows" / "ocm-cd.yml"
PYPROJECT = ROOT / "pyproject.toml"


def test_deploy_script_exists_and_executable() -> None:
    assert DEPLOY_SH.is_file(), "deploy_ocm.sh debe existir (B-57)"
    assert DEPLOY_SH.stat().st_mode & 0o111, "deploy_ocm.sh debe ser ejecutable"


def test_deploy_script_bash_n_passes() -> None:
    res = subprocess.run(
        ["bash", "-n", str(DEPLOY_SH)],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"bash -n falló:\n{res.stderr}"


def test_deploy_script_has_health_rollback_modes() -> None:
    text = DEPLOY_SH.read_text()
    assert "--check-health" in text
    assert "--deploy" in text
    assert "--rollback" in text
    assert "ACCEPT" in text
    assert "REJECT" in text


def test_systemd_unit_exists() -> None:
    assert SYSTEMD_UNIT.is_file(), "ocm-streaming.service debe existir (B-59/ADR-0022)"


def test_systemd_unit_references_streaming_entrypoint() -> None:
    unit = SYSTEMD_UNIT.read_text()
    pyproject = PYPROJECT.read_text()
    assert "streaming" in unit, "la unit debe referenciar el proceso streaming"
    assert "streaming = " in pyproject, "pyproject debe registrar el entrypoint streaming"
    assert "streaming_hydra" in pyproject


@pytest.mark.skipif(
    not shutil.which("systemd-analyze"),
    reason="systemd-analyze no disponible en el runner",
)
def test_systemd_unit_verify_syntax() -> None:
    res = subprocess.run(
        ["systemd-analyze", "verify", str(SYSTEMD_UNIT)],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, (
        f"systemd-analyze verify falló:\n{res.stdout}\n{res.stderr}"
    )


def test_cd_workflow_no_longer_placeholder() -> None:
    text = CD_WORKFLOW.read_text()
    assert "workflow_dispatch" in text
    assert "deploy_ocm.sh" in text
    assert "placeholder" not in text.lower(), "ocm-cd.yml ya no debe ser placeholder"