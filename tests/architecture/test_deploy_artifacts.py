"""
tests/architecture/test_deploy_artifacts.py — Validación de artefactos de deploy (B-57/B-58/B-59).

Verifica que la solución de deploy verificado (ADR-0037), el provisioning de
Grafana reproducible (B-58) y la unidad systemd del streaming (ADR-0022)
existen y son sintácticamente válidos:

  - deploy/scripts/deploy_ocm.sh existe, es ejecutable y pasa `bash -n`
  - deploy/systemd/ocm-streaming.service existe y su sintaxis es válida
    (systemd-analyze verify — evidencia mecánica real, no simulación)
  - la unit referencia el entrypoint `streaming` registrado en pyproject
  - .github/workflows/ocm-cd.yml ya no es un placeholder
  - el provisioning de Grafana (datasource + provider + dashboard) es válido,
    coherente con docker-compose.yml y usa métricas OCM reales
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
DEPLOY_SH = ROOT / "deploy" / "scripts" / "deploy_ocm.sh"
SYSTEMD_UNIT = ROOT / "deploy" / "systemd" / "ocm-streaming.service"
CD_WORKFLOW = ROOT / ".github" / "workflows" / "ocm-cd.yml"
PYPROJECT = ROOT / "pyproject.toml"
GRAFANA_ROOT = ROOT / "deploy" / "monitoring" / "grafana"
COMPOSE = ROOT / "docker-compose.yml"


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


def test_deploy_script_has_artifact_verify_mode() -> None:
    # B-59: build → digest → publish → verify → deploy (ADR-0037).
    text = DEPLOY_SH.read_text()
    assert "--verify-artifact" in text, "deploy_ocm.sh debe exponer --verify-artifact (B-59)"
    assert "docker image inspect" in text, "debe usar config digest (determinista)"
    assert "refusing deploy" in text, "debe bloquear el deploy si el digest no coincide"


def test_deploy_script_artifact_verify_positive_negative() -> None:
    # Mecanismo real de verificación de digest: imagen local vs digest esperado.
    # Positivo: digest coincide → ACCEPT. Negativo: digest distinto → REJECT.
    # Skip si no hay docker o la imagen de test no existe.
    res = subprocess.run(["docker", "version"], capture_output=True, text=True)
    if res.returncode != 0:
        pytest.skip("docker no disponible")
    if subprocess.run(
        ["docker", "image", "inspect", "ocm_market_data:latest"],
        capture_output=True,
    ).returncode != 0:
        pytest.skip("imagen ocm_market_data:latest no construida localmente")

    digest = subprocess.run(
        ["docker", "image", "inspect", "ocm_market_data:latest", "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    with tempfile.TemporaryDirectory() as tmp:
        good = Path(tmp) / "good.digest"
        good.write_text(digest)
        ok = subprocess.run(
            [str(DEPLOY_SH), "--verify-artifact", str(good)],
            capture_output=True,
            text=True,
        )
        assert ok.returncode == 0, f"digest correcto debe ACCEPT:\n{ok.stdout}\n{ok.stderr}"

        bad = Path(tmp) / "bad.digest"
        bad.write_text("sha256:" + "0" * 64)
        rej = subprocess.run(
            [str(DEPLOY_SH), "--verify-artifact", str(bad)],
            capture_output=True,
            text=True,
        )
        assert rej.returncode == 1, "digest incorrecto debe REJECT (exit 1)"


def test_dockerfile_builds_with_valid_uv_flag() -> None:
    # B-59: el Dockerfile de producción no debe usar flags de uv inexistentes.
    # Verificado 2026-08-19: `uv sync --no-dev --system` falla en uv 0.11.14
    # (flag --system removida). El Dockerfile debe usar UV_PROJECT_ENVIRONMENT.
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert "--system" not in dockerfile, (
        "Dockerfile no debe usar `uv sync --system` (flag removida en uv 0.11.14)"
    )
    assert "UV_PROJECT_ENVIRONMENT=/usr/local" in dockerfile, (
        "Dockerfile debe instalar deps en /usr/local via UV_PROJECT_ENVIRONMENT"
    )
    assert "COPY packages ./packages" in dockerfile, (
        "Dockerfile debe copiar packages/ para el remap editable de hatchling"
    )
    assert "COPY ocm ./ocm" in dockerfile
    assert "COPY shared ./shared" in dockerfile
    assert "COPY apps ./apps" in dockerfile
    assert "COPY pyproject.toml README.md ." in dockerfile


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


def test_grafana_datasource_declarative() -> None:
    ds = GRAFANA_ROOT / "provisioning" / "datasources" / "ocm-prometheus.yaml"
    assert ds.is_file(), "datasource declarativo debe existir (B-58)"
    text = ds.read_text()
    assert "prometheus" in text
    assert "http://prometheus:9090" in text


def test_grafana_dashboard_provider_declarative() -> None:
    provider = GRAFANA_ROOT / "provisioning" / "dashboards" / "ocm-provider.yaml"
    assert provider.is_file(), "provider declarativo debe existir (B-58)"
    text = provider.read_text()
    assert "/var/lib/grafana/dashboards" in text, "path del provider debe coincidir con el mount"


def test_grafana_dashboard_json_valid_and_real_metrics() -> None:
    dash = GRAFANA_ROOT / "dashboards" / "ocm_pipeline.json"
    assert dash.is_file(), "dashboard ocm_pipeline.json debe existir (B-58)"
    data = json.loads(dash.read_text())
    assert data["title"] == "OCM Pipeline"
    exprs = [
        t["expr"]
        for panel in data["panels"]
        for t in panel.get("targets", [])
    ]
    assert any("ocm_pipeline_last_run_timestamp" in e for e in exprs)
    assert any("ocm_kafka_events_published_total" in e for e in exprs)
    assert any("ocm_silver_freshness_seconds" in e for e in exprs)


def test_grafana_compose_mounts_match_versioned_files() -> None:
    compose = COMPOSE.read_text()
    assert "./deploy/monitoring/grafana/provisioning:/etc/grafana/provisioning" in compose
    assert "./deploy/monitoring/grafana/dashboards:/var/lib/grafana/dashboards" in compose
    assert "ocm_pipeline.json" in compose, "home dashboard del compose debe existir versionado"


def test_grafana_provisioning_not_gitignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text()
    assert "deploy/monitoring/grafana/provisioning/" not in gitignore
    assert "deploy/monitoring/grafana/dashboards/" not in gitignore