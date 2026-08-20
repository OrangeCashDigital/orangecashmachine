"""
tests/architecture/test_check_production_gates.py — Production Gate binario (ADR-0033).

Verifica que el gate G1..G11 reporta veredicto determinista y honesto:

  - gate-dev (estado actual): PASS (G10 excluida, B-15 EN_CURSO)
  - gate-release: BLOCK (G10 obligatoria, B-15 EN_CURSO)
  - cada cheque reporta id/name/status/threshold/actual/evidence
  - los subprocesos pesados (G5..G8) se inyectan como fake determinista
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = ROOT / "scripts" / "check_production_gates.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("check_production_gates", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


cg = _load_module()


@pytest.fixture()
def fake_run(monkeypatch: pytest.MonkeyPatch) -> subprocess.CompletedProcess:
    """Inyecta un RUN que simula gates de subproceso exitosos (G5..G8, G12)."""

    def fake(cmd: list[str]) -> subprocess.CompletedProcess:
        joined = " ".join(cmd)
        if "lint-imports" in joined:
            return subprocess.CompletedProcess(cmd, 0, stdout="Contracts: 50 kept, 0 broken.\n", stderr="")
        if "--cov" in joined:
            return subprocess.CompletedProcess(cmd, 0, stdout="TOTAL 15096 11954 55%\n", stderr="")
        if "bandit" in joined:
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        if "mypy" in joined:
            return subprocess.CompletedProcess(cmd, 0, stdout="Success\n", stderr="")
        if "verify_policy_integrity" in joined:
            return subprocess.CompletedProcess(cmd, 0, stdout="PASS", stderr="")
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=f"unmocked: {joined}")

    monkeypatch.setattr(cg, "RUN", fake)
    return subprocess.CompletedProcess([], 0)


def test_gate_dev_passes(fake_run: subprocess.CompletedProcess) -> None:
    checks = cg.run_gate("gate-dev")
    assert checks
    assert all(c["status"] == "PASS" for c in checks)
    assert "G10" not in {c["id"] for c in checks}


def test_gate_release_blocks_on_b15(fake_run: subprocess.CompletedProcess) -> None:
    checks = cg.run_gate("gate-release")
    ids = {c["id"] for c in checks}
    assert "G10" in ids
    g10 = next(c for c in checks if c["id"] == "G10")
    assert g10["status"] == "BLOCK"
    assert g10["actual"] == "EN_CURSO"


def test_every_check_has_evidence_schema(fake_run: subprocess.CompletedProcess) -> None:
    for check in cg.run_gate("gate-dev"):
        assert check["id"].startswith("G")
        assert check["name"]
        assert check["status"] in ("PASS", "BLOCK")
        assert check["threshold"]
        assert check["evidence"]


def test_policy_registry_rules_mapped() -> None:
    """R1..R4 y R7 (G1..G4, G9) deben estar activas en tracking.yaml."""
    for rid in ("R1", "R2", "R3", "R4", "R7"):
        assert cg._tracking_rule_status(rid) == "ok", f"{rid} no activa en CI"


def test_backlog_dependencies_honest() -> None:
    assert cg._backlog_status("B-15") == "EN_CURSO"  # G10 real
    assert cg._backlog_status("B-17") == "HECHO"  # G11 real
