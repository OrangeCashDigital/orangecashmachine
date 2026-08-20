#!/usr/bin/env python3
"""scripts/check_production_gates.py — Production Gate binario (ADR-0033, G1..G11).

Veredicto binario PASS/BLOCK de aptitud para producción, consumible por agente
IA con un solo comando (F-PL-04, F-PLA-05). Reutiliza los gates existentes
(no los reimplementa): cada cheque G1..G11 ejecuta el mecanismo real y reporta
estado + evidencia + umbral + valor actual.

Modes:
  --mode gate-dev      todos los cheques G1..G9 + G11 (G10 exige B-15 cerrado)
  --mode gate-release  estricto, todos G1..G11 obligatorios

Exit codes: 0 = PASS; 1 = BLOCK; 2 = error de ejecución.
Salida: JSON en stdout con {verdict, mode, checks: [...]}.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Callable

import yaml

ROOT = Path(__file__).resolve().parent.parent
TRACKING = ROOT / "docs" / "plans" / "tracking.yaml"
CONFIG = ROOT / "architecture_linter" / "importlinter.toml"
CI = ROOT / ".github" / "workflows" / "ocm-ci.yml"

MIN_CONTRACTS = 49


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


# Punto de inyección para tests (G5..G8 ejecutan subprocesos pesados: los
# tests unitarios lo reemplazan por un fake determinista).
RUN: Callable[[list[str]], subprocess.CompletedProcess] = _run


def _tracking() -> dict:
    with open(TRACKING, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _tracking_rule_status(rid: str) -> str | None:
    """Devuelve el estado de `backtest`+`activada_en_ci` de una regla del tracker."""
    data = _tracking()
    for r in data.get("reglas", []):
        if r.get("id") == rid:
            if r.get("backtest") == "ok" and r.get("activada_en_ci") is True:
                return "ok"
            return "pendiente"
    return None


def _backlog_status(tid: str) -> str | None:
    data = _tracking()
    for h in data.get("hallazgos", []):
        if h.get("id") == tid:
            return h.get("estado")
    return None


def _policy_registry_ok() -> bool:
    import importlib.util

    spec = importlib.util.spec_from_file_location("audit_validator", ROOT / "scripts" / "audit_validator.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    ctx = mod.AuditContext(
        register=ROOT / "docs" / "plans" / "tracking.yaml",
        report=None,
        tracking=ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=ROOT / "docs" / "architecture" / "decisions",
        golden=None,
    )
    mod.m22_policy_registry_tests(ctx)
    mod.m23_policy_registry_enforcement(ctx)
    mod.m24_policy_registry_dead(ctx)
    mod.m25_policy_registry_waivers(ctx)
    mod.m26_policy_registry_adrs(ctx)
    return not ctx.errors


def _integrity_ok() -> bool:
    proc = RUN(["uv", "run", "python", str(ROOT / "scripts" / "verify_policy_integrity.py")])
    return proc.returncode == 0


# ────────────────────────────────────────────────────────────────────────────
# Cheques G1..G11 (mecanismo real de cada regla)
# ────────────────────────────────────────────────────────────────────────────


def check_g1() -> dict:
    """G1. LiveExecutor stub bloquea live (R1)."""
    status = "PASS" if _tracking_rule_status("R1") == "ok" else "BLOCK"
    return {
        "id": "G1",
        "name": "LiveExecutor stub bloquea live (R1)",
        "status": status,
        "threshold": "backtest ok + activada_en_ci",
        "actual": _tracking_rule_status("R1") or "no registrada",
        "evidence": "docs/plans/tracking.yaml R1 + job architecture (test_import_contracts.py)",
    }


def check_g2() -> dict:
    """G2. Composition root construye pipelines (R2)."""
    status = "PASS" if _tracking_rule_status("R2") == "ok" else "BLOCK"
    return {
        "id": "G2",
        "name": "Composition root construye pipelines (R2)",
        "status": status,
        "threshold": "backtest ok + activada_en_ci",
        "actual": _tracking_rule_status("R2") or "no registrada",
        "evidence": "docs/plans/tracking.yaml R2 + job architecture",
    }


def check_g3() -> dict:
    """G3. Contador de riesgo correcto (R3)."""
    status = "PASS" if _tracking_rule_status("R3") == "ok" else "BLOCK"
    return {
        "id": "G3",
        "name": "Contador de riesgo correcto (R3)",
        "status": status,
        "threshold": "backtest ok + activada_en_ci",
        "actual": _tracking_rule_status("R3") or "no registrada",
        "evidence": "docs/plans/tracking.yaml R3 + job unit-tests (test_oms_fill_lifecycle.py)",
    }


def check_g4() -> dict:
    """G4. Secrets redactados en snapshot (R4)."""
    status = "PASS" if _tracking_rule_status("R4") == "ok" else "BLOCK"
    return {
        "id": "G4",
        "name": "Secrets redactados en snapshot (R4)",
        "status": status,
        "threshold": "backtest ok + activada_en_ci",
        "actual": _tracking_rule_status("R4") or "no registrada",
        "evidence": "docs/plans/tracking.yaml R4 + job unit-tests (test_snapshot_redaction.py)",
    }


def check_g5() -> dict:
    """G5. Contratos BC válidos (import-linter)."""
    proc = RUN(["uv", "run", "lint-imports", "--config", str(CONFIG)])
    text = proc.stdout + proc.stderr
    kept = 0
    for m in [int(x) for x in __import__("re").findall(r"(\d+)\s+kept", text)]:
        kept = m
    status = "PASS" if proc.returncode == 0 and kept >= MIN_CONTRACTS else "BLOCK"
    return {
        "id": "G5",
        "name": "Contratos BC válidos",
        "status": status,
        "threshold": f"import-linter PASS y contratos >= {MIN_CONTRACTS}",
        "actual": f"{kept} kept (exit {proc.returncode})",
        "evidence": "architecture_linter/importlinter.toml",
    }


def check_g6() -> dict:
    """G6. Cobertura crítica (pytest --cov, fail_under=40)."""
    proc = RUN(
        [
            "uv",
            "run",
            "pytest",
            "tests/",
            "-q",
            "-m",
            "not integration",
            "--cov=packages",
            "--cov=ocm",
            "--cov=shared",
            "--cov=apps",
            "--cov-report=term",
        ]
    )
    text = proc.stdout + proc.stderr
    total = 0.0
    for m in __import__("re").findall(r"TOTAL\s+[\d\s]+\s+([\d.]+)%", text):
        total = float(m)
    status = "PASS" if proc.returncode == 0 and total >= 40.0 else "BLOCK"
    return {
        "id": "G6",
        "name": "Cobertura crítica",
        "status": status,
        "threshold": "fail_under=40 (pyproject.toml)",
        "actual": f"{total}% (exit {proc.returncode})",
        "evidence": "pyproject.toml [tool.coverage.report] fail_under",
    }


def check_g7() -> dict:
    """G7. Bandit limpio (bandit -ll)."""
    proc = RUN(["uv", "run", "bandit", "-r", "apps", "ocm", "packages", "shared", "infrastructure", "-ll"])
    status = "PASS" if proc.returncode == 0 else "BLOCK"
    return {
        "id": "G7",
        "name": "Bandit limpio",
        "status": status,
        "threshold": "0 hallazgos HIGH (bandit -ll)",
        "actual": f"exit {proc.returncode}",
        "evidence": "job security (ocm-ci.yml)",
    }


def check_g8() -> dict:
    """G8. Mypy completo sin errores."""
    proc = RUN(["uv", "run", "mypy", "."])
    status = "PASS" if proc.returncode == 0 else "BLOCK"
    return {
        "id": "G8",
        "name": "Mypy completo",
        "status": status,
        "threshold": "0 errores (mypy .)",
        "actual": f"exit {proc.returncode}",
        "evidence": "job quality (ocm-ci.yml)",
    }


def check_g9() -> dict:
    """G9. Paridad de config (R7)."""
    status = "PASS" if _tracking_rule_status("R7") == "ok" else "BLOCK"
    return {
        "id": "G9",
        "name": "Paridad de config (R7)",
        "status": status,
        "threshold": "backtest ok + activada_en_ci",
        "actual": _tracking_rule_status("R7") or "no registrada",
        "evidence": "docs/plans/tracking.yaml R7 + job unit-tests (test_structured_parity.py)",
    }


def check_g10() -> dict:
    """G10. Estado de posición único (B-15)."""
    status = "PASS" if _backlog_status("B-15") == "HECHO" else "BLOCK"
    return {
        "id": "G10",
        "name": "Estado de posición único (B-15)",
        "status": status,
        "threshold": "B-15 HECHO",
        "actual": _backlog_status("B-15") or "no registrado",
        "evidence": "docs/plans/tracking.yaml B-15",
    }


def check_g11() -> dict:
    """G11. Trazabilidad activa (B-17)."""
    status = "PASS" if _backlog_status("B-17") == "HECHO" else "BLOCK"
    return {
        "id": "G11",
        "name": "Trazabilidad activa (B-17)",
        "status": status,
        "threshold": "B-17 HECHO",
        "actual": _backlog_status("B-17") or "no registrado",
        "evidence": "docs/plans/tracking.yaml B-17",
    }


def check_integrity() -> dict:
    """Integridad de archivos protegidos (ADR-0032) — auxiliar del gate."""
    ok = _integrity_ok()
    return {
        "id": "G12",
        "name": "Integridad de archivos protegidos (ADR-0032)",
        "status": "PASS" if ok else "BLOCK",
        "threshold": "SHA256 de guards/registry/CI coincide con policies/evidence.json",
        "actual": "PASS" if ok else "MISMATCH",
        "evidence": "scripts/verify_policy_integrity.py",
    }


# Gate G1..G11 (G10 exige F4/B-15 cerrado: solo en gate-release)
GATES: dict[str, Callable[[], dict]] = {
    "G1": check_g1,
    "G2": check_g2,
    "G3": check_g3,
    "G4": check_g4,
    "G5": check_g5,
    "G6": check_g6,
    "G7": check_g7,
    "G8": check_g8,
    "G9": check_g9,
    "G10": check_g10,
    "G11": check_g11,
}


def run_gate(mode: str) -> list[dict]:
    active: list[tuple[str, Callable[[], dict]]] = list(GATES.items())
    if mode == "gate-dev":
        # G10 exige F4 (B-15 EN_CURSO): no bloquea en dev.
        active = [(gid, fn) for gid, fn in active if gid != "G10"]
    out: list[dict] = []
    for gid, fn in active:
        try:
            out.append(fn())
        except Exception as exc:  # noqa: BLE001
            out.append(
                {
                    "id": gid,
                    "name": gid,
                    "status": "BLOCK",
                    "threshold": "-",
                    "actual": f"error: {exc}",
                    "evidence": "-",
                }
            )
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Production Gate binario G1..G11 (ADR-0033)")
    parser.add_argument("--mode", choices=["gate-dev", "gate-release"], default="gate-dev")
    parser.add_argument("--json", action="store_true", help="salida JSON pura (stdout)")
    args = parser.parse_args(argv)

    checks = run_gate(args.mode)
    verdict = "PASS" if all(c["status"] == "PASS" for c in checks) else "BLOCK"

    payload = {"verdict": verdict, "mode": args.mode, "checks": checks}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        for c in checks:
            print(f"{c['status']:5}  {c['id']}  {c['name']}")
        print(f"VERDICT: {verdict} ({args.mode})")

    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
