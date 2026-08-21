#!/usr/bin/env python3
"""
scripts/engineering_health_check.py — Engineering Health Check (Plan F2.0).

Gate de coherencia Plan Maestro ↔ tracking.yaml ↔ ADR ↔ contratos ↔ CI.
Es la base de ejecución del resto de F2 (PLAN-Maestro-Ingenieria.md §4 F2.0).

Valida en una sola pasada (artefactos normativos N1..N7, ver Plan §13):
  1. tracking.yaml parsea (YAML) y sus enums están cerrados.
  2. coherencia interna: backtest ok ⇒ activada_en_ci true; estado HECHO ⇒
     fecha_cierre + cadena.cierre.evidencia; CONFIRMADO ⇒ evidencia no vacía.
  3. contratos de arquitectura >= baseline (49) y no-vacuo (sin "Could not find").
  4. cada regla activa (activada_en_ci: true) está respaldada por un test/guard
     presente en CI (los nombres de job guardan relación con el dominio).

Salida binaria: exit 0 = PASS, exit 1 = FAIL (bloquea merge en CI por fail-fast).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TRACKING = ROOT / "docs" / "plans" / "tracking.yaml"
CONFIG = ROOT / "architecture_linter" / "importlinter.toml"
CI = ROOT / ".github" / "workflows" / "ocm-ci.yml"
REGISTRY = ROOT / "policies" / "registry.yaml"

MIN_CONTRACTS = 49

# Enums cerrados — SSOT del tracker (ver cabecera de tracking.yaml).
ESTADOS = {"PENDIENTE", "EN_CURSO", "HECHO", "VERIFICACION", "RECHAZADO"}
ESTADOS_AUDITORIA = {
    "CONFIRMADO",
    "NO_CONFIRMADO",
    "PARCIALMENTE_CONFIRMADO",
    "REFORMULADO",
}
PRIORIDADES = {"CRITICA", "ALTA", "MEDIA", "BAJA"}
FASES = {"F1", "F2", "F3", "F4", "F5"}
ESLABON_ESTADO = {"NO_APLICA", "PENDIENTE", "HECHO", "PARCIAL"}

errors: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


# ---------------------------------------------------------------------------
# 1. tracking.yaml parseable + enums cerrados
# ---------------------------------------------------------------------------
def check_tracking_parses() -> None:
    try:
        data = yaml.safe_load(TRACKING.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        err(f"tracking.yaml no es YAML válido: {exc}")
        return
    if not isinstance(data, dict):
        err("tracking.yaml no es un mapeo raíz")
        return

    for h in data.get("hallazgos", []):
        for field, allowed in (
            ("fase", FASES),
            ("prioridad", PRIORIDADES),
            ("estado", ESTADOS),
            ("estado_auditoria", ESTADOS_AUDITORIA),
        ):
            v = h.get(field)
            if v not in allowed:
                err(f"{h.get('id', '?')}: {field}={v!r} fuera de enum {sorted(allowed)}")

        for slot, esl in (h.get("cadena") or {}).items():
            if isinstance(esl, dict):
                st = esl.get("estado")
                if st not in ESLABON_ESTADO:
                    err(f"{h.get('id', '?')}: cadena.{slot}.estado={st!r} fuera de enum")

    # NOTA (ADR-0031): validación de reglas migrada a check_registry_parses().
    # tracking.yaml ya no es SSOT de reglas -- ver policies/registry.yaml.


# ---------------------------------------------------------------------------
# 2. Coherencia interna y trazabilidad de referencias
# ---------------------------------------------------------------------------
def check_coherence(data: dict) -> None:

    # NOTA (ADR-0031): coherencia backtest<->activada_en_ci retirada de aqui
    # -- ver check_registry_parses(). La referencia regla->hallazgo NO tiene
    # equivalente en registry.yaml (gap conocido, registrar finding).

    for h in data.get("hallazgos", []):
        bid = h.get("id", "?")
        if h.get("estado") == "HECHO":
            if not h.get("fecha_cierre"):
                err(f"{bid}: estado HECHO requiere fecha_cierre")
            cierre = (h.get("cadena") or {}).get("cierre") or {}
            # El tracker (SSOT operativo) usa `cadena.*.referencia`; el Plan §2
            # lo denomina `evidencia`. Ambos significan "dónde se demostró el
            # cierre" — basta con que el campo esté presente y no vacío.
            evid = cierre.get("evidencia") or cierre.get("referencia")
            if not evid:
                err(f"{bid}: estado HECHO requiere cadena.cierre.evidencia")
        if h.get("estado_auditoria") == "CONFIRMADO" and not h.get("evidencia"):
            err(f"{bid}: estado_auditoria CONFIRMADO requiere evidencia no vacía")


# ---------------------------------------------------------------------------
# 3. Contratos de arquitectura: no-vacuo y no por debajo de baseline
# ---------------------------------------------------------------------------
def check_import_contracts() -> None:
    proc = subprocess.run(
        ["uv", "run", "lint-imports", "--config", str(CONFIG)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    combined = proc.stdout + proc.stderr
    if "Could not find" in combined:
        err(f"import-linter no-vacuo falla: {combined.strip()[-200:]}")
        return
    match = re.search(r"(\d+)\s+kept,\s+(\d+)\s+broken", combined)
    if not match:
        err(f"import-linter no reportó sumario: {combined[-300:]}")
        return
    kept, broken = int(match.group(1)), int(match.group(2))
    if broken:
        err(f"import-linter: {broken} contratos rotos")
    if kept < MIN_CONTRACTS:
        err(f"import-linter: {kept} contratos < baseline {MIN_CONTRACTS}")


# ---------------------------------------------------------------------------
# 4. Cada gate de CI está respaldado por una regla activa del tracker
# ---------------------------------------------------------------------------
def load_registry() -> dict:
    try:
        data = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        err(f"policies/registry.yaml no es YAML valido: {exc}")
        return {}
    if not isinstance(data, dict):
        err("policies/registry.yaml no es un mapeo raiz")
        return {}
    return data


REGISTRY_STATUS = {"ACTIVE", "DEPRECATED"}
REGISTRY_ENFORCEMENT = {"blocking", "warning", "informational"}
REGISTRY_ACTIVE_ENFORCEMENT = {"blocking", "warning"}


def check_registry_parses(registry: dict) -> None:
    for r in registry.get("rules", []):
        rid = r.get("id", "?")
        status = r.get("status")
        enforcement = r.get("enforcement")
        if status not in REGISTRY_STATUS:
            err(f"{rid}: status={status!r} fuera de enum {sorted(REGISTRY_STATUS)}")
        if enforcement not in REGISTRY_ENFORCEMENT:
            err(f"{rid}: enforcement={enforcement!r} fuera de enum {sorted(REGISTRY_ENFORCEMENT)}")
        if status == "ACTIVE" and enforcement in REGISTRY_ACTIVE_ENFORCEMENT:
            ci = r.get("ci") or {}
            if not ci.get("job"):
                err(f"{rid}: ACTIVE + enforcement={enforcement} exige ci.job")


def check_registry_ci_gates(registry: dict) -> None:
    active_rules = [
        r
        for r in registry.get("rules", [])
        if r.get("status") == "ACTIVE" and r.get("enforcement") in REGISTRY_ACTIVE_ENFORCEMENT
    ]
    if not active_rules:
        err("policies/registry.yaml: no hay reglas ACTIVE con enforcement blocking/warning")
        return
    if not CI.exists():
        err(f"ocm-ci.yml no existe ({CI})")
        return

    try:
        ci_data = yaml.safe_load(CI.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        err(f"ocm-ci.yml no es YAML valido: {exc}")
        return
    ci_jobs = set((ci_data.get("jobs") or {}).keys())

    for r in active_rules:
        rid = r.get("id", "?")
        job = (r.get("ci") or {}).get("job")
        if job not in ci_jobs:
            err(f"{rid}: ci.job={job!r} no existe como job real en ocm-ci.yml")


# ---------------------------------------------------------------------------
def main() -> int:
    try:
        data = yaml.safe_load(TRACKING.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        err(f"tracking.yaml no es YAML válido: {exc}")
        data = None

    if data is None:
        if errors:
            print_errors()
        return 1

    registry = load_registry()

    check_tracking_parses()
    check_coherence(data)
    check_import_contracts()
    check_registry_parses(registry)
    check_registry_ci_gates(registry)

    if errors:
        print(f"[EngineeringHealth] FAIL — {len(errors)} incoherencia(s):")
        for e in errors:
            print(f"  ✗ {e}")
        return 1
    print("[EngineeringHealth] PASS — Plan ↔ tracker ↔ ADR ↔ contratos ↔ CI alineados")
    return 0


# ---------------------------------------------------------------------------
def print_errors() -> None:
    print("[EngineeringHealth] FAIL — errores de parseo:")
    for e in errors:
        print(f"  ✗ {e}")


if __name__ == "__main__":
    sys.exit(main())
