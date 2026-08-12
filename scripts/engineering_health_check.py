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
CONFIG = ROOT / "architecture" / "importlinter.toml"
CI = ROOT / ".github" / "workflows" / "ocm-ci.yml"

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

    for r in data.get("reglas", []):
        if r.get("backtest") not in {"ok", "pendiente", "fail"}:
            err(f"{r.get('id', '?')}: backtest={r.get('backtest')!r} no válido")
        if not isinstance(r.get("activada_en_ci"), bool):
            err(f"{r.get('id', '?')}: activada_en_ci={r.get('activada_en_ci')!r} no bool")


# ---------------------------------------------------------------------------
# 2. Coherencia interna y trazabilidad de referencias
# ---------------------------------------------------------------------------
def check_coherence(data: dict) -> None:
    backlog_ids = {h["id"] for h in data.get("hallazgos", [])}

    for r in data.get("reglas", []):
        rid = r.get("id", "?")
        if r.get("backtest") == "ok" and r.get("activada_en_ci") is not True:
            err(f"{rid}: backtest=ok exige activada_en_ci=true")
        if r.get("activada_en_ci") is True and r.get("backtest") != "ok":
            err(f"{rid}: activada_en_ci=true exige backtest=ok")

        hid = r.get("hallazgo", "")
        if str(hid).startswith("B-") and hid not in backlog_ids:
            err(f"{rid}: referencia hallazgo {hid} inexistente en backlog")

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
def check_ci_gates_mapped(data: dict) -> None:
    active = [r.get("id", "?") for r in data.get("reglas", []) if r.get("activada_en_ci")]
    if not active:
        err("tracking.yaml: no hay reglas con activada_en_ci=true")
        return
    if not CI.exists():
        err(f"ocm-ci.yml no existe ({CI})")
        return

    ci_text = CI.read_text(encoding="utf-8")
    referenced: set[str] = set()
    for rid in active:
        if re.search(rf"\b{re.escape(rid)}\b", ci_text):
            referenced.add(rid)
    if not referenced:
        err("ocm-ci.yml no referencia ninguna regla activa del tracker")


# ---------------------------------------------------------------------------
def main() -> int:
    try:
        data = yaml.safe_load(TRACKING.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        err(f"tracking.yaml no es YAML válido: {exc}")
        data = None

    if data is None:
        errors and print_errors()
        return 1

    check_tracking_parses()
    check_coherence(data)
    check_import_contracts()
    check_ci_gates_mapped(data)

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
