#!/usr/bin/env python3
"""scripts/audit_validator.py — Audit Validator (sistema de auditoría OCM).

Validador mecánico y determinista del sistema de auditoría de OCM.
Implementa las reglas M1..M20 del AUDIT_PROTOCOL.md.

Principio: MACHINE CHECKS FIRST. LLM JUDGMENT SECOND.
El LLM solo decide donde la semántica lo requiere; todo lo verificable
por código se verifica aquí, de forma reproducible y con exit codes.

Patrón de referencia: scripts/engineering_health_check.py (F2.0).

Entradas (por defecto, las rutas canónicas del repo):
  --register   Registro de findings (OCM_AUDIT_FINDINGS_*.md)
  --report     Informe canónico (AUDIT_OCM_FORENSIC_COMPLIANCE_*.md)
  --tracking   tracking.yaml
  --adrs       directorio de ADRs (docs/architecture/decisions/)
  --golden     test_golden.py (para M11)

Exit codes: 0 = sin errores mecánicos; 1 = hay errores; 2 = error de ejecución.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Sequence

import yaml

ROOT = Path(__file__).resolve().parent.parent

# ───────────────────────────────────────────────────────────────────────────
# Enums cerrados (SSOT del protocolo de auditoría — AUDIT_PROTOCOL.md)
# ───────────────────────────────────────────────────────────────────────────
CLASSIFICATIONS = frozenset(
    {
        "NUEVO",
        "REVALIDADO",
        "REGRESIÓN",
        "CERRADO",
        "CONTRADICCIÓN",
        "RECOMENDACIÓN",
        "NO_VERIFICADO",
    }
)
SEVERITIES = frozenset({"CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"})
CONTROL_STATES = frozenset({"PASS", "FAIL", "PARTIAL", "NO_VERIFICADO", "INFRA_FAILURE"})
# Estados canónicos base de ADR (ADR-template.md). "Reemplazado por ADR-XXXX"
# y matices ("Aceptado (recuperado...)") se normalizan al estado base.
ADR_BASE_STATES = frozenset({"PROPUESTO", "ACEPTADO", "REEMPLAZADO", "OBSOLETO"})
DECISION_STATES = frozenset({"BLOCKING", "NON_BLOCKING"})

ADR_NUMBER_RE = re.compile(r"\bADR-(\d{4})\b")

# ───────────────────────────────────────────────────────────────────────────
# Comandos canónicos (fuente: CI real .github/workflows/ocm-ci.yml y
# workflows asociados — verificados en la auditoría 2026-08-18).
# control_id -> (comando exacto, exit code esperado, interpretación)
# ───────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Canonical audit filename grammar
#
# REPORT:
#   AUDIT_OCM_<slug>_<YYYY-MM-DD>.md
#   AUDIT_OCM_<slug>_<YYYY-MM-DD>_<NN>.md
#
# FINDINGS REGISTER:
#   OCM_AUDIT_FINDINGS_<YYYY-MM-DD>_<slug>.md
#   OCM_AUDIT_FINDINGS_<YYYY-MM-DD>_<slug>_<NN>.md
#
# NN is required only when more than one artifact exists for the same
# subject/date. The validator uses the filename itself as the stable
# machine-readable identity of the artifact.
# ──────────────────────────────────────────────────────────────────────────────

CANONICAL_REPORT_FILENAME_RE = re.compile(
    r"^AUDIT_OCM_(?P<slug>.+)_(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:_(?P<seq>\d{2}))?\.md$"
)

CANONICAL_REGISTER_FILENAME_RE = re.compile(
    r"^OCM_AUDIT_FINDINGS_(?P<date>\d{4}-\d{2}-\d{2})_"
    r"(?P<slug>.+?)(?:_(?P<seq>\d{2}))?\.md$"
)


def parse_canonical_report_filename(name: str) -> dict[str, str] | None:
    """Parse a canonical audit report filename."""
    m = CANONICAL_REPORT_FILENAME_RE.fullmatch(name)
    return m.groupdict() if m else None


def parse_canonical_register_filename(name: str) -> dict[str, str] | None:
    """Parse a canonical audit findings-register filename."""
    m = CANONICAL_REGISTER_FILENAME_RE.fullmatch(name)
    return m.groupdict() if m else None


CANONICAL_COMMANDS: dict[str, tuple[str, int, str]] = {
    "ARCH_CONTRACTS": (
        "uv run lint-imports --config architecture_linter/importlinter.toml",
        0,
        "GATE: broken = blocked merge (fail-fast). Expect 0 broken contracts.",
    ),
    "ENGINEERING_HEALTH": (
        "uv run python scripts/engineering_health_check.py",
        0,
        "GATE F2.0: Plan ↔ tracker ↔ ADR ↔ contratos ↔ CI alineados.",
    ),
    "ARCH_LINTER": (
        "uv run python -m architecture_linter --root . --json",
        1,
        "DETECTOR: exit 1 si hay FAIL/PARTIAL. Golden test fija el estado esperado (no-regresión).",
    ),
    "GOLDEN": (
        "uv run pytest tests/architecture_linter/test_golden.py -q --no-cov",
        0,
        "GATE de no-regresión: resultado actual == GOLDEN_EXPECTED. GOLDEN PASS != conformidad.",
    ),
    "RUFF_CHECK": ("uv run ruff check .", 0, "GATE calidad: 0 errores de lint."),
    "RUFF_FORMAT": ("uv run ruff format . --check", 0, "GATE calidad: formato conforme."),
    "MYPY": ("uv run mypy . --no-incremental", 0, "GATE calidad: tipado sin errores."),
    "SSOT_ENUMS": ("uv run python scripts/check_ssot_enums.py", 0, "GATE SSOT: literales solo en shared/enums.py."),
    "DEPENDENCY_AUDIT": (
        "uv run pip-audit . --ignore-vuln PYSEC-2026-113 --ignore-vuln PYSEC-2026-1325",
        0,
        "GATE seguridad. Comando CANÓNICO = el del CI (risk-accept documentado 2026-08-03). "
        "Sin los ignores del CI produce 6 vulns (no 4) — eso NO es un finding distinto.",
    ),
    "BANDIT": (
        "uv run bandit -r apps ocm packages shared infrastructure -ll",
        0,
        "GATE seguridad: 0 vulnerabilidades Med/High.",
    ),
    "UNIT_TESTS": (
        "uv run pytest tests/ -x -q -m 'not integration'",
        0,
        "GATE runtime: suite unit completa en verde.",
    ),
    "YAMLLINT": ("uvx yamllint -c .yamllint .", 0, "GATE YAML: 0 errores."),
}

# ───────────────────────────────────────────────────────────────────────────
# Modelos
# ───────────────────────────────────────────────────────────────────────────


@dataclass
class Finding:
    fid: str
    severity: str | None = None
    status: str | None = None
    classification: str | None = None
    control: str | None = None
    evidence: list[str] = field(default_factory=list)
    tracking_refs: list[str] = field(default_factory=list)
    adr_refs: list[str] = field(default_factory=list)
    block: str = ""


@dataclass
class AuditContext:
    register: Path
    report: Path | None
    tracking: Path
    adrs_dir: Path
    golden: Path | None
    registry: Path = field(default_factory=lambda: ROOT / "policies" / "registry.yaml")
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)
    report_findings: dict[str, tuple[str, str]] = field(default_factory=dict)

    def err(self, rule: str, msg: str) -> None:
        self.errors.append(f"[{rule}] {msg}")

    def warn(self, rule: str, msg: str) -> None:
        self.warnings.append(f"[{rule}] {msg}")

    def skip(self, rule: str, msg: str) -> None:
        self.skipped.append(f"[{rule}] {msg}")


# ───────────────────────────────────────────────────────────────────────────
# Parsers
# ───────────────────────────────────────────────────────────────────────────


def parse_findings(text: str) -> list[Finding]:
    """Parse del registro de findings (`## F-XXX — título` + fichas de campo)."""
    out: list[Finding] = []
    blocks = re.split(r"^(?:#{1,3}) (F-\S+)", text, flags=re.MULTILINE)
    # blocks = [prefacio, id1, cuerpo1, id2, cuerpo2, ...]
    for i in range(1, len(blocks), 2):
        fid = blocks[i].strip()
        body = blocks[i + 1]
        finding = Finding(fid=fid, block=body)

        m = re.search(r"^Severity:\s*(\S+)", body, re.MULTILINE)
        finding.severity = m.group(1) if m else None
        m = re.search(r"^Status:\s*(\S+)", body, re.MULTILINE)
        finding.status = m.group(1) if m else None
        m = re.search(r"^Classification:\s*(\S+)", body, re.MULTILINE)
        finding.classification = m.group(1) if m else None
        m = re.search(r"^Control:\s*(.+)", body, re.MULTILINE)
        finding.control = m.group(1).strip() if m else None

        ev_block = re.search(r"^Evidence:\s*\n((?:^-\s.*\n?)+)", body, re.MULTILINE)
        if ev_block:
            finding.evidence = [line.strip("- ").strip() for line in ev_block.group(1).splitlines() if line.strip()]

        tr_block = re.search(r"^Traceability:\s*\n((?:^-\s.*\n?)+)", body, re.MULTILINE)
        if tr_block:
            tr = tr_block.group(1)
            m = re.search(r"Tracking:\s*([^\·]+)", tr)
            if m:
                tracking_val = m.group(1).strip()
                # Formato real: "B-15 (EN_CURSO)" | "NOT_TRACED" | "B-MD-008 (PENDIENTE)"
                tracking_ids = re.findall(r"\b(?:B-[A-Z0-9-]+|R\d+)\b", tracking_val)
                if tracking_ids:
                    finding.tracking_refs = tracking_ids
                elif tracking_val != "NOT_TRACED":
                    finding.tracking_refs = [tracking_val]
            m = re.search(r"ADR:\s*([^\·]+)", tr)
            if m:
                finding.adr_refs = ADR_NUMBER_RE.findall(m.group(1))
                # formato "ADR-NNNN" sin número (p.ej. NOT_TRACED) no aporta refs
        out.append(finding)
    return out


def parse_report_findings(text: str) -> dict[str, tuple[str, str]]:
    """Parse de la tabla de findings del informe canónico → {fid: (severity, classification)}."""
    table: dict[str, tuple[str, str]] = {}
    for line in text.splitlines():
        if not line.startswith("| F-"):
            continue
        cols = [c.strip() for c in line.strip("|").split("|")]
        if len(cols) < 3:
            continue
        fid, severity, classification = cols[0], cols[1], cols[2]
        table[fid] = (severity, classification)
    return table


def parse_report_control_counts(text: str) -> dict[str, int]:
    """Parse de la matriz de controles del informe → {estado: count}.

    Solo considera las filas dentro de la sección 'Matriz de Controles',
    no cualquier tabla del documento.
    """
    counts: dict[str, int] = {}
    section = re.search(
        r"^#{1,4}\s+[^\n]*Matriz de Controles.*(?=\n^#{1,4}\s|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not section:
        return counts
    for line in section.group(0).splitlines():
        if not line.startswith("| "):
            continue
        cols = [c.strip() for c in line.strip("|").split("|")]
        if len(cols) < 4:
            continue
        state = cols[-1].replace("*", "").split("(")[0].strip()
        if state in CONTROL_STATES:
            counts[state] = counts.get(state, 0) + 1
    return counts


def load_adr_states(adrs_dir: Path) -> dict[str, str]:
    """Extrae el estado base de cada ADR (formato `**Estado:**` o `## Estado`)."""
    states: dict[str, str] = {}
    if not adrs_dir.is_dir():
        return states
    for p in sorted(adrs_dir.glob("ADR-*.md")):
        lines = p.read_text(encoding="utf-8").splitlines()
        raw = ""
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("**Estado**") or stripped.startswith("**Estado:"):
                raw = stripped.split("**", 2)[-1].strip().lstrip(":").strip()
                break
            if stripped.startswith("## Estado"):
                # el estado va en la siguiente línea no vacía
                for nxt in lines[i + 1 :]:
                    if nxt.strip():
                        raw = nxt.strip()
                        break
                break
        base = normalize_adr_state(raw)
        if base is not None:
            states[p.name] = base
    return states


def normalize_adr_state(raw: str) -> str | None:
    """Normaliza el texto de estado a un estado base del enum."""
    if not raw:
        return None
    token = raw.strip().lower()
    if token.startswith("propuest"):
        return "PROPUESTO"
    if token.startswith("aceptad"):
        return "ACEPTADO"
    if token.startswith("reemplazad"):
        return "REEMPLAZADO"
    if token.startswith("obsolet"):
        return "OBSOLETO"
    # estados inventados / desconocidos
    return token


# ───────────────────────────────────────────────────────────────────────────
# Reglas mecánicas M1..M20
# ───────────────────────────────────────────────────────────────────────────


def m01_unique_ids(ctx: AuditContext) -> None:
    """M1 — IDs únicos en el registro."""
    seen: set[str] = set()
    for f in ctx.findings:
        if f.fid in seen:
            ctx.err("M1", f"ID duplicado en registro: {f.fid}")
        seen.add(f.fid)


def m02_classification_enum(ctx: AuditContext) -> None:
    """M2 — clasificación dentro del enum cerrado."""
    for f in ctx.findings:
        if f.classification is None:
            ctx.err("M2", f"{f.fid}: sin Classification")
        elif f.classification not in CLASSIFICATIONS:
            ctx.err("M2", f"{f.fid}: Classification={f.classification!r} fuera de enum {sorted(CLASSIFICATIONS)}")


def m03_severity_enum(ctx: AuditContext) -> None:
    """M3 — severidad dentro del enum cerrado."""
    for f in ctx.findings:
        if f.severity is None:
            ctx.err("M3", f"{f.fid}: sin Severity")
        elif f.severity not in SEVERITIES:
            ctx.err("M3", f"{f.fid}: Severity={f.severity!r} fuera de enum {sorted(SEVERITIES)}")


def m04_adr_states(ctx: AuditContext) -> None:
    """M4 — estados ADR válidos (enum cerrado)."""
    states = load_adr_states(ctx.adrs_dir)
    if not states:
        ctx.skip("M4", "sin ADRs parseables")
        return
    for name, base in states.items():
        if base not in ADR_BASE_STATES:
            ctx.err("M4", f"{name}: estado inventado/inválido: {base!r} (enum {sorted(ADR_BASE_STATES)})")


def m05_reference_existence(ctx: AuditContext) -> None:
    """M5 — existencia de referencias (archivos citados en evidencia).

    WARN (no error): una ficha puede documentar legítimamente un archivo
    ausente (el sujeto del hallazgo). Solo se emite aviso cuando el path
    referenciado no existe y la línea no marca negación explícita.
    Las referencias rotas REALES (tracking, ADR) se detectan en M6/M9.
    """
    NEGATION = (
        "no existe",
        "no exist",
        "ausente",
        "inexistente",
        "obsolet",
        "muerta",
        "dead",
        "rota",
        "no encontrado",
        "pendiente de crear",
        "no encontramos",
    )
    for f in ctx.findings:
        for ev in f.evidence:
            if any(n in ev.lower() for n in NEGATION):
                continue
            for m in re.finditer(r"`([^`]+\.(?:py|yml|yaml|toml|md|json))`", ev):
                ref = m.group(1)
                candidate = ROOT / ref
                if candidate.exists():
                    continue
                # Fallback: el nombre puede citarse sin ruta completa (p.ej. `tracking.yaml`
                # en vez de `docs/plans/tracking.yaml`). Buscar recursivamente antes de avisar.
                basename = Path(ref).name
                matches = list(ROOT.rglob(basename))
                if matches:
                    continue
                msg = f"{f.fid}: archivo referenciado inexistente: {ref} (¿sujeto del hallazgo o referencia rota?)"
                ctx.warn("M5", msg)


def m06_findings_tracking(ctx: AuditContext) -> None:
    """M6 — findings ↔ tracking (referencias B-*/R-* existen en tracking.yaml)."""
    try:
        data = yaml.safe_load(ctx.tracking.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        ctx.err("M6", f"tracking.yaml no parsea: {exc}")
        return
    ids = set()
    for h in data.get("hallazgos", []):
        ids.add(str(h.get("id", "")))
    for r in data.get("reglas", []):
        ids.add(str(r.get("id", "")))
    for f in ctx.findings:
        for ref in f.tracking_refs:
            if ref in {"NOT_TRACED", "PENDIENTE", "EN_CURSO", "HECHO"}:
                continue
            if ref and ref not in ids:
                ctx.err("M6", f"{f.fid}: referencia tracking {ref!r} inexistente en tracking.yaml")


def m07_finding_evidence(ctx: AuditContext) -> None:
    """M7 — finding ↔ evidence (cada finding con evidence no vacía)."""
    for f in ctx.findings:
        if not f.evidence:
            ctx.err("M7", f"{f.fid}: sin evidence (cadena Finding→Evidencia rota)")


def m08_finding_control(ctx: AuditContext) -> None:
    """M8 — finding ↔ control (cada finding declara su control)."""
    for f in ctx.findings:
        if not f.control:
            ctx.err("M8", f"{f.fid}: sin control declarado")


def m09_finding_adr(ctx: AuditContext) -> None:
    """M9 — finding ↔ ADR (referencias ADR-NNNN existen como archivos)."""
    if not ctx.adrs_dir.is_dir():
        ctx.skip("M9", "directorio ADR inexistente")
        return
    existing = {p.name for p in ctx.adrs_dir.glob("ADR-*.md")}
    for f in ctx.findings:
        for adr_num in f.adr_refs:
            matches = [n for n in existing if f"ADR-{adr_num}-" in n or f"ADR-{adr_num}.md" in n]
            if not matches:
                ctx.err("M9", f"{f.fid}: referencia ADR-{adr_num} sin archivo en {ctx.adrs_dir}")


def m10_reconciliation(ctx: AuditContext) -> None:
    """M10 — reconciliación matemática del registro (clasificación + severidad)."""
    if not ctx.findings:
        ctx.skip("M10", "registro vacío")
        return
    total = len(ctx.findings)
    by_class: dict[str, int] = {}
    for f in ctx.findings:
        c = f.classification or "SIN_CLASIFICAR"
        by_class[c] = by_class.get(c, 0) + 1
    if sum(by_class.values()) != total:
        ctx.err("M10", f"suma clasificación {sum(by_class.values())} != total {total}")
    by_sev: dict[str, int] = {}
    for f in ctx.findings:
        s = f.severity or "SIN_SEVERIDAD"
        by_sev[s] = by_sev.get(s, 0) + 1
    if sum(by_sev.values()) != total:
        ctx.err("M10", f"suma severidad {sum(by_sev.values())} != total {total}")

    # El registro declara resumen explícito — comparar contra lo parseado.
    register_text = ctx.register.read_text(encoding="utf-8")
    declared = _parse_register_summary(register_text)
    if declared:
        if declared.get("total") is not None and declared["total"] != total:
            ctx.err("M10", f"total declarado en registro {declared['total']} != parseado {total}")
        for key, dcount in declared["classifications"].items():
            actual = by_class.get(key, 0)
            if actual != dcount:
                ctx.err("M10", f"{key}: declarado {dcount} != parseado {actual}")


def _parse_register_summary(text: str) -> dict | None:
    """Parse del resumen del registro (líneas `- CLASS: N` y `total N`)."""
    summary: dict = {"classifications": {}, "total": None}
    found = False
    for line in text.splitlines():
        m = re.match(r"^-\s*([A-ZÁÉÍÓÚÑ_]+):\s*(\d+)", line.strip())
        if m and m.group(1) in CLASSIFICATIONS:
            summary["classifications"][m.group(1)] = int(m.group(2))
            found = True
        m2 = re.match(r".*\btotal\s*(\d+)", line.strip(), re.IGNORECASE)
        if m2 and not line.startswith("-"):
            summary["total"] = int(m2.group(1))
    return summary if found else None


def m11_golden_state(ctx: AuditContext) -> None:
    """M11 — golden state definido, reproducible y coincidente con el linter."""
    if ctx.golden is None or not ctx.golden.exists():
        ctx.skip("M11", "--golden no provisto o inexistente")
        return
    expected = _parse_golden_expected(ctx.golden)
    if not expected:
        ctx.err("M11", f"no se pudo parsear GOLDEN_EXPECTED de {ctx.golden.name}")
        return
    actual = _run_linter_json()
    if actual is None:
        ctx.err("M11", "architecture_linter --json no ejecutable")
        return
    for rule_id, expected_status in expected.items():
        actual_status = actual.get(rule_id)
        if actual_status != expected_status:
            ctx.err("M11", f"golden mismatch: {rule_id} esperado {expected_status}, obtenido {actual_status}")
    # Semántica: GOLDEN PASS != conformidad. El golden DEBE contener FAIL/PARTIAL
    # (deuda gobernada) para ser un golden honesto.
    if expected and not any(s in {"FAIL", "PARTIAL"} for s in expected.values()):
        ctx.err("M11", "GOLDEN_EXPECTED sin FAIL/PARTIAL: golden vacío sospechoso (¿paso a conformidad?)")


def _parse_golden_expected(golden: Path) -> dict[str, str]:
    """Extrae {rule_id: status} del dict GOLDEN_EXPECTED en test_golden.py."""
    text = golden.read_text(encoding="utf-8")
    out: dict[str, str] = {}
    for line in text.splitlines():
        m = re.match(r'\s*"(ARCH-\d+)":\s*Status\.(\w+),?', line)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def _run_linter_json() -> dict[str, str] | None:
    """Ejecuta el linter --json y devuelve {rule_id: status}."""
    try:
        proc = subprocess.run(
            ["uv", "run", "python", "-m", "architecture_linter", "--root", str(ROOT), "--json"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[M11] linter falló: {exc}", file=sys.stderr)
        return None
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None
    return {r["rule_id"]: r["status"] for r in data.get("rules", [])}


def m12_report_structure(ctx: AuditContext) -> None:
    """M12 — estructura mínima del informe canónico (secciones obligatorias)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M12", "--report no provisto o inexistente")
        return
    text = ctx.report.read_text(encoding="utf-8")
    required = [
        "Executive Summary",
        "Matriz de Findings",
        "Matriz de Controles",
        "Matriz de Decisiones",
        "Integridad",
    ]
    for section in required:
        if not re.search(rf"^\s*#{{1,4}}\s+.*{re.escape(section)}", text, re.MULTILINE):
            ctx.err("M12", f"informe sin sección: {section!r}")


def m13_canonical_commands(ctx: AuditContext) -> None:
    """M13 — el informe declara comandos canónicos para los controles críticos."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M13", "--report no provisto o inexistente")
        return
    text = ctx.report.read_text(encoding="utf-8")
    for control_id, (cmd, _exit, interp) in CANONICAL_COMMANDS.items():
        if control_id not in {"DEPENDENCY_AUDIT", "ARCH_CONTRACTS", "ARCH_LINTER", "YAMLLINT"}:
            continue
        # El informe debe citar el comando exacto (o un fragmento distinguible).
        fragment = cmd.split("uv run ")[-1].split(" -")[0] if "uv run " in cmd else cmd.split()[0]
        if fragment and fragment not in text:
            ctx.err("M13", f"informe no cita comando canónico de {control_id} (esperado fragmento {fragment!r})")


def m14_tool_versions(ctx: AuditContext) -> None:
    """M14 — el informe declara versiones de herramientas (reproducibilidad)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M14", "--report no provisto o inexistente")
        return
    text = ctx.report.read_text(encoding="utf-8")
    if not re.search(r"(versión|version|herramienta|pip-audit \d)", text, re.IGNORECASE):
        ctx.err("M14", "informe sin bloque de versiones de herramientas")


def m15_invented_states(ctx: AuditContext) -> None:
    """M15 — detección de estados inventados en ADRs ("Superado", "Resuelto", ...)."""
    invented = {"superado", "resuelto", "aceptado implícitamente", "no aplica", "pendiente de estado"}
    states = load_adr_states(ctx.adrs_dir)
    for name, base in states.items():
        if base is not None and base not in ADR_BASE_STATES and base.lower() in invented:
            ctx.err("M15", f"{name}: estado inventado: {base!r}")


def m16_duplicate_ids(ctx: AuditContext) -> None:
    """M16 — findings duplicados por identificador (misma causa raíz con IDs distintos)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M16", "--report no provisto o inexistente")
        return
    report_ids = [f.fid for f in ctx.findings]
    dupes = {x for x in report_ids if report_ids.count(x) > 1}
    for d in sorted(dupes):
        ctx.err("M16", f"finding duplicado por identificador: {d}")


def m17_report_register_consistency(ctx: AuditContext) -> None:
    """M17 — consistencia entre informe canónico y registro (mismos IDs).

    Regla: todo finding del REGISTRO debe aparecer en el INFORME (error si falta).
    El informe puede contener findings adicionales (p.ej. del sistema de
    auditoría) que no viven en el registro de producto → aviso, no error.
    """
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M17", "--report no provisto o inexistente")
        return
    report_text = ctx.report.read_text(encoding="utf-8")
    report_findings = parse_report_findings(report_text)
    register_ids = {f.fid for f in ctx.findings}
    report_ids = set(report_findings)
    missing_in_report = register_ids - report_ids
    for fid in sorted(missing_in_report):
        ctx.err("M17", f"finding {fid} en registro pero ausente en informe")
    extra_in_report = report_ids - register_ids
    for fid in sorted(extra_in_report):
        msg = f"finding {fid} en informe pero no en el registro de producto (¿consolidación de otro sistema?)"
        ctx.warn("M17", msg)


def m18_severity_consistency(ctx: AuditContext) -> None:
    """M18 — consistencia de severidades (misma severidad en informe y registro)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M18", "--report no provisto o inexistente")
        return
    report_findings = parse_report_findings(ctx.report.read_text(encoding="utf-8"))
    by_id = {f.fid: f for f in ctx.findings}
    for fid, (sev, _cls) in report_findings.items():
        reg = by_id.get(fid)
        if reg and reg.severity != sev:
            ctx.err("M18", f"{fid}: severidad informe {sev} != registro {reg.severity}")


def m19_classification_consistency(ctx: AuditContext) -> None:
    """M19 — consistencia de clasificación (misma clasificación en informe y registro)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M19", "--report no provisto o inexistente")
        return
    report_findings = parse_report_findings(ctx.report.read_text(encoding="utf-8"))
    by_id = {f.fid: f for f in ctx.findings}
    for fid, (_sev, cls) in report_findings.items():
        reg = by_id.get(fid)
        if reg and reg.classification != cls:
            ctx.err("M19", f"{fid}: clasificación informe {cls} != registro {reg.classification}")


def m20_control_counts(ctx: AuditContext) -> None:
    """M20 — consistencia de control counts (informe declara counts correctos)."""
    if ctx.report is None or not ctx.report.exists():
        ctx.skip("M20", "--report no provisto o inexistente")
        return
    report_text = ctx.report.read_text(encoding="utf-8")
    counts = parse_report_control_counts(report_text)
    total = sum(counts.values())
    if total == 0:
        ctx.skip("M20", "sin filas de control parseables")
        return
    # El informe declara un resumen tipo "Controles = PASS(16) + ... = 23"
    declared = _parse_control_summary(report_text)
    if declared is not None:
        if declared != total:
            ctx.err("M20", f"controles declarados {declared} != filas {total}")
    # CONSISTENCIA FAIL↔FINDINGS: CONTROL FAIL != FINDING NUEVO.
    # Si hay controles FAIL y ninguno genera finding NUEVO, es consistente.
    # Si un control FAIL no tiene ningún finding asociado (por control), aviso.
    fails = counts.get("FAIL", 0)
    nuevos = sum(1 for f in ctx.findings if f.classification == "NUEVO")
    if fails > 0 and nuevos == 0:
        ctx.warn("M20", f"{fails} controles FAIL sin ningún finding NUEVO (verificar deduplicación)")


def _parse_control_summary(text: str) -> int | None:
    """Parse de la línea 'Controles = PASS(x) + ... = total' del informe."""
    m = re.search(r"Controles\s*=\s*.+?=\s*(\d+)\s*", text)
    if not m:
        # formato alterno: "Controles = PASS(16) + FAIL(4) ... = 23"
        joined = " ".join(line for line in text.splitlines() if line.startswith("Controles"))
        m = re.search(r"=\s*(\d+)\s*$", re.sub(r"\s+", " ", joined))
    return int(m.group(1)) if m else None


# ───────────────────────────────────────────────────────────────────────────
# Ejecución
# ───────────────────────────────────────────────────────────────────────────


def m21_canonical_audit_filenames(ctx: AuditContext) -> None:
    """M21 — todos los artefactos documentales de docs/audits deben usar naming canónico."""
    audits_dir = ROOT / "docs" / "audits"

    if not audits_dir.is_dir():
        ctx.err("M21", "no existe docs/audits/")
        return

    for path in sorted(audits_dir.glob("*.md")):
        name = path.name

        is_report = parse_canonical_report_filename(name) is not None
        is_register = parse_canonical_register_filename(name) is not None

        if is_report or is_register:
            continue

        ctx.err(
            "M21",
            f"{name}: nombre no canónico en docs/audits/ "
            "(esperado AUDIT_OCM_<slug>_<YYYY-MM-DD>[_<NN>].md "
            "o OCM_AUDIT_FINDINGS_<YYYY-MM-DD>_<slug>[_<NN>].md)",
        )


def load_registry(path: Path) -> dict | None:
    """Carga policies/registry.yaml; None si no existe o no parsea."""
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError:
        return None
    except yaml.YAMLError:
        return None
    if not isinstance(data, dict):
        return None
    return data


def m22_enforcement_ci_verification(ctx: AuditContext) -> None:
    """M22 -- enforcement obligatorio + verificacion contra CI real."""
    data = load_registry(ctx.registry)
    if data is None:
        ctx.err("M22", f"{ctx.registry}: no existe o no parsea")
        return
    for r in data.get("rules") or []:
        rid = r.get("id", "?")
        enforcement = r.get("enforcement")
        if enforcement not in ("blocking", "warning", "informational"):
            ctx.err("M22", f"{rid}: enforcement ausente o invalido ({enforcement!r})")
            continue
        if enforcement == "blocking":
            ci = r.get("ci") or {}
            job, command = ci.get("job"), ci.get("command")
            if not job or not command:
                ctx.err("M22", f"{rid}: enforcement=blocking requiere ci.job y ci.command")
                continue
            wf_dir = ROOT / ".github" / "workflows"
            found = wf_dir.is_dir() and any(job in wf.read_text(encoding="utf-8") for wf in wf_dir.glob("*.yml"))
            if not found:
                ctx.err("M22", f"{rid}: ci.job {job!r} no aparece en ningun workflow de .github/workflows/")


def m23_dead_rule_detection(ctx: AuditContext) -> None:
    """M23 -- dead rule: evidence.path referenciado no existe en disco."""
    data = load_registry(ctx.registry)
    if data is None:
        ctx.err("M23", f"{ctx.registry}: no existe o no parsea")
        return
    for r in data.get("rules") or []:
        rid = r.get("id", "?")
        ev_path = (r.get("evidence") or {}).get("path")
        if not ev_path:
            ctx.err("M23", f"{rid}: sin evidence.path -- sin implementacion referenciada")
            continue
        if not (ROOT / ev_path).exists():
            ctx.err("M23", f"{rid}: evidence.path {ev_path!r} no existe en disco (dead rule)")


def m24_waiver_expiration(ctx: AuditContext) -> None:
    """M24 -- waiver con expires ISO; expirado o sin ADR -> FAIL."""
    data = load_registry(ctx.registry)
    if data is None:
        ctx.err("M24", f"{ctx.registry}: no existe o no parsea")
        return
    today = date.today()
    for r in data.get("rules") or []:
        rid = r.get("id", "?")
        waiver = r.get("waiver") or {}
        if not waiver.get("allowed"):
            continue
        expires_raw = waiver.get("expires")
        if not expires_raw:
            ctx.err("M24", f"{rid}: waiver.allowed=true sin expires (ISO date obligatorio)")
            continue
        try:
            expires = date.fromisoformat(str(expires_raw))
        except ValueError:
            ctx.err("M24", f"{rid}: waiver.expires {expires_raw!r} no es fecha ISO valida")
            continue
        if expires < today:
            ctx.err("M24", f"{rid}: waiver expirado ({expires_raw})")
        if not waiver.get("adr"):
            ctx.err("M24", f"{rid}: waiver sin adr asociado")


def m25_orphan_adr_warning(ctx: AuditContext) -> None:
    """M25 -- ADR en related_adrs sin ninguna regla que lo referencie -> WARNING."""
    data = load_registry(ctx.registry)
    if data is None:
        ctx.err("M25", f"{ctx.registry}: no existe o no parsea")
        return
    referenced = {r.get("adr") for r in data.get("rules") or [] if r.get("adr")}
    for adr in data.get("related_adrs") or []:
        if adr not in referenced:
            ctx.warn("M25", f"{adr}: en related_adrs pero ninguna regla lo referencia")


ALL_RULES: Sequence[tuple[str, str, object]] = [
    ("M21", "Canonicalidad de nombres en docs/audits", m21_canonical_audit_filenames),
    ("M22", "Enforcement + verificacion CI", m22_enforcement_ci_verification),
    ("M23", "Dead rule detection", m23_dead_rule_detection),
    ("M24", "Waiver expiration", m24_waiver_expiration),
    ("M25", "ADR huerfano", m25_orphan_adr_warning),
    ("M01", "IDs únicos", m01_unique_ids),
    ("M02", "Clasificación enum", m02_classification_enum),
    ("M03", "Severidad enum", m03_severity_enum),
    ("M04", "Estados ADR", m04_adr_states),
    ("M05", "Referencias existen", m05_reference_existence),
    ("M06", "Findings ↔ tracking", m06_findings_tracking),
    ("M07", "Finding ↔ evidence", m07_finding_evidence),
    ("M08", "Finding ↔ control", m08_finding_control),
    ("M09", "Finding ↔ ADR", m09_finding_adr),
    ("M10", "Reconciliación matemática", m10_reconciliation),
    ("M11", "Golden state", m11_golden_state),
    ("M12", "Estructura informe", m12_report_structure),
    ("M13", "Comandos canónicos", m13_canonical_commands),
    ("M14", "Versiones de herramientas", m14_tool_versions),
    ("M15", "Estados inventados", m15_invented_states),
    ("M16", "Duplicados por ID", m16_duplicate_ids),
    ("M17", "Informe ↔ registro", m17_report_register_consistency),
    ("M18", "Consistencia severidad", m18_severity_consistency),
    ("M19", "Consistencia clasificación", m19_classification_consistency),
    ("M20", "Control counts", m20_control_counts),
]


def resolve_defaults(args: argparse.Namespace) -> AuditContext:
    register = Path(args.register) if args.register else _latest(ROOT / "docs" / "audits", "OCM_AUDIT_FINDINGS_")
    report = Path(args.report) if args.report else _latest(ROOT / "docs" / "audits", "AUDIT_OCM_FORENSIC_COMPLIANCE_")
    golden = Path(args.golden) if args.golden else ROOT / "tests" / "architecture_linter" / "test_golden.py"
    return AuditContext(
        register=register,
        report=report,
        tracking=Path(args.tracking) if args.tracking else ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=Path(args.adrs) if args.adrs else ROOT / "docs" / "architecture" / "decisions",
        golden=golden,
        registry=Path(args.registry) if args.registry else ROOT / "policies" / "registry.yaml",
    )


def _latest(directory: Path, prefix: str) -> Path:
    matches = sorted(directory.glob(f"{prefix}*.md")) if directory.is_dir() else []
    return matches[-1] if matches else directory / f"{prefix}MISSING.md"


def run_checks(ctx: AuditContext) -> int:
    if not ctx.register.exists():
        print(f"[audit-validator] FAIL — registro inexistente: {ctx.register}", file=sys.stderr)
        return 2
    try:
        ctx.findings = parse_findings(ctx.register.read_text(encoding="utf-8"))
    except OSError as exc:
        print(f"[audit-validator] error leyendo registro: {exc}", file=sys.stderr)
        return 2
    for name, desc, fn in ALL_RULES:
        try:
            fn(ctx)  # type: ignore[operator]
        except Exception as exc:  # noqa: BLE001
            ctx.err(name, f"excepción de validación: {exc}")

    for msg in ctx.errors:
        print(f"FAIL  {msg}")
    for msg in ctx.warnings:
        print(f"WARN  {msg}")
    for msg in ctx.skipped:
        print(f"SKIP  {msg}")

    if not ctx.errors:
        summary = (
            f"[audit-validator] PASS — {len(ctx.findings)} findings, "
            f"{len(ALL_RULES)} reglas mecánicas "
            f"(warnings {len(ctx.warnings)}, skipped {len(ctx.skipped)})"
        )
        print(summary)
        return 0
    print(f"[audit-validator] FAIL — {len(ctx.errors)} error(es) mecánico(s)")
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="OCM Audit Validator (M1..M20)")
    parser.add_argument("--register", help="registro de findings (default: último OCM_AUDIT_FINDINGS_*.md)")
    parser.add_argument("--report", help="informe canónico (default: último AUDIT_OCM_FORENSIC_COMPLIANCE_*.md)")
    parser.add_argument("--tracking", help="tracking.yaml")
    parser.add_argument("--adrs", help="directorio de ADRs")
    parser.add_argument("--golden", help="test_golden.py para M11")
    parser.add_argument("--registry", help="policies/registry.yaml")
    parser.add_argument("--versions", action="store_true", help="imprime versiones de herramientas canónicas y sale")
    parser.add_argument("--list-rules", action="store_true", help="imprime las reglas M1..M20 y sale")
    args = parser.parse_args(argv)

    if args.list_rules:
        for code, desc, _fn in ALL_RULES:
            print(f"{code}  {desc}")
        return 0

    if args.versions:
        _print_versions()
        return 0

    ctx = resolve_defaults(args)
    return run_checks(ctx)


def _print_versions() -> None:
    """Imprime las versiones de herramientas canónicas (reproducibilidad)."""
    commands = [
        ("pip-audit", ["uv", "run", "pip-audit", "--version"]),
        ("ruff", ["uv", "run", "ruff", "--version"]),
        ("mypy", ["uv", "run", "mypy", "--version"]),
        ("bandit", ["uv", "run", "bandit", "--version"]),
        ("pytest", ["uv", "run", "pytest", "--version"]),
        ("yamllint", ["uvx", "yamllint", "--version"]),
    ]
    for name, cmd in commands:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            ver = (proc.stdout + proc.stderr).strip().splitlines()
            print(f"{name}: {ver[0] if ver else 'n/d'}")
        except Exception as exc:  # noqa: BLE001
            print(f"{name}: error ({exc})")


if __name__ == "__main__":
    sys.exit(main())
