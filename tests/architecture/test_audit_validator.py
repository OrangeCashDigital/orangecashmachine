"""
tests/architecture/test_audit_validator.py — Audit Validator (M1..M20).

Demuestra que las reglas mecánicas del sistema de auditoría OCM son
ejecutables y deterministas:
  - finding duplicado            → FAIL (M1)
  - enum inválido                → FAIL (M2)
  - severidad inválida           → FAIL (M3)
  - estado ADR inventado         → FAIL (M4/M15)
  - contadores sin cuadrar       → FAIL (M10)
  - referencia tracking faltante → FAIL (M6)
  - evidencia faltante           → FAIL (M7)
  - golden mismatch              → FAIL (M11)
  - informe válido               → PASS
  - reconciliación válida        → PASS
  - golden PASS con FAIL/PARTIAL conocidos → PASS (no se interpreta como compliance)
  - CONTROL FAIL con tracking    → NO NUEVO automático (REVALIDADO)
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = ROOT / "scripts" / "audit_validator.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("audit_validator", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


av = _load_module()


def _ctx(tmp_path: Path, register: str, report: str | None = None) -> av.AuditContext:
    reg = tmp_path / "register.md"
    reg.write_text(register, encoding="utf-8")
    rep = None
    if report is not None:
        rep = tmp_path / "report.md"
        rep.write_text(report, encoding="utf-8")
    return av.AuditContext(
        register=reg,
        report=rep,
        tracking=ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=ROOT / "docs" / "architecture" / "decisions",
        golden=None,
    )


def _findings_only(ctx: av.AuditContext) -> None:
    ctx.findings = av.parse_findings(ctx.register.read_text(encoding="utf-8"))


VALID_REGISTER = """\
# REGISTER
Resumen: CRITICAL 1 · HIGH 0 · MEDIUM 0 · LOW 0 · INFO 0 · **total 1**.

- NUEVO: 1

## F-001 — Algo

Severity: CRITICAL
Status: OPEN
Classification: NUEVO
Control: Test

Evidence:
- `docs/plans/tracking.yaml`

Traceability:
- Tracking: NOT_TRACED · ADR: NOT_TRACED · Implementation: NOT_TRACED
"""


def test_m01_duplicate_id_fails(tmp_path: Path) -> None:
    reg = VALID_REGISTER + "\n## F-001 — Duplicado\n\nSeverity: HIGH\nStatus: OPEN\nClassification: NUEVO\n"
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m01_unique_ids(ctx)
    assert any(e.startswith("[M1]") for e in ctx.errors)


def test_m02_invalid_classification_fails(tmp_path: Path) -> None:
    reg = VALID_REGISTER.replace("Classification: NUEVO", "Classification: SUPERADO")
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m02_classification_enum(ctx)
    assert any(e.startswith("[M2]") for e in ctx.errors)


def test_m03_invalid_severity_fails(tmp_path: Path) -> None:
    reg = VALID_REGISTER.replace("Severity: CRITICAL", "Severity: BLOQUEANTE")
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m03_severity_enum(ctx)
    assert any(e.startswith("[M3]") for e in ctx.errors)


def test_m04_invented_adr_state_fails(tmp_path: Path) -> None:
    adr_dir = tmp_path / "decisions"
    adr_dir.mkdir()
    (adr_dir / "ADR-9999-prueba.md").write_text("# ADR-9999\n\n**Estado:** Superado\n\n## Decisión\n", encoding="utf-8")
    ctx = _ctx(tmp_path, VALID_REGISTER)
    ctx.adrs_dir = adr_dir
    av.m04_adr_states(ctx)
    assert any(e.startswith("[M4]") for e in ctx.errors)


def test_m10_mismatched_counters_fails(tmp_path: Path) -> None:
    # total declarado 2, pero solo 1 finding parseable
    reg = VALID_REGISTER.replace("**total 1**", "**total 2**")
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m10_reconciliation(ctx)
    assert any(e.startswith("[M10]") for e in ctx.errors)


def test_m06_missing_tracking_reference_fails(tmp_path: Path) -> None:
    reg = VALID_REGISTER.replace("Tracking: NOT_TRACED", "Tracking: B-99999 (EN_CURSO)")
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m06_findings_tracking(ctx)
    assert any(e.startswith("[M6]") for e in ctx.errors)


def test_m07_missing_evidence_fails(tmp_path: Path) -> None:
    reg = VALID_REGISTER.replace("Evidence:\n- `docs/plans/tracking.yaml`\n", "Evidence:\n")
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m07_finding_evidence(ctx)
    assert any(e.startswith("[M7]") for e in ctx.errors)


def test_m11_golden_mismatch_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    golden = tmp_path / "test_golden.py"
    golden.write_text(
        '# GOLDEN_EXPECTED: dict[str, Status] = {\n    "ARCH-001": Status.PASS,  # inventado\n}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        av,
        "_run_linter_json",
        lambda: {"ARCH-001": "FAIL", "ARCH-002": "FAIL", "ARCH-003": "PARTIAL", "ARCH-006": "PASS"},
    )
    ctx = _ctx(tmp_path, VALID_REGISTER)
    ctx.golden = golden
    av.m11_golden_state(ctx)
    assert any("mismatch" in e for e in ctx.errors)


def test_m11_golden_with_known_fail_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Golden con FAIL/PARTIAL conocidos (deuda gobernada) = PASS, no se
    # interpreta como conformidad (semántica GOLDEN PASS != COMPLIANT).
    golden = tmp_path / "test_golden.py"
    golden.write_text(
        '# GOLDEN_EXPECTED: dict[str, Status] = {\n    "ARCH-001": Status.FAIL, "ARCH-006": Status.PASS,\n}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(av, "_run_linter_json", lambda: {"ARCH-001": "FAIL", "ARCH-006": "PASS"})
    ctx = _ctx(tmp_path, VALID_REGISTER)
    ctx.golden = golden
    av.m11_golden_state(ctx)
    assert not ctx.errors


def test_m12_valid_report_structure_passes(tmp_path: Path) -> None:
    report = """\
# Informe

## 1. Executive Summary
Resumen.

## 18. Matriz de Findings

## 19. Matriz de Controles

## 20. Matriz de Decisiones

## 24. Integridad
"""
    ctx = _ctx(tmp_path, VALID_REGISTER, report=report)
    av.m12_report_structure(ctx)
    assert not ctx.errors


def test_m20_control_counts_ok(tmp_path: Path) -> None:
    report = """\
# Informe

## 19. Matriz de Controles (4)

| Control | Comando | Resultado | Estado |
|---|---|---|---|
| A | x | ok | **PASS** |
| B | y | ko | **FAIL** |
| C | z | ok | **PASS** |
| D | w | ok | **PASS** |

Controles = PASS(3) + FAIL(1) = 4 ✅
"""
    ctx = _ctx(tmp_path, VALID_REGISTER, report=report)
    av.m20_control_counts(ctx)
    assert not ctx.errors


def test_control_fail_with_tracking_is_revalidado_not_nuevo(tmp_path: Path) -> None:
    """CONTROL FAIL con tracking = REVALIDADO, NO NUEVO automático (M6/M10)."""
    reg = """\
# REGISTER
Resumen: CRITICAL 0 · HIGH 1 · MEDIUM 0 · LOW 0 · INFO 0 · **total 1**.

- REVALIDADO: 1

## F-ARCH-01 — Multi-owner del estado de posición

Severity: HIGH
Status: OPEN
Classification: REVALIDADO
Control: Architecture — Position State Ownership

Evidence:
- ARCH-001 FAIL

Traceability:
- Tracking: B-15 (EN_CURSO) · ADR: ADR-0006 · Implementation: PARCIAL
"""
    ctx = _ctx(tmp_path, reg)
    _findings_only(ctx)
    av.m06_findings_tracking(ctx)
    av.m10_reconciliation(ctx)
    assert not ctx.errors  # B-15 existe en tracking.yaml → sin error M6; suma 1 → sin error M10


def test_full_repo_audit_validator_passes() -> None:
    """El validador pasa contra el repo real (registro canónico + informe)."""
    audits_dir = ROOT / "docs" / "audits"
    register = sorted(audits_dir.glob("OCM_AUDIT_FINDINGS_*.md"))[-1]
    report = av._matching_report_for_register(audits_dir, register)
    assert report is not None, f"sin informe emparejado para {register.name}"
    ctx = av.AuditContext(
        register=register,
        report=report,
        tracking=ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=ROOT / "docs" / "architecture" / "decisions",
        golden=None,
    )
    code = av.run_checks(ctx)
    assert code == 0


class TestMatchingReportForRegister:
    """M17 -- vinculo registro<->informe: 'Fuente primaria' declarada
    tiene prioridad sobre el fallback por nombre de archivo (regresion
    2026-08-29: un slug con punto extra rompia el fallback y el rename
    del archivo lo escondia en vez de arreglarlo)."""

    def test_usa_fuente_primaria_declarada_cuando_existe(self, tmp_path):
        av = _load_module()
        audits = tmp_path / "docs" / "audits"
        audits.mkdir(parents=True)
        report = audits / "AUDIT_OCM_algo_2026-08-28.md"
        report.write_text("# informe\n", encoding="utf-8")
        register = audits / "OCM_AUDIT_FINDINGS_2026-08-28_algo.md"
        register.write_text(
            "# registro\n\n**Fuente primaria:** `docs/audits/" + report.name + "`\n",
            encoding="utf-8",
        )
        assert av._matching_report_for_register(audits, register) == report

    def test_fuente_primaria_declarada_pero_inexistente_no_usa_fallback_de_nombre(self, tmp_path):
        av = _load_module()
        audits = tmp_path / "docs" / "audits"
        audits.mkdir(parents=True)
        (audits / "AUDIT_OCM_algo_2026-08-28.md").write_text("# informe distinto\n", encoding="utf-8")
        register = audits / "OCM_AUDIT_FINDINGS_2026-08-28_algo.md"
        register.write_text(
            "# registro\n\n**Fuente primaria:** `docs/audits/AUDIT_OCM_NO_EXISTE.md`\n",
            encoding="utf-8",
        )
        assert av._declared_report_for_register(register) is None

    def test_sin_fuente_primaria_usa_fallback_por_slug_fecha(self, tmp_path):
        av = _load_module()
        audits = tmp_path / "docs" / "audits"
        audits.mkdir(parents=True)
        report = audits / "AUDIT_OCM_algo_2026-08-28.md"
        report.write_text("# informe\n", encoding="utf-8")
        register = audits / "OCM_AUDIT_FINDINGS_2026-08-28_algo.md"
        register.write_text("# registro sin fuente primaria\n", encoding="utf-8")
        assert av._matching_report_for_register(audits, register) == report

    def test_slug_con_punto_extra_no_rompe_via_fuente_primaria(self, tmp_path):
        av = _load_module()
        audits = tmp_path / "docs" / "audits"
        audits.mkdir(parents=True)
        report = audits / "AUDIT_OCM_data-plane-streaming_2026-08-28.md"
        report.write_text("# informe\n", encoding="utf-8")
        register = audits / "OCM_AUDIT_FINDINGS_2026-08-28_data-plane-streaming.yaml.md"
        register.write_text(
            "# registro\n\n**Fuente primaria:** `docs/audits/" + report.name + "`\n",
            encoding="utf-8",
        )
        assert av._matching_report_for_register(audits, register) == report
