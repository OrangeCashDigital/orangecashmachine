"""
tests/architecture/test_policy_registry.py — Policy Registry (ADR-0031) + M22..M26.

Valida el contrato de ADR-0031 de forma determinista:

  - el registry parsa y tiene 16 reglas con IDs únicos
  - cada regla declara mechanism_type, enforcement, status y evidencia
  - guard_script/import_linter exigen test positive/negative resolubles (M22)
  - tool_gate exige ci.job + ci.command (M22/M23)
  - absence_gate exige evidencia de ausencia (M22)
  - waiver vigente → WARN, no FAIL (M25)
  - waiver expirado → FAIL (M25)
  - regla DEPRECATED con waiver → FAIL (M24)
  - ADR referenciado inexistente → FAIL (M26)
  - el repo real pasa el validador completo
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = ROOT / "scripts" / "audit_validator.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("audit_validator_policy", SCRIPT)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


av = _load_module()

REGISTRY = ROOT / "policies" / "registry.yaml"

GUARD_FILE = "tests/architecture/test_import_contracts.py::TestLiveExecutorFailClosed"
GUARD_POS = f"{GUARD_FILE}::test_executor_declares_stub_flag"
GUARD_NEG = f"{GUARD_FILE}::test_stub_means_true_flag"


def _ctx(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, registry: str) -> av.AuditContext:
    reg = tmp_path / "register.md"
    reg.write_text(
        "Resumen: CRITICAL 0 · HIGH 0 · MEDIUM 0 · LOW 0 · INFO 0 · **total 0**.\n\n- NUEVO: 0\n",
        encoding="utf-8",
    )
    fake = tmp_path / "registry.yaml"
    fake.write_text(registry, encoding="utf-8")
    monkeypatch.setattr(av, "POLICY_REGISTRY_PATH", fake)
    return av.AuditContext(
        register=reg,
        report=None,
        tracking=ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=ROOT / "docs" / "architecture" / "decisions",
        golden=None,
    )


def _errors(ctx: av.AuditContext) -> list[str]:
    return ctx.errors


def _registry_yaml(
    mech: str,
    *,
    tests: dict | None = None,
    evidence: str = "pyproject.toml",
    adr: str = "ADR-0031",
    **overrides: object,
) -> str:
    rule: dict = {
        "id": "R1",
        "name": "Regla sintética",
        "scope": "cross-cutting",
        "severity": "HIGH",
        "owner": "test",
        "enforcement": "blocking",
        "mechanism_type": mech,
        "tests": tests or {},
        "evidence": {"type": "backtest", "path": evidence} if evidence else {},
        "ci": {"job": "unit-tests", "command": "pytest -q"},
        "adr": adr,
        "status": "ACTIVE",
    }
    rule.update(overrides)
    data = {"schema_version": 1, "adr": "ADR-0031", "rules": [rule]}
    return yaml.safe_dump(data, sort_keys=False)


# ───────────────────────────────────────────────────────────────────────────
# Registry real (integridad del artefacto)
# ───────────────────────────────────────────────────────────────────────────


def test_registry_exists_and_parses() -> None:
    data = av.load_policy_registry()
    assert data is not None
    rules = data["rules"]
    assert len(rules) == 16
    ids = [r["id"] for r in rules]
    assert len(set(ids)) == len(ids) == 16


def test_registry_all_rules_have_contract_fields() -> None:
    for rule in av._registry_rules():
        assert rule.get("id")
        assert rule.get("mechanism_type") in av.MECHANISM_TYPES
        assert rule.get("enforcement") in av.ENFORCEMENT_LEVELS
        assert rule.get("status") in ("ACTIVE", "DEPRECATED")
        assert rule.get("evidence")


def test_guard_rules_have_resolvable_tests() -> None:
    """M22 — todo guard_script/import_linter con test positivo y negativo resolubles."""
    for rule in av._registry_rules():
        if rule.get("mechanism_type") in ("guard_script", "import_linter"):
            tests = rule.get("tests") or {}
            assert tests.get("positive"), f"{rule['id']} sin test positivo"
            assert tests.get("negative"), f"{rule['id']} sin test negativo"
            assert av._test_node_exists(tests["positive"]), f"{rule['id']} positive no resuelve"
            assert av._test_node_exists(tests["negative"]), f"{rule['id']} negative no resuelve"


def test_tool_gate_rules_have_ci_gate() -> None:
    """M22/M23 — tool_gate exige ci.job + ci.command."""
    for rule in av._registry_rules():
        if rule.get("mechanism_type") == "tool_gate":
            ci = rule.get("ci") or {}
            assert ci.get("job"), f"{rule['id']} sin ci.job"
            assert ci.get("command"), f"{rule['id']} sin ci.command"


def test_absence_gate_rule_deprecated_with_evidence() -> None:
    """M24 — absence_gate: única regla DEPRECATED con evidencia de ausencia."""
    for rule in av._registry_rules():
        if rule.get("mechanism_type") == "absence_gate":
            assert rule.get("status") == "DEPRECATED"
            assert rule.get("evidence")


def test_waiver_fields_are_complete_and_linked() -> None:
    """M25 — todo waiver presente es explícito, temporal y enlazado a deuda real."""
    for rule in av._registry_rules():
        waiver = rule.get("waiver")
        if not waiver:
            continue
        assert waiver.get("allowed") is True
        assert waiver.get("expires")
        assert waiver.get("motivo")
        assert waiver.get("adr").startswith("ADR-")
        ticket = waiver.get("ticket")
        assert ticket
        tracking = yaml.safe_load((ROOT / "docs" / "plans" / "tracking.yaml").read_text(encoding="utf-8"))
        tickets = {h["id"] for h in tracking["hallazgos"] if isinstance(h, dict)}
        assert ticket in tickets, f"waiver.ticket {ticket} no está en tracking.yaml"


# ───────────────────────────────────────────────────────────────────────────
# M22 — tests según mechanism_type
# ───────────────────────────────────────────────────────────────────────────


def test_m22_missing_guard_test_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        tests={"positive": GUARD_POS},
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m22_policy_registry_tests(ctx)
    assert any("R1: falta test negative" in e for e in _errors(ctx))


def test_m22_guard_test_inexistent_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        tests={
            "positive": f"{GUARD_FILE}::test_does_not_exist",
            "negative": GUARD_POS,
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m22_policy_registry_tests(ctx)
    assert any("test positive no existe" in e for e in _errors(ctx))


def test_m22_tool_gate_without_ci_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml("tool_gate", ci={})
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m22_policy_registry_tests(ctx)
    assert any("tool_gate exige ci.job y ci.command" in e for e in _errors(ctx))


def test_m22_absence_gate_without_evidence_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml("absence_gate", evidence="")
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m22_policy_registry_tests(ctx)
    assert any("absence_gate exige evidencia" in e for e in _errors(ctx))


def test_m22_invalid_mechanism_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml("mystery")
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m22_policy_registry_tests(ctx)
    assert any("mechanism_type inválido" in e for e in _errors(ctx))


# ───────────────────────────────────────────────────────────────────────────
# M24 — regla muerta
# ───────────────────────────────────────────────────────────────────────────


def test_m24_deprecated_with_waiver_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "absence_gate",
        status="DEPRECATED",
        waiver={
            "allowed": True,
            "expires": "2099-01-01",
            "motivo": "x",
            "adr": "ADR-0031",
            "ticket": "B-61",
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m24_policy_registry_dead(ctx)
    assert any("regla DEPRECATED no puede tener waiver" in e for e in _errors(ctx))


# ───────────────────────────────────────────────────────────────────────────
# M25 — semántica de waiver
# ───────────────────────────────────────────────────────────────────────────


def test_m25_valid_waiver_is_warn_not_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        tests={
            "positive": GUARD_POS,
            "negative": GUARD_NEG,
        },
        waiver={
            "allowed": True,
            "expires": "2099-01-01",
            "motivo": "deuda temporal",
            "adr": "ADR-0031",
            "ticket": "B-61",
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m25_policy_registry_waivers(ctx)
    assert not _errors(ctx)
    assert any("waiver vigente" in w for w in ctx.warnings)


def test_m25_expired_waiver_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        waiver={
            "allowed": True,
            "expires": "2000-01-01",
            "motivo": "deuda temporal",
            "adr": "ADR-0031",
            "ticket": "B-61",
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m25_policy_registry_waivers(ctx)
    assert any("waiver EXPIRADO" in e for e in _errors(ctx))


def test_m25_waiver_without_ticket_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        waiver={
            "allowed": True,
            "expires": "2099-01-01",
            "motivo": "deuda temporal",
            "adr": "ADR-0031",
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m25_policy_registry_waivers(ctx)
    assert any("waiver.ticket obligatorio" in e for e in _errors(ctx))


def test_m25_waiver_ticket_inexistent_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml(
        "guard_script",
        waiver={
            "allowed": True,
            "expires": "2099-01-01",
            "motivo": "deuda temporal",
            "adr": "ADR-0031",
            "ticket": "B-9999",
        },
    )
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m25_policy_registry_waivers(ctx)
    assert any("no existe en tracking.yaml" in e for e in _errors(ctx))


# ───────────────────────────────────────────────────────────────────────────
# M26 — ADR huérfano
# ───────────────────────────────────────────────────────────────────────────


def test_m26_inexistent_adr_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml("tool_gate", adr="ADR-9999")
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m26_policy_registry_adrs(ctx)
    assert any("adr referenciado no existe: ADR-9999" in e for e in _errors(ctx))


def test_m26_existent_adr_passes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reg = _registry_yaml("tool_gate", adr="ADR-0031")
    ctx = _ctx(tmp_path, monkeypatch, reg)
    av.m26_policy_registry_adrs(ctx)
    assert not _errors(ctx)


# ───────────────────────────────────────────────────────────────────────────
# Gate completo contra el repo real
# ───────────────────────────────────────────────────────────────────────────


def test_full_repo_policy_registry_passes() -> None:
    """M22..M26 contra el registry real no generan errores (solo el WARN de R7)."""
    ctx = av.AuditContext(
        register=ROOT / "docs" / "plans" / "tracking.yaml",
        report=None,
        tracking=ROOT / "docs" / "plans" / "tracking.yaml",
        adrs_dir=ROOT / "docs" / "architecture" / "decisions",
        golden=None,
    )
    av.m22_policy_registry_tests(ctx)
    av.m23_policy_registry_enforcement(ctx)
    av.m24_policy_registry_dead(ctx)
    av.m25_policy_registry_waivers(ctx)
    av.m26_policy_registry_adrs(ctx)
    assert not ctx.errors
    # Sin waivers vigentes tras la activación de R7 (B-61 cerrado 2026-08-19).
    assert not any("waiver vigente" in w for w in ctx.warnings)
