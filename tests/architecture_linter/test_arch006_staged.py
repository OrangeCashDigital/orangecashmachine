"""Integración ARCH-006 ↔ Staged Ports (FASE B2).

Cobertura del port huérfano vía `policies/registry.yaml` consumiendo el núcleo
`architecture_linter.staged_ports` (B1): casos A–I, fail-closed y límites
delegados a M26/B3 (resolución ADR/tracking y Caso I completo no son
responsabilidad de ARCH-006). Queda prohibido un bypass hardcodeado
(`MarketDataViewPort`/`market_data_view` en RESERVED o en código fuente).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import architecture_linter.rules.arch_006 as arch006
from architecture_linter.models import Status
from architecture_linter.rules.arch_006 import Arch006Rule

ORPHAN = "packages/market_data/ports/outbound/orphan_port.py"
ORPHAN_CODE = "from typing import Protocol\nclass OrphanPort(Protocol):\n    pass\n"

CONSUMER = "packages/market_data/application/use_cases/consumer.py"
CONSUMER_CODE = "def f(p: OrphanPort) -> None:\n    return None\n"

FILE = "packages/market_data/ports/outbound/orphan_port.py"

REGISTRY_EMPTY = (
    "schema_version: 1\n"
    "adr: ADR-0031\n"
    "created: '2026-08-19'\n"
    "modified: '2026-09-24'\n"
    "related_adrs:\n"
    "  - ADR-0044\n"
    "staged_ports: []\n"
    "rules:\n"
    "  - id: R1\n    name: x\n"
)

REGISTRY_NO_STAGED = (
    "schema_version: 1\n"
    "adr: ADR-0031\n"
    "created: '2026-08-19'\n"
    "modified: '2026-09-24'\n"
    "related_adrs:\n"
    "  - ADR-0044\n"
    "rules:\n"
    "  - id: R1\n    name: x\n"
)


def _entry_body(**overrides: str) -> str:
    fields = {
        "port": "OrphanPort",
        "file": FILE,
        "adr": "ADR-0028",
        "adr_section": "§3.6",
        "current_phase": "F1",
        "target_phase": "F2",
        "consumer": "B-MD-004",
        "tracking_item": "B-MD-004",
        "owner": "market_data",
        "expires": "2099-12-31",
        "justification": "justificación",
        "revocation": "revocación",
    }
    fields.update(overrides)
    return "".join(f"    {k}: {v}\n" for k, v in fields.items())


def _registry(*bodies: str) -> str:
    items = "".join(f"  - {body}" for body in bodies)
    return (
        "schema_version: 1\n"
        "adr: ADR-0031\n"
        "created: '2026-08-19'\n"
        "modified: '2026-09-24'\n"
        "related_adrs:\n"
        "  - ADR-0044\n"
        f"staged_ports:\n{items}"
        "rules:\n"
        "  - id: R1\n    name: x\n"
    )


def _run(ctx) -> "object":
    return Arch006Rule().run(ctx)


# ── Caso A — consumidor real ────────────────────────────────────────────────


def test_arch006_caso_a_consumidor_real_pasa(make_repo) -> None:
    ctx = make_repo(
        {
            ORPHAN: ORPHAN_CODE,
            CONSUMER: CONSUMER_CODE,
            "policies/registry.yaml": REGISTRY_EMPTY,
        }
    )
    assert _run(ctx).status == Status.PASS


# ── Caso B — huérfano sin staged ────────────────────────────────────────────


def test_arch006_caso_b_huerfano_sin_staged_falla(make_repo) -> None:
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": REGISTRY_EMPTY})
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert all("staged" not in f.message.lower() for f in result.findings)


# ── Caso C — staged válido ──────────────────────────────────────────────────


def test_arch006_caso_c_staged_valido_pasa(make_repo) -> None:
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body())})
    result = _run(ctx)
    assert result.status == Status.PASS
    found = [f for f in result.findings if f.symbol == "OrphanPort"]
    assert found and found[0].status == Status.PASS
    assert any("staged coverage valid" in e.text for f in found for e in f.evidence)


# ── Caso D — staged expirado ────────────────────────────────────────────────


def test_arch006_caso_d_staged_expirado_falla(make_repo, monkeypatch) -> None:
    monkeypatch.setattr(arch006, "_evaluation_date", lambda: date(2026, 9, 24))
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body(expires="2020-01-01"))})
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert any("staged coverage invalid" in e.text for f in result.findings for e in f.evidence)
    assert any("expirada" in e.text for f in result.findings for e in f.evidence)


# ── Caso E — ADR irresoluble (límite delegado a M26/B3) ─────────────────────


def test_arch006_caso_e_adr_irresoluble_no_es_responsabilidad_b2(make_repo) -> None:
    # ARCH-006 solo decide cobertura del port; resolución ADR ↔ registry es M26.
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body(adr="ADR-9999"))})
    assert _run(ctx).status == Status.PASS


# ── Caso F — tracking irresoluble (límite delegado a M26/B3) ────────────────


def test_arch006_caso_f_tracking_irresoluble_no_es_responsabilidad_b2(make_repo) -> None:
    # Igual que el Caso E: tracking/ADR no está en el core B1; lo enforce M26/B3.
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body(tracking_item="B-XXX"))})
    assert _run(ctx).status == Status.PASS


# ── Caso G — campo obligatorio faltante / vacío ─────────────────────────────


def test_arch006_caso_g_campo_obligatorio_vacio_falla(make_repo) -> None:
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body(owner=""))})
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert any("campo obligatorio vacío" in e.text for f in result.findings for e in f.evidence)


# ── Caso H — entrada duplicada ──────────────────────────────────────────────


def test_arch006_caso_h_duplicado_falla(make_repo) -> None:
    ctx = make_repo(
        {
            ORPHAN: ORPHAN_CODE,
            "policies/registry.yaml": _registry(_entry_body(), _entry_body()),
        }
    )
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert any("duplicada" in e.text for f in result.findings for e in f.evidence)


# ── Caso I — consumidor real + staged registry ──────────────────────────────


def test_arch006_caso_i_consumidor_mas_staged_pasa_como_no_huerfano(make_repo) -> None:
    # ARCH-006: PASS como no-huérfano (hay consumidor real).
    # La inconsistencia registry↔AST (consumidor + staged) es M26, no de B2.
    ctx = make_repo(
        {
            ORPHAN: ORPHAN_CODE,
            CONSUMER: CONSUMER_CODE,
            "policies/registry.yaml": _registry(_entry_body()),
        }
    )
    assert _run(ctx).status == Status.PASS


# ── Fail-closed del registry ────────────────────────────────────────────────


def test_arch006_fail_closed_registry_invalido_falla(make_repo) -> None:
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": REGISTRY_NO_STAGED})
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert any("registry staged no disponible" in f.message for f in result.findings)
    assert any("staged coverage invalid" in e.text for f in result.findings for e in f.evidence)


def test_arch006_huerfano_recibe_solo_cobertura_por_entrada_propia(make_repo) -> None:
    ctx = make_repo({ORPHAN: ORPHAN_CODE, "policies/registry.yaml": _registry(_entry_body(port="OtherPort"))})
    result = _run(ctx)
    assert result.status == Status.FAIL
    assert result.findings[0].symbol == "OrphanPort"


# ── Anti-bypass: único mecanismo nuevo es el registry staged ────────────────


def test_arch006_anti_bypass_reserved_legacy_intacto() -> None:
    assert {"feature_reader"} == arch006.RESERVED


def test_arch006_sin_bypass_hardcodeado_de_marketdata() -> None:
    import ast

    source = Path(arch006.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    string_literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                continue  # docstrings/bare literals no son lógica
            string_literals.add(node.value)
    assert "MarketDataViewPort" not in string_literals
    assert "market_data_view" not in string_literals
