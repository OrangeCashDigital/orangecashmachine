"""
tests/architecture/test_m26_staged_governance.py — M26 Staged Ports Governance.

M26 (ADR-0044) valida de forma fail-closed el registro staged completo:
identidad Port↔Protocol, consumer reality (Caso I), registry↔ADR (sección),
registry↔tracking, fases, owner y reutiliza el core B1 (gramática, campos
obligatorios, expiry, duplicados). Ningún test hardcodea nombres del repo real
(no se usa MarketDataViewPort/market_data_view/B-MD-004).
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

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

PORT_FILE = "packages/market_data/ports/outbound/orphan_port.py"
CONSUMER_FILE = "packages/market_data/application/use_cases/consumer.py"

PORT_CODE = "from typing import Protocol\n\n\nclass OrphanPort(Protocol):\n    pass\n"

CONSUMER_CODE = "def handler(p: OrphanPort) -> None:\n    return\n"

ADR_9999 = (
    "# ADR-9999\n"
    "\n"
    "**Estado:** Aceptado\n"
    "\n"
    "## Contexto\n"
    "\n"
    "### 2.1 OrphanPort\n"
    "\n"
    "OrphanPort es un Port sin consumidor en Fase 1; se implementa en Fase 2.\n"
)

TRACKING = "hallazgos:\n  - id: B-004\n    fase: F2\n    estado: PENDIENTE\n"

ENTRY = (
    "  - port: OrphanPort\n"
    "    file: packages/market_data/ports/outbound/orphan_port.py\n"
    "    adr: ADR-9999\n"
    "    adr_section: '2.1'\n"
    "    current_phase: F1\n"
    "    target_phase: F2\n"
    "    consumer: B-004\n"
    "    tracking_item: B-004\n"
    "    owner: market_data\n"
    "    expires: '2099-12-31'\n"
    "    justification: staged de ejemplo\n"
    "    revocation: revocación de ejemplo\n"
)


def _registry(entries: str, *, rules: bool = True) -> str:
    rules_block = ""
    if rules:
        rules_block = (
            "rules:\n"
            "  - id: R1\n"
            "    name: regla de ejemplo\n"
            "    owner: market_data\n"
            "    enforcement: blocking\n"
            "    adr: ADR-0044\n"
        )
    return (
        "schema_version: 1\n"
        "adr: ADR-0031\n"
        "created: '2026-08-19'\n"
        "modified: '2026-09-24'\n"
        "related_adrs:\n"
        "  - ADR-0044\n"
        "staged_ports:\n" + entries + rules_block
    )


def _build(
    tmp_path: Path,
    registry_text: str,
    *,
    registry: Path | None = None,
    port_code: str = PORT_CODE,
    adr: str = ADR_9999,
    adr_name: str = "ADR-9999-port.md",
    tracking: str = TRACKING,
    consumer: bool = False,
) -> av.AuditContext:
    root = tmp_path
    policies = root / "policies"
    policies.mkdir(exist_ok=True)
    adrs = root / "adrs"
    adrs.mkdir(exist_ok=True)
    if registry_text is not None:
        (policies / "registry.yaml").write_text(registry_text, encoding="utf-8")
    (adrs / adr_name).write_text(adr, encoding="utf-8")
    (root / "tracking.yaml").write_text(tracking, encoding="utf-8")
    port = root / PORT_FILE
    port.parent.mkdir(parents=True, exist_ok=True)
    port.write_text(port_code, encoding="utf-8")
    if consumer:
        consumer_file = root / CONSUMER_FILE
        consumer_file.parent.mkdir(parents=True, exist_ok=True)
        consumer_file.write_text(CONSUMER_CODE, encoding="utf-8")
    reg = registry if registry is not None else policies / "registry.yaml"
    return av.AuditContext(
        register=root / "register.md",
        report=None,
        tracking=root / "tracking.yaml",
        adrs_dir=root / "adrs",
        golden=None,
        registry=reg,
        root=root,
    )


def _m26_errors(ctx: av.AuditContext) -> list[str]:
    av.m26_staged_ports_governance(ctx)
    return list(ctx.errors)


class TestM26Valid:
    """Registro staged íntegro y coherente → PASS sin errores."""

    def test_caso_F_entrada_valida_con_arquitectura_coherente(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY)))
        assert errors == []

    def test_pas_w_registry_vacio_no_inventa_obligacion(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry("")))
        assert errors == []

    def test_k_expiry_futura_aceptada(self, tmp_path, monkeypatch):
        monkeypatch.setattr(av, "_m26_today", lambda: date(2026, 9, 24))
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY)))
        assert errors == []


class TestM26FailClosed:
    """Cualquier imposibilidad de demostrar gobernanza válida → FAIL."""

    def test_b_registry_inexistente(self, tmp_path):
        ctx = _build(tmp_path, _registry(ENTRY), registry=tmp_path / "policies" / "missing.yaml")
        errors = _m26_errors(ctx)
        assert any("registry staged ilegible o inválido" in e for e in errors)

    def test_u_sin_clave_staged_ports_falla(self, tmp_path):
        body = _registry(ENTRY).replace("staged_ports:\n", "")
        errors = _m26_errors(_build(tmp_path, body))
        assert any("sin clave 'staged_ports'" in e for e in errors)

    def test_n_campo_no_permitido_falla_parse(self, tmp_path):
        body = ENTRY + "    extra: campo_no_permitido\n"
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("campo no permitido" in e for e in errors)

    def test_l_campo_obligatorio_ausente(self, tmp_path):
        body = ENTRY.replace("    owner: market_data\n", "")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("campo obligatorio ausente: 'owner'" in e for e in errors)

    def test_d_archivo_port_inexistente(self, tmp_path):
        body = ENTRY.replace(PORT_FILE, "packages/market_data/ports/outbound/missing.py")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("archivo referenciado no existe" in e for e in errors)

    def test_m_entrada_duplicada(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY + ENTRY)))
        assert any("duplicado en el registry: 'OrphanPort'" in e for e in errors)


class TestM26Expiry:
    def test_i_expired_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(av, "_m26_today", lambda: date(2026, 9, 24))
        body = ENTRY.replace("expires: '2099-12-31'", "expires: '2020-01-01'")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("expirada" in e for e in errors)

    def test_j_expires_hoy_requiere_renovacion(self, tmp_path, monkeypatch):
        monkeypatch.setattr(av, "_m26_today", lambda: date(2026, 9, 24))
        body = ENTRY.replace("expires: '2099-12-31'", "expires: '2026-09-24'")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("coincide con la evaluación" in e for e in errors)


class TestM26Identity:
    def test_e_protocol_inexistente_en_archivo(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY), port_code=PORT_CODE.replace("OrphanPort", "FooPort")))
        assert any("Protocol/contract no declarado en" in e for e in errors)

    def test_v_declaracion_no_es_protocol(self, tmp_path):
        plain = "class OrphanPort:\n    pass\n"
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY), port_code=plain))
        assert any("no es un Protocol" in e for e in errors)


class TestM26Adr:
    def test_f_adr_inexistente(self, tmp_path):
        body = ENTRY.replace("adr: ADR-9999", "adr: ADR-0000")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("ADR 'ADR-0000' inexistente" in e for e in errors)

    def test_g_seccion_inexistente(self, tmp_path):
        body = ENTRY.replace("adr_section: '2.1'", "adr_section: '§9.9'")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("sección '§9.9' inexistente" in e for e in errors)

    def test_q_seccion_sin_mencion_del_port(self, tmp_path):
        adr = "# ADR-9999\n\n### 2.1 OtroPort\n\nalgo sin relación.\n"
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY), adr=adr))
        assert any("no menciona el Port" in e for e in errors)


class TestM26Tracking:
    def test_h_tracking_item_inexistente(self, tmp_path):
        body = ENTRY.replace("tracking_item: B-004", "tracking_item: B-777")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("tracking_item 'B-777' inexistente" in e for e in errors)

    def test_s_target_phase_diverge_de_tracking(self, tmp_path):
        body = ENTRY.replace("target_phase: F2", "target_phase: F3")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("target_phase F3 != fase F2" in e for e in errors)

    def test_estado_cerrado_staged_obsoleto(self, tmp_path):
        tracking = TRACKING.replace("estado: PENDIENTE", "estado: HECHO")
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY), tracking=tracking))
        assert any("consumidor ya realizado" in e for e in errors)


class TestM26Phases:
    def test_r_phase_token_invalido(self, tmp_path):
        body = ENTRY.replace("current_phase: F1", "current_phase: X1")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("fases inválidas" in e for e in errors)

    def test_r_target_no_estrictamente_posterior(self, tmp_path):
        body = ENTRY.replace("current_phase: F1", "current_phase: F3").replace("target_phase: F2", "target_phase: F2")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("no es estrictamente anterior" in e for e in errors)


class TestM26Owner:
    def test_t_owner_fuera_de_categorias_del_registry(self, tmp_path):
        body = ENTRY.replace("owner: market_data", "owner: research")
        errors = _m26_errors(_build(tmp_path, _registry(body)))
        assert any("owner 'research' no es categoría" in e for e in errors)


class TestM26ConsumerReality:
    def test_o_caso_I_consumidor_real_con_staged(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry(ENTRY), consumer=True))
        assert any("consumidor real detectado" in e for e in errors)

    def test_p_consumidor_sin_entrada_staged_no_inventa(self, tmp_path):
        errors = _m26_errors(_build(tmp_path, _registry(""), consumer=True))
        assert errors == []
