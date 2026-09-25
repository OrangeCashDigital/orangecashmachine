"""Tests del núcleo semántico de Staged Ports (architecture_linter/staged_ports.py).

Cubren: registro vacío/válido, gramática constreñida fail-closed, campos
obligatorios, expiración, duplicados, integridad de archivo y no-bypass.

La resolución de identidad (port -> clase Protocol) y el Caso I (consumidor real)
se delegan a M26/B3: aquí se documenta el límite, no se improvisa AST.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from architecture_linter.staged_ports import (
    REQUIRED_FIELDS,
    ParseResult,
    RegistryReport,
    get_staged_entry,
    load_staged_registry,
    parse_staged_registry,
    validate_entry,
    validate_registry,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

TODAY = date(2026, 9, 24)

FIELD_LINES: tuple[tuple[str, str], ...] = (
    ("port", "MercadoVistaPort"),
    ("file", "packages/market_data/ports/outbound/mercado_vista.py"),
    ("adr", "ADR-0028"),
    ("adr_section", '"§3.6"'),
    ("current_phase", "F1"),
    ("target_phase", "F2"),
    ("consumer", "B-MD-004"),
    ("tracking_item", "B-MD-004"),
    ("owner", "market_data"),
    ("expires", '"2026-12-31"'),
    ("justification", '"Texto: con dos puntos."'),
    ("revocation", '"Criterio ADR-0044 §3.5."'),
)

PORT_ERROR_LINE = "    foo: bar"


def registry_text(
    *,
    remove: set[str] | None = None,
    overrides: dict[str, str] | None = None,
    empty: bool = False,
    extra_items: int = 0,
    extra_port: str = "MercadoVistaPort",
    extra_lines: list[str] | None = None,
) -> str:
    """Genera texto de registry para el subconjunto constreñido usando la gramática soportada."""
    remove = remove or set()
    overrides = overrides or {}
    if empty:
        return "schema_version: 1\nstaged_ports: []\nrules:\n  - id: R1\n"
    lines = ["schema_version: 1", "# comentario", "staged_ports:"]
    first = True
    for key, value in FIELD_LINES:
        if key in remove:
            continue
        value = overrides.get(key, value)
        prefix = "  - " if first else "    "
        lines.append(f"{prefix}{key}: {value}")
        first = False
    for extra in extra_lines or []:
        lines.append(f"    {extra}")
    for _ in range(extra_items):
        lines.append(f"  - port: {extra_port}")
        lines.append("    file: packages/market_data/ports/outbound/otro_port.py")
        lines.append('    expires: "2027-01-31"')
    lines.append("rules:")
    lines.append("  - id: R1")
    return "\n".join(lines) + "\n"


def assert_not_ok(result: ParseResult | RegistryReport) -> None:
    assert not result.ok
    assert result.errors


# ── Registro ─────────────────────────────────────────────────────────────────


def test_registry_vacio_valido() -> None:
    parse = parse_staged_registry(registry_text(empty=True))
    assert parse.ok
    assert parse.entries == ()
    assert validate_registry(parse, today=TODAY).ok


def test_registry_valido_con_una_entrada() -> None:
    parse = parse_staged_registry(registry_text())
    assert parse.ok
    assert len(parse.entries) == 1
    report = validate_registry(parse, today=TODAY)
    assert report.ok, report.errors


def test_registry_real_de_ocm_se_parsea() -> None:
    if not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file():
        pytest.skip("requiere repo OCM")
    parse = load_staged_registry(REPO_ROOT / "policies" / "registry.yaml")
    assert parse.ok, parse.errors
    names = {e.get("port") for e in parse.entries}
    assert names == {"MarketDataViewPort"}
    assert validate_registry(parse, today=TODAY, root=REPO_ROOT).ok
    assert validate_entry(parse.entries[0], today=TODAY, root=REPO_ROOT) == []


@pytest.mark.parametrize(
    "malformed",
    [
        "nada de staged aqui\n",
        "staged_ports:\n  - port",
        "staged_ports:\n  - ",
        "staged_ports:\n    port",
        "staged_ports:\n  - port: A\n  - \n",
        "staged_ports:\n  - port: A\n    foo\n",
        "staged_ports: {\n  port: A\n}\n",
        "staged_ports:\n- port: A\n    foo: bar\n",
        "staged_ports:\n  - port: A\n    mechanism: x\n",
        'staged_ports:\n  - port: A\n    adr_section: "§3.6\n',
    ],
)
def test_yaml_invalido_falla(malformed: str) -> None:
    parse = parse_staged_registry(malformed)
    assert_not_ok(parse)
    assert not validate_registry(parse, today=TODAY).ok


def test_yaml_invalido_no_devuelve_vacio_ni_pass() -> None:
    for malformed in ("staged_ports:\n  - port\n", "staged_ports: {\n", "staged_ports:\n  - "):
        parse = parse_staged_registry(malformed)
        assert parse.entries == ()
        assert_not_ok(parse)


def test_staged_ports_tipo_incorrecto_falla() -> None:
    for header in ("staged_ports: {}", "staged_ports: 42", "staged_ports: nada", "staged_ports: :"):
        assert_not_ok(parse_staged_registry(header + "\n"))


def test_staged_ports_clave_duplicada_falla() -> None:
    text = registry_text() + "staged_ports: []\n"
    assert_not_ok(parse_staged_registry(text))


def test_update_sin_staged_ports_falla() -> None:
    assert_not_ok(parse_staged_registry("# solo comentario\nrules: []\n"))


def test_load_registry_archivo_inexistente_falla() -> None:
    assert_not_ok(load_staged_registry(REPO_ROOT / "policies" / "no_existe.yaml"))
    assert_not_ok(load_staged_registry(None))


# ── Campos ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("field_name", REQUIRED_FIELDS)
def test_campo_obligatorio_ausente_falla(field_name: str) -> None:
    parse = parse_staged_registry(registry_text(remove={field_name}))
    assert parse.ok
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any(field_name in e for e in report.errors)


@pytest.mark.parametrize("field_name", REQUIRED_FIELDS)
def test_campo_obligatorio_vacio_falla(field_name: str) -> None:
    parse = parse_staged_registry(registry_text(overrides={field_name: '""'}))
    assert parse.ok
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any(field_name in e for e in report.errors)


def test_campo_desconocido_no_permitido_falla() -> None:
    # Gramática cerrada: campo extra se rechaza (anti-bypass), no se ignora.
    parse = parse_staged_registry(registry_text(extra_lines=[PORT_ERROR_LINE]))
    assert_not_ok(parse)
    assert any("foo" in e for e in parse.errors)


# ── Expiración ───────────────────────────────────────────────────────────────


def test_expires_futuro_valido() -> None:
    parse = parse_staged_registry(registry_text(overrides={"expires": '"2026-12-31"'}))
    assert validate_registry(parse, today=date(2026, 9, 24)).ok


def test_expires_hoy_falla() -> None:
    parse = parse_staged_registry(registry_text(overrides={"expires": '"2026-09-24"'}))
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any("2026-09-24" in e and "futura" in e for e in report.errors)


def test_expires_pasado_falla() -> None:
    parse = parse_staged_registry(registry_text(overrides={"expires": '"2026-06-01"'}))
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any("expirada" in e for e in report.errors)


@pytest.mark.parametrize("bad_expires", ["2026-13-40", "2026/12/31", "nunca", "TBD", "indefinite", "null", "20261231"])
def test_expires_invalido_falla(bad_expires: str) -> None:
    parse = parse_staged_registry(registry_text(overrides={"expires": f'"{bad_expires}"'}))
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any("ISO-8601" in e for e in report.errors)


def test_expires_ausente_falla() -> None:
    parse = parse_staged_registry(registry_text(remove={"expires"}))
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any("expires" in e for e in report.errors)


# ── Duplicados ───────────────────────────────────────────────────────────────


def test_port_duplicado_falla() -> None:
    parse = parse_staged_registry(registry_text(extra_items=1, extra_port="MercadoVistaPort"))
    assert parse.ok
    report = validate_registry(parse, today=TODAY)
    assert_not_ok(report)
    assert any("Port duplicado" in e for e in report.errors)


# ── Integridad ───────────────────────────────────────────────────────────────


def test_archivo_inexistente_falla() -> None:
    overrides = {"file": "packages/market_data/ports/outbound/no_existe.py"}
    parse = parse_staged_registry(registry_text(overrides=overrides))
    assert validate_registry(parse, today=TODAY, root=REPO_ROOT).ok is False
    # Sin root (validación de forma pura) no comprueba disco.
    assert validate_registry(parse, today=TODAY).ok


def test_identidad_port_a_protocol_se_delega_a_m26() -> None:
    """El core B1 NO resuelve identidad AST; solo valida forma y archivo.

    La resolución `port -> clase Protocol` requiere el recorrido AST de ARCH-006 y
    queda explícitamente delegada a M26/B3 (ver docstring del módulo). Este test
    fija esa frontera para impedir que se cuelen excepciones por nombre de port.
    """
    parse = parse_staged_registry(registry_text(overrides={"port": "PortQueNuncaExistira"}))
    assert parse.ok
    assert validate_registry(parse, today=TODAY, root=REPO_ROOT).ok is False
    assert validate_registry(parse, today=TODAY).ok


# ── No-bypass ────────────────────────────────────────────────────────────────


def test_modulo_no_hardcodea_ports() -> None:
    source = (Path(__file__).resolve().parents[2] / "architecture_linter" / "staged_ports.py").read_text(
        encoding="utf-8"
    )
    for forbidden in ("MarketDataViewPort", "feature_reader", "market_data_view"):
        assert forbidden not in source, f"núcleo con excepción hardcodeada: {forbidden}"


def test_get_staged_entry_no_fabrica_resultados() -> None:
    parse = parse_staged_registry(registry_text())
    assert get_staged_entry(parse, port="MercadoVistaPort") is not None
    assert get_staged_entry(parse, port="FeatureReader") is None
    assert get_staged_entry(parse, port="NoExiste") is None
    assert get_staged_entry(None, port="MercadoVistaPort") is None


def test_entry_para_port_arbitrario_es_valida() -> None:
    """Cualquier port (no solo los del repo) sigue el mismo camino: sin excepciones."""
    text = registry_text(overrides={"port": "CualquierPortFuturo", "expires": '"2027-05-01"'})
    parse = parse_staged_registry(text)
    assert validate_registry(parse, today=TODAY).ok
