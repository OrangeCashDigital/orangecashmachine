"""
tests/architecture/test_import_contracts — Policy tests de contratos de import.

Complementan lint-imports: verifican invariantes estructurales que no se expresan
fácilmente en pyproject.toml (complejidad, pureza de capa, etc.).

No son tests de unidad — son tests de *política arquitectónica*.
Fallar aquí = violación de boundary, no bug de lógica.

Principio: si lint-imports es el linter estático, estos tests son
el linter dinámico (AST-based) que corre en CI.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT         = Path(__file__).resolve().parent.parent.parent
MARKET_DATA  = ROOT / "packages" / "market_data"
APPLICATION  = MARKET_DATA / "application"
DOMAIN       = MARKET_DATA / "domain"
PORTS        = MARKET_DATA / "ports"
ADAPTERS     = MARKET_DATA / "adapters"
INFRA        = MARKET_DATA / "infrastructure"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _python_files(path: Path) -> list[Path]:
    return [f for f in path.rglob("*.py") if "test_" not in f.name]


def _imports_in(filepath: Path) -> list[str]:
    """Extrae todos los módulos importados de un archivo .py."""
    try:
        tree = ast.parse(filepath.read_text())
    except SyntaxError:
        return []
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def _has_lazy_infra_import(filepath: Path) -> bool:
    """
    Detecta el anti-patrón: import concreto de infra dentro de try/except
    con retorno None — lazy fallback que debe vivir en CompositionRoot.
    """
    src = filepath.read_text()
    tree = ast.parse(src)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        # Buscar import de infrastructure dentro del try
        for child in ast.walk(node):
            if isinstance(child, (ast.Import, ast.ImportFrom)):
                mod = ""
                if isinstance(child, ast.ImportFrom) and child.module:
                    mod = child.module
                elif isinstance(child, ast.Import):
                    mod = child.names[0].name if child.names else ""
                if "infrastructure" in mod or "kafka" in mod.lower():
                    # Verificar que el handler retorna None
                    for handler in node.handlers:
                        for stmt in ast.walk(handler):
                            if (
                                isinstance(stmt, ast.Return)
                                and isinstance(stmt.value, ast.Constant)
                                and stmt.value.value is None
                            ):
                                return True
    return False


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestDomainPurity:
    """BC-03: El dominio no importa capas externas."""

    FORBIDDEN = [
        "market_data.infrastructure",
        "market_data.adapters",
        "market_data.application",
        "kafka",
        "redis",
        "ccxt",
        "sqlalchemy",
        "prefect",
    ]

    def test_domain_does_not_import_infrastructure(self):
        violations: list[str] = []
        for f in _python_files(DOMAIN):
            for imp in _imports_in(f):
                for forbidden in self.FORBIDDEN:
                    if forbidden in imp:
                        violations.append(f"{f.relative_to(ROOT)}: imports {imp}")
        assert not violations, (
            "Dominio importa capas externas (BC-03):\n"
            + "\n".join(f"  {v}" for v in violations)
        )


class TestApplicationIsolation:
    """BC-05: application/ no importa infrastructure/ ni adapters/."""

    def test_no_direct_infrastructure_imports(self):
        violations: list[str] = []
        for f in _python_files(APPLICATION):
            for imp in _imports_in(f):
                if (
                    imp.startswith("market_data.infrastructure")
                    or imp.startswith("market_data.adapters")
                ):
                    violations.append(f"{f.relative_to(ROOT)}: {imp}")
        assert not violations, (
            "application/ importa infra/adapters directamente (BC-05):\n"
            + "\n".join(f"  {v}" for v in violations)
        )

    def test_no_lazy_infrastructure_fallbacks(self):
        """
        Anti-patrón: try: from infrastructure import X / except: return None.
        Este patrón debe vivir en CompositionRoot, no en application/.
        """
        violations: list[str] = []
        for f in _python_files(APPLICATION):
            if _has_lazy_infra_import(f):
                violations.append(str(f.relative_to(ROOT)))
        assert not violations, (
            "Lazy fallbacks de infrastructure en application/ (mover a CompositionRoot):\n"
            + "\n".join(f"  {v}" for v in violations)
        )


class TestPortsAbstraction:
    """BC-04: Los ports son abstracciones (Protocol/ABC), no implementaciones."""

    def test_publisher_port_is_protocol(self):
        port_file = PORTS / "outbound" / "publisher_port.py"
        assert port_file.exists(), (
            f"publisher_port.py no encontrado en {PORTS}/outbound/"
        )
        src = port_file.read_text()
        assert "Protocol" in src, (
            "publisher_port.py debe definir un Protocol — "
            "los puertos son contratos, no implementaciones"
        )

    def test_ports_do_not_import_infrastructure(self):
        violations: list[str] = []
        for f in _python_files(PORTS):
            for imp in _imports_in(f):
                if "infrastructure" in imp or "adapters" in imp:
                    violations.append(f"{f.relative_to(ROOT)}: {imp}")
        assert not violations, (
            "ports/ importa infrastructure o adapters (BC-04):\n"
            + "\n".join(f"  {v}" for v in violations)
        )

    def test_null_publisher_is_co_located_with_protocol(self):
        port_file = PORTS / "outbound" / "publisher_port.py"
        if not port_file.exists():
            pytest.skip("publisher_port.py no existe aún")
        src = port_file.read_text()
        assert "NullPublisher" in src, (
            "NullPublisher (NullObject del port) debe estar co-ubicado "
            "con OHLCVPublisherPort en publisher_port.py"
        )


class TestCompositionRoot:
    """BC-38: CompositionRoot es el único punto de ensamblaje."""

    def test_composition_root_exists(self):
        cr = INFRA / "bootstrap" / "composition_root.py"
        assert cr.exists(), (
            "composition_root.py no encontrado — "
            "el Composition Root formal es obligatorio (BC-38)"
        )

    def test_composition_root_is_frozen_dataclass(self):
        cr = INFRA / "bootstrap" / "composition_root.py"
        if not cr.exists():
            pytest.skip("composition_root.py no existe")
        src = cr.read_text()
        assert "frozen=True" in src, (
            "CompositionRoot debe ser un dataclass frozen=True — "
            "inmutable tras construcción"
        )

    def test_pipeline_factory_not_imported_outside_bootstrap(self):
        """
        PipelineFactory (concreta) solo se importa desde infrastructure/bootstrap/.

        NOTA: ports/inbound/pipeline_factory.py es un Port abstracto — nombre
        coincidente legítimo que NO viola BC-38. La verificación usa AST para
        detectar el import del módulo concreto de infraestructura, no coincidencias
        de string (lo que causaría falsos positivos en los propios ports).
        """
        INFRA_MODULE = "market_data.infrastructure.bootstrap.pipeline_factory"
        violations: list[str] = []
        forbidden_layers = [APPLICATION, DOMAIN, PORTS, ADAPTERS]
        for layer_dir in forbidden_layers:
            if not layer_dir.exists():
                continue
            for f in _python_files(layer_dir):
                for imp in _imports_in(f):
                    if INFRA_MODULE in imp:
                        violations.append(
                            f"{f.relative_to(ROOT)}: imports {imp}"
                        )
        assert not violations, (
            "PipelineFactory de infra importada fuera de infrastructure/bootstrap/ (BC-38):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nNota: ports/inbound/pipeline_factory.py es un Port abstracto — legítimo."
        )
