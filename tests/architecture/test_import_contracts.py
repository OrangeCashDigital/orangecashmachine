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
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
MARKET_DATA = ROOT / "packages" / "market_data"
APPLICATION = MARKET_DATA / "application"
DOMAIN = MARKET_DATA / "domain"
PORTS = MARKET_DATA / "ports"
ADAPTERS = MARKET_DATA / "adapters"
INFRA = MARKET_DATA / "infrastructure"

PORTFOLIO = ROOT / "packages" / "portfolio"
PORTFOLIO_SERVICES = PORTFOLIO / "services"
PORTFOLIO_INFRA = PORTFOLIO / "infra"
PORTFOLIO_BOOTSTRAP = PORTFOLIO / "bootstrap"

TRADING = ROOT / "packages" / "trading"
TRADING_BOOTSTRAP = TRADING / "bootstrap"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _python_files(path: Path) -> list[Path]:
    return [f for f in path.rglob("*.py") if "test_" not in f.name]


def _dynamic_market_data_targets(filepath: Path) -> list[str]:
    """
    Detecta imports DINÁMICOS con argumento literal hacia market_data.

    Cubre exactamente (F-6):
      · importlib.import_module("market_data.adapters...")
      · importlib.import_module("market_data.infrastructure...")
      · __import__("market_data.adapters...")
      · __import__("market_data.infrastructure...")

    Solo cuando el primer argumento es un LITERAL string (ast.Constant).
    Nombres construidos con variables/f-strings son indetectables de forma
    estática fiable — fuera del alcance del detector (evita falsos positivos
    y análisis estático imposible, ver F-6).

    Devuelve la lista de targets dinámicos market_data detectados.
    """
    try:
        tree = ast.parse(filepath.read_text())
    except SyntaxError:
        return []
    targets: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        is_dynamic_import = (
            (isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name))
            and fn.value.id == "importlib"
            and fn.attr == "import_module"
        ) or (isinstance(fn, ast.Name) and fn.id == "__import__")
        if not is_dynamic_import:
            continue
        if not node.args:
            continue
        arg = node.args[0]
        if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
            continue  # no literal — fuera de alcance (F-6)
        target = arg.value
        if target == "market_data" or target.startswith("market_data."):
            targets.append(target)
    return targets


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
        assert not violations, "Dominio importa capas externas (BC-03):\n" + "\n".join(f"  {v}" for v in violations)


class TestApplicationIsolation:
    """BC-05: application/ no importa infrastructure/ ni adapters/."""

    def test_no_direct_infrastructure_imports(self):
        violations: list[str] = []
        for f in _python_files(APPLICATION):
            for imp in _imports_in(f):
                if imp.startswith("market_data.infrastructure") or imp.startswith("market_data.adapters"):
                    violations.append(f"{f.relative_to(ROOT)}: {imp}")
        assert not violations, "application/ importa infra/adapters directamente (BC-05):\n" + "\n".join(
            f"  {v}" for v in violations
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
        assert port_file.exists(), f"publisher_port.py no encontrado en {PORTS}/outbound/"
        src = port_file.read_text()
        assert "Protocol" in src, (
            "publisher_port.py debe definir un Protocol — los puertos son contratos, no implementaciones"
        )

    def test_ports_do_not_import_infrastructure(self):
        violations: list[str] = []
        for f in _python_files(PORTS):
            for imp in _imports_in(f):
                if "infrastructure" in imp or "adapters" in imp:
                    violations.append(f"{f.relative_to(ROOT)}: {imp}")
        assert not violations, "ports/ importa infrastructure o adapters (BC-04):\n" + "\n".join(
            f"  {v}" for v in violations
        )

    def test_null_publisher_is_co_located_with_protocol(self):
        port_file = PORTS / "outbound" / "publisher_port.py"
        if not port_file.exists():
            pytest.skip("publisher_port.py no existe aún")
        src = port_file.read_text()
        assert "NullPublisher" in src, (
            "NullPublisher (NullObject del port) debe estar co-ubicado con OHLCVPublisherPort en publisher_port.py"
        )


class TestCompositionRoot:
    """BC-38: CompositionRoot es el único punto de ensamblaje."""

    def test_composition_root_exists(self):
        cr = INFRA / "bootstrap" / "composition_root.py"
        assert cr.exists(), "composition_root.py no encontrado — el Composition Root formal es obligatorio (BC-38)"

    def test_composition_root_is_frozen_dataclass(self):
        cr = INFRA / "bootstrap" / "composition_root.py"
        if not cr.exists():
            pytest.skip("composition_root.py no existe")
        src = cr.read_text()
        assert "frozen=True" in src, "CompositionRoot debe ser un dataclass frozen=True — inmutable tras construcción"

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
                        violations.append(f"{f.relative_to(ROOT)}: imports {imp}")
        assert not violations, (
            "PipelineFactory de infra importada fuera de infrastructure/bootstrap/ (BC-38):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nNota: ports/inbound/pipeline_factory.py es un Port abstracto — legítimo."
        )

    def test__build_catalog_imports_iceberg_get_catalog_not_stale_path(self):
        """
        R2 / B-02: `_build_catalog` DEBE importar `get_catalog` desde el módulo
        iceberg real, no desde el path stale `storage.catalog` (módulo inexistente).

        El bug histórico apuntaba a
        `market_data.infrastructure.storage.catalog` (no existe) y a
        `build_catalog` (no exportada). El módulo real es
        `market_data.infrastructure.storage.iceberg.catalog` y exporta
        `get_catalog`.
        """
        src = (INFRA / "bootstrap" / "pipeline_factory.py").read_text()
        assert "market_data.infrastructure.storage.iceberg.catalog" in src, (
            "`_build_catalog` debe importar el modulo iceberg.catalog real (BC-R2)"
        )
        assert "get_catalog" in src, "`_build_catalog` debe usar get_catalog() (R2)"

        # El modulo objetivo y su simbolo deben existir (guard del import real).
        import importlib

        mod = importlib.import_module("market_data.infrastructure.storage.iceberg.catalog")
        assert callable(getattr(mod, "get_catalog")), "iceberg.catalog.get_catalog inexistente (R2)"

    def test__build_catalog_does_not_import_legacy_path(self):
        """B-02: ningun import a `storage.catalog` (path inexistente) en pipeline_factory."""
        src = (INFRA / "bootstrap" / "pipeline_factory.py").read_text()
        assert "storage.catalog import" not in src, "import legacy storage.catalog presente (B-02)"


class TestPortfolioCompositionRoot:
    """BC-43: CompositionRoot de portfolio es el único punto de ensamblaje."""

    def test_composition_root_exists(self):
        cr = PORTFOLIO_BOOTSTRAP / "composition_root.py"
        assert cr.exists(), "composition_root.py no encontrado — el CompositionRoot formal es obligatorio (BC-43)"

    def test_composition_root_is_frozen_dataclass(self):
        cr = PORTFOLIO_BOOTSTRAP / "composition_root.py"
        if not cr.exists():
            pytest.skip("composition_root.py no existe")
        src = cr.read_text()
        assert "frozen=True" in src, "CompositionRoot debe ser un dataclass frozen=True — inmutable tras construcción"

    def test_position_store_adapters_not_imported_outside_bootstrap(self):
        """
        RedisPositionStore/InMemoryPositionStore solo se importan desde
        portfolio/bootstrap/ o desde portfolio/infra/ (co-ubicación legítima
        del propio adapter). portfolio.services debe recibir PositionStore
        (Protocol) por constructor — nunca instanciar el adapter concreto.
        """
        FORBIDDEN_MODULES = (
            "portfolio.infra.memory_store",
            "portfolio.infra.redis_store",
            "portfolio.infra.redis_factory",
        )
        violations: list[str] = []
        if PORTFOLIO_SERVICES.exists():
            for f in _python_files(PORTFOLIO_SERVICES):
                for imp in _imports_in(f):
                    if imp in FORBIDDEN_MODULES:
                        violations.append(f"{f.relative_to(ROOT)}: imports {imp}")
        assert not violations, "PositionStore adapter importado fuera de portfolio/bootstrap/ (BC-43):\n" + "\n".join(
            f"  {v}" for v in violations
        )


class TestTradingCompositionRoot:
    """BC-50: trading importa market_data solo desde trading/bootstrap/composition_root."""

    def test_market_data_imports_only_in_composition_root(self):
        """
        BC-50: todo import REAL de market_data en trading (incluidos los lazy
        dentro de funciones, que import-linter no ve al analizar solo el grafo
        estático de nivel de módulo) debe estar en trading/bootstrap/
        composition_root.py — el único punto autorizado (ADR-0004/BC-50).
        """
        allowed = TRADING_BOOTSTRAP / "composition_root.py"
        violations: list[str] = []
        for f in _python_files(TRADING):
            for imp in _imports_in(f):
                if imp == "market_data" or imp.startswith("market_data."):
                    if f != allowed:
                        violations.append(f"{f.relative_to(ROOT)}: imports {imp}")
        assert not violations, (
            "trading importa market_data fuera del composition root (BC-50):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nMover el acoplamiento a trading.bootstrap.composition_root "
            "(ADP: GoldReader es el único contacto trading→market_data)."
        )

    def test_trading_runtime_exposes_exactly_three_fields(self):
        """
        SSOT (forense §2 + ADR-0003 enmendado): TradingRuntime es exactamente
        (engine, portfolio, tracker) — sin estado oculto adicional.
        """
        import dataclasses

        from trading.bootstrap.composition_root import TradingRuntime

        fields = [f.name for f in dataclasses.fields(TradingRuntime)]
        assert fields == ["engine", "portfolio", "tracker"], (
            f"TradingRuntime debe ser exactamente (engine, portfolio, tracker) — actual: {fields}"
        )
        assert TradingRuntime.__dataclass_params__.frozen is True
        assert hasattr(TradingRuntime, "__slots__") or TradingRuntime.__dataclass_params__.slots is True

    def test_no_dynamic_market_data_imports_outside_composition_root(self):
        """
        F-6: ningún import DINÁMICO con literal hacia market_data fuera del
        composition root.

        import-linter y el test estático de BC-50 solo ven imports en el grafo
        de nivel de módulo. Un `importlib.import_module("market_data.adapters..."
        )` con literal string en otro archivo de trading bypasearía esa frontera
        sin que el grafo estático lo detecte. Este detector AST lo cierra.

        Alcance (deliberadamente acotado, F-6):
          · importlib.import_module("market_data...")
          · __import__("market_data...")
          · solo cuando el argumento es un LITERAL string
        Nombres dinámicos construidos con variables/f-strings NO se detectan
        (análisis estático imposible sin ejecución) — fuera de alcance.

        Legítimo y NO dispara el detector: trading/strategies/registry.py usa
        importlib.import_module("trading.strategies.ema_crossover") — módulo
        interno de trading, no market_data.
        """
        allowed = TRADING_BOOTSTRAP / "composition_root.py"
        violations: list[str] = []
        for f in _python_files(TRADING):
            if f == allowed:
                continue  # el composition root es el punto autorizado (BC-50)
            for target in _dynamic_market_data_targets(f):
                violations.append(f"{f.relative_to(ROOT)}: importlib/__import__ {target}")
        assert not violations, (
            "trading usa import DINÁMICO de market_data fuera del composition root (F-6):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\n\nMover el acoplamiento a trading.bootstrap.composition_root "
            "(ADP: único punto autorizado a tocar market_data)."
        )


class TestDynamicMarketDataDetector:
    """
    F-6: tests de regresión del detector de imports dinámicos con literal.

    Demuestran los casos del contrato F-6 sin depender del estado del repo:
      2. importlib.import_module("market_data.adapters...") → detectado
      3. __import__("market_data.infrastructure...") → detectado
      5. imports no relacionados (trading.* / stdlib) → NO detectados
      6. nombres dinámicos no literales (variable / f-string) → fuera de alcance

    (El caso 1 — import normal prohibido — lo cubre BC-50 y el caso 4 — patrón
    permitido en el CR — lo cubre la exclusión del escáner, ver
    test_no_dynamic_market_data_imports_outside_composition_root.)
    """

    def _write(self, tmp_path: Path, src: str) -> Path:
        f = tmp_path / "sample.py"
        f.write_text(src)
        return f

    def test_detects_importlib_import_module_market_data_adapters(self, tmp_path):
        f = self._write(
            tmp_path,
            'import importlib\nimportlib.import_module("market_data.adapters.outbound.storage.iceberg_factory")\n',
        )
        assert _dynamic_market_data_targets(f) == ["market_data.adapters.outbound.storage.iceberg_factory"]

    def test_detects_builtin_import_market_data_infrastructure(self, tmp_path):
        f = self._write(tmp_path, '__import__("market_data.infrastructure.redis.redis_stream")\n')
        assert _dynamic_market_data_targets(f) == ["market_data.infrastructure.redis.redis_stream"]

    def test_detects_top_level_market_data_module(self, tmp_path):
        f = self._write(tmp_path, 'importlib.import_module("market_data")\n')
        assert _dynamic_market_data_targets(f) == ["market_data"]

    def test_ignores_non_market_data_import_module(self, tmp_path):
        f = self._write(
            tmp_path,
            'import importlib\nimportlib.import_module("trading.strategies.ema_crossover")\n__import__("os")\n',
        )
        assert _dynamic_market_data_targets(f) == []

    def test_ignores_dynamic_non_literal_names(self, tmp_path):
        f = self._write(
            tmp_path,
            "import importlib\n"
            'mod = "market_data.adapters.outbound.storage.iceberg_factory"\n'
            "importlib.import_module(mod)\n"
            'importlib.import_module(f"market_data.adapters.{suffix}")\n',
        )
        assert _dynamic_market_data_targets(f) == []

    def test_scan_excludes_authorized_composition_root(self, tmp_path):
        """
        Caso 4: el escáner de trading excluye explícitamente el composition root
        autorizado (BC-50/ADP) — mismo patrón, punto permitido.
        """
        allowed = TRADING_BOOTSTRAP / "composition_root.py"
        scanned = [f for f in _python_files(TRADING) if f != allowed]
        assert allowed not in scanned
        assert scanned  # la lista de escaneo no está vacía (no test trivial)


class TestLiveExecutorFailClosed:
    """R1 / B-01: mientras LiveExecutor sea un stub, live trading queda BLOQUEADO."""

    LIVE_EXECUTOR = TRADING / "execution" / "live_executor.py"
    COMPOSITION_ROOT = TRADING_BOOTSTRAP / "composition_root.py"

    def test_executor_declares_stub_flag(self):
        """El executor debe declarar IS_STUB para que el arranque decida fail-closed."""
        src = self.LIVE_EXECUTOR.read_text()
        assert "IS_STUB" in src, "LiveExecutor debe exponer `IS_STUB` (B-01)"

    def test_stub_means_true_flag(self):
        """Fail-closed: si el STUB sigue presente (CCXT sin activar), IS_STUB debe ser True."""
        src = self.LIVE_EXECUTOR.read_text()
        stub_present = "[LIVE-STUB]" in src
        if stub_present:
            assert "IS_STUB: ClassVar[bool] = True" in src, (
                "LiveExecutor es STUB pero IS_STUB no es True — fail-open, viola B-01"
            )

    def test_assemble_live_checks_stub_before_returning(self):
        """El arranque live debe abortar si el executor es un stub."""
        src = self.COMPOSITION_ROOT.read_text()
        assert "IS_STUB" in src, "assemble_live() debe comprobar LiveExecutor.IS_STUB (B-01)"
        assert "raise" in src, "assemble_live() debe abortar (raise) cuando detecta stub"
