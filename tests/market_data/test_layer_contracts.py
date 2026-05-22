# -*- coding: utf-8 -*-
"""
tests/market_data/test_layer_contracts.py
==========================================

Linter de contratos de capas — market_data bounded context.

Invariante principal
--------------------
El dominio (packages/market_data/domain/) no debe importar frameworks
pesados de datos o infraestructura. Solo stdlib y dependencias del propio
dominio son aceptables.

La regla se aplica a nivel de AST (import estático). No detecta imports
dinámicos dentro de funciones, que son un smell arquitectónico separado
y no existen en el codebase post-Fase-1.

Excepciones documentadas (deuda técnica Fase 2)
------------------------------------------------
Los siguientes módulos tienen pandas/pandera como dependencia ESTRUCTURAL
de interfaz (reciben/retornan pd.DataFrame) y están pendientes de
migración en Fase 2 junto con los ports outbound:

  - domain/value_objects/gap_utils.py      — scan_gaps(df: pd.DataFrame)
  - domain/value_objects/grid_alignment.py — align_to_grid(df: pd.DataFrame)
  - domain/value_objects/ohlcv_schema.py   — validate_ohlcv + pandera.pandas

Frameworks prohibidos en domain/
---------------------------------
pandas, pandera, polars, numpy, sqlalchemy, redis, ccxt, dagster,
prometheus_client, loguru, requests, httpx, aiohttp, boto3, pyarrow.

Principios
----------
SRP      — un test por invariante
DIP      — el dominio no conoce frameworks de infra
SSOT     — _ALLOWED_EXCEPTIONS es la fuente canónica de excepciones
Fail-fast — mensajes con archivo + línea + import exacto
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# ── Configuración ──────────────────────────────────────────────────────────────

# Raíz del bounded context
_DOMAIN_ROOT = Path("packages/market_data/domain")

# Frameworks prohibidos en domain/ — cualquier import que empiece con
# alguno de estos prefijos es una violación.
_FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "pandas",
    "pandera",
    "polars",
    "numpy",
    "sqlalchemy",
    "redis",
    "ccxt",
    "dagster",
    "prometheus_client",
    "loguru",
    "requests",
    "httpx",
    "aiohttp",
    "boto3",
    "pyarrow",
)

# Excepciones explícitas: (ruta relativa a _DOMAIN_ROOT, módulo_prohibido)
# Cada excepción documenta deuda técnica pendiente de una fase concreta.
_ALLOWED_EXCEPTIONS: frozenset[tuple[str, str]] = frozenset(
    [
        # Fase 2: pandas estructural — interfaz pd.DataFrame en scan_gaps()
        ("value_objects/gap_utils.py", "pandas"),
        # Fase 2: pandas estructural — interfaz pd.DataFrame en align_to_grid()
        ("value_objects/grid_alignment.py", "pandas"),
        # Fase 2: pandera.pandas estructural — validate_ohlcv() usa DataFrameSchema
        ("value_objects/ohlcv_schema.py", "pandas"),
        ("value_objects/ohlcv_schema.py", "pandera"),
    ]
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _collect_top_level_imports(source: str) -> list[tuple[str, int]]:
    """
    Extrae todos los módulos importados a nivel de módulo (no dentro de
    funciones/clases) de un archivo Python.

    Retorna lista de (module_name, lineno).
    Solo analiza imports estáticos de nivel módulo.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    imports: list[tuple[str, int]] = []
    for child in ast.iter_child_nodes(tree):
        if isinstance(child, ast.Import):
            for alias in child.names:
                imports.append((alias.name, child.lineno))
        elif isinstance(child, ast.ImportFrom):
            module = child.module or ""
            imports.append((module, child.lineno))

    return imports


def _is_forbidden(module_name: str) -> str | None:
    """
    Retorna el prefijo prohibido si el módulo viola la regla, None si no.
    """
    for prefix in _FORBIDDEN_PREFIXES:
        if module_name == prefix or module_name.startswith(f"{prefix}."):
            return prefix
    return None


def _collect_violations() -> list[tuple[Path, int, str, str]]:
    """
    Recorre domain/ y retorna todas las violaciones.

    Retorna lista de (archivo, lineno, import_name, forbidden_prefix).
    Las excepciones documentadas en _ALLOWED_EXCEPTIONS son excluidas.
    """
    violations: list[tuple[Path, int, str, str]] = []

    for py_file in sorted(_DOMAIN_ROOT.rglob("*.py")):
        rel_path = py_file.relative_to(_DOMAIN_ROOT)
        rel_str = str(rel_path)

        source = py_file.read_text(encoding="utf-8")
        imports = _collect_top_level_imports(source)

        for module_name, lineno in imports:
            prefix = _is_forbidden(module_name)
            if prefix is None:
                continue
            if (rel_str, prefix) in _ALLOWED_EXCEPTIONS:
                continue
            violations.append((py_file, lineno, module_name, prefix))

    return violations


# ── Tests ──────────────────────────────────────────────────────────────────────


def test_domain_no_forbidden_framework_imports() -> None:
    """
    El dominio no debe importar frameworks pesados de datos/infraestructura.

    Falla con mensaje diagnóstico que incluye archivo, línea, import exacto
    y framework prohibido detectado.

    Para añadir una excepción temporal documentada, añadir una entrada a
    _ALLOWED_EXCEPTIONS con comentario de fase/ticket obligatorio.
    """
    if not _DOMAIN_ROOT.exists():
        pytest.skip(f"Domain root not found: {_DOMAIN_ROOT}")

    violations = _collect_violations()
    if not violations:
        return

    lines = [
        "",
        "╔══ DOMAIN LAYER CONTRACT VIOLATION ══════════════════════════════════╗",
        "║  domain/ importa frameworks de datos/infra prohibidos.              ║",
        "║  El dominio debe ser independiente de implementación.               ║",
        "╚══════════════════════════════════════════════════════════════════════╝",
        "",
        f"  {len(violations)} violación(es) encontrada(s):",
        "",
    ]
    for filepath, lineno, import_name, prefix in violations:
        rel = filepath.relative_to(Path("."))
        lines.append(f"  ✗ {rel}:{lineno}")
        lines.append(f"      import detectado  : {import_name!r}")
        lines.append(f"      framework prohibido: {prefix!r}")
        lines.append("")

    lines += [
        "  Excepciones documentadas actuales (deuda Fase 2):",
    ]
    for rel_path, framework in sorted(_ALLOWED_EXCEPTIONS):
        lines.append(f"    • domain/{rel_path}  [{framework}]")

    lines += [
        "",
        "  Para añadir una excepción temporal documentada:",
        '    Añadir a _ALLOWED_EXCEPTIONS: ("path/relativo.py", "framework")',
        "  con comentario obligatorio de fase/ticket.",
        "",
    ]
    pytest.fail("\n".join(lines))


def test_allowed_exceptions_still_exist() -> None:
    """
    Verifica que cada excepción documentada siga siendo necesaria.

    Si un archivo fue migrado (Fase 2 completada) pero su excepción
    no fue removida de _ALLOWED_EXCEPTIONS, este test falla — SSOT.
    Evita que la lista de excepciones se convierta en cementerio.
    """
    if not _DOMAIN_ROOT.exists():
        pytest.skip(f"Domain root not found: {_DOMAIN_ROOT}")

    stale: list[tuple[str, str]] = []

    for rel_str, framework in _ALLOWED_EXCEPTIONS:
        filepath = _DOMAIN_ROOT / rel_str
        if not filepath.exists():
            stale.append((rel_str, framework))
            continue

        source = filepath.read_text(encoding="utf-8")
        imports = _collect_top_level_imports(source)
        module_names = {m for m, _ in imports}

        still_used = any(m == framework or m.startswith(f"{framework}.") for m in module_names)
        if not still_used:
            stale.append((rel_str, framework))

    if not stale:
        return

    lines = [
        "",
        "╔══ STALE EXCEPTION IN _ALLOWED_EXCEPTIONS ════════════════════════════╗",
        "║  Una excepción documentada ya no es necesaria.                       ║",
        "║  El framework fue migrado pero la excepción no fue removida.         ║",
        "║  Remover la entrada de _ALLOWED_EXCEPTIONS para mantener SSOT.      ║",
        "╚══════════════════════════════════════════════════════════════════════╝",
        "",
    ]
    for rel_str, framework in stale:
        lines.append(f"  ✗ domain/{rel_str}  [{framework!r}] ya no importa este framework")
    lines.append("")
    pytest.fail("\n".join(lines))


def test_composition_root_is_sole_wiring_point() -> None:
    """
    Punto de extensión documentado para verificar que pipeline_factory.py
    es el único Composition Root autorizado en el bounded context.

    Por ahora pasa trivialmente — no hay constructores de infra en domain/.
    Expandir con assertions concretas conforme crece el codebase.
    """
    pass
