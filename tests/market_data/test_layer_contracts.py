# -*- coding: utf-8 -*-
"""
tests/market_data/test_layer_contracts.py

BC-09 (domain no importa frameworks de infra/datos) está gobernado por
import-linter en architecture_linter/importlinter.toml.

F-022 (twin-import, docs/audits/2026-08-08-streaming-canary-audit.md):
el pythonpath incluye "." — packages/ es importable como packages.* y
market_data.* a la vez → Duplicated timeseries en CollectorRegistry al
importar métricas por ambas rutas (reproducido: ValueError). Guard estático:
ningún .py del repo puede importar módulos vía el prefijo "packages.".
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def _all_imports_in(path: Path) -> list[tuple[Path, str]]:
    """Retorna (archivo, módulo_importado) para todos los .py de un directorio."""
    result: list[tuple[Path, str]] = []
    if not path.exists():
        return result
    for f in path.rglob("*.py"):
        if "test_" in f.name and f.parent.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    result.append((f, alias.name))
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    result.append((f, node.module))
    return result


def test_no_twin_import_via_packages_prefix() -> None:
    """F-022: ningún import puede usar el prefijo doble-ruta 'packages.'."""
    culprits: list[str] = []
    for f in ROOT.rglob("*.py"):
        if ".venv" in f.parts or ".git" in f.parts or "node_modules" in f.parts:
            continue
        for import_path in _scan_imports_from_path(f):
            if import_path == "packages" or import_path.startswith("packages."):
                culprits.append(f"{f.relative_to(ROOT)}: {import_path}")
    assert not culprits, (
        "F-022 twin-import: 'packages.market_data.*' duplica series en el "
        "CollectorRegistry (market_data importable por doble ruta). Usar SIEMPRE "
        "ruta canónica market_data.*. Detectados:\n" + "\n".join(culprits)
    )


def _scan_imports_from_path(path: Path) -> list[str]:
    """Imports de un solo archivo (raíz de módulo)."""
    imports: list[str] = []
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return imports
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports
