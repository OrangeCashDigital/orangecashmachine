"""scripts/domain_subprocess_guard.py — Guard AST R11: pureza de dominio.

Convierte el hallazgo H-11 / B-20 (subprocess en domain) en una regla
estructural verificable automáticamente (PLAN-Maestro-Ingenieria.md §6):

    R11 → H-11  `subprocess` en domain (pureza de dominio)

El dominio no debe depender de infraestructura: ejecutar subprocess (p.ej.
`git rev-parse` para resolver el git_hash) es una dependencia de ejecución
que rompe la pureza. La resolución de git_hash se inyecta desde el
composition root (ADR-0006, B-20) via CheckerFactory/QualityPipeline.

Escanea el TEXTO FUENTE vía AST sobre todos los paquetes (`packages/*/domain/`),
incluyendo imports lazy a nivel de función y llamadas indirectas.

Backtest obligatorio antes de activar en CI:
    tests/architecture/test_domain_subprocess_guard.py
    (prueba positiva: árbol real sin subprocess en domain → PASS;
     prueba negativa: anti-patrón → violación detectada).

Uso:
    from scripts.domain_subprocess_guard import guard_domain_subprocess
    violations = guard_domain_subprocess(ROOT)
    assert not violations, "\\n".join(violations)
"""

from __future__ import annotations

import ast
from pathlib import Path

DOMAIN_DIRS = ("packages", "domain")
_DOMAIN_BASENAME = "domain"

# Nombres que, referenciados en domain, indican uso de subprocess.
_SUBPROCESS_ATTRS = frozenset({"run", "Popen", "check_output", "call", "check_call"})
_SUBPROCESS_IMPORTS = frozenset({"subprocess"})


def _py_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.py") if not p.name.startswith("test_"))


def _domain_packages(root: Path) -> list[Path]:
    """Retorna las carpetas domain/ de cada paquete en packages/."""
    pkgs = root / "packages"
    if not pkgs.is_dir():
        return []
    return [p / _DOMAIN_BASENAME for p in sorted(pkgs.iterdir()) if (p / _DOMAIN_BASENAME).is_dir()]


def _parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return None


def _imports_subprocess(tree: ast.Module) -> bool:
    """Detecta `import subprocess`, `import subprocess as X`, `from subprocess import ...`."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(
                alias.name in _SUBPROCESS_IMPORTS or alias.name.split(".")[0] in _SUBPROCESS_IMPORTS
                for alias in node.names
            ):
                return True
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "subprocess" or node.module.split(".")[0] == "subprocess":
                return True
    return False


def _calls_subprocess(tree: ast.Module) -> bool:
    """Detecta `subprocess.run(...)`, `subprocess.Popen(...)`, etc.

    También detecta alias: `import subprocess as sp; sp.run(...)`.
    """
    alias_names: set[str] = {"subprocess"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in _SUBPROCESS_IMPORTS and alias.asname:
                    alias_names.add(alias.asname)
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] == "subprocess":
            for alias in node.names:
                if alias.asname:
                    alias_names.add(alias.asname)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = node.func.value
            if isinstance(name, ast.Name) and name.id in alias_names:
                if node.func.attr in _SUBPROCESS_ATTRS:
                    return True
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            # `from subprocess import run; run(...)`
            if node.func.id in _SUBPROCESS_ATTRS:
                # Requiere evidencia de que vino de subprocess (import directo).
                for imp in ast.walk(tree):
                    if isinstance(imp, ast.ImportFrom) and imp.module == "subprocess":
                        if any(a.name == node.func.id for a in imp.names):
                            return True
    return False


def guard_domain_subprocess(root: Path) -> list[str]:
    """Devuelve la lista de violaciones R11 (subprocess en domain). Vacío = limpio."""
    violations: list[str] = []
    for domain_dir in _domain_packages(root):
        for path in _py_files(domain_dir):
            tree = _parse(path)
            if tree is None:
                continue
            if _imports_subprocess(tree) or _calls_subprocess(tree):
                violations.append(
                    f"R11: {path.relative_to(root)} usa subprocess en domain — "
                    "resolver el valor desde el composition root (B-20/ADR-0006), "
                    "no ejecutar subprocess en domain."
                )
    return violations


__all__ = ["guard_domain_subprocess"]
