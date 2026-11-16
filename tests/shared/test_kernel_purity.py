"""Pureza del Shared Kernel: `shared/` solo importa stdlib y `shared.*`."""

import ast
import sys
from pathlib import Path

SHARED_ROOT = Path(__file__).resolve().parents[2] / "shared"
STDLIB = set(sys.stdlib_module_names) | {"__future__"}


def _module_root(module: str) -> str:
    return module.split(".", 1)[0]


def _iter_shared_py_files():
    for path in SHARED_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def _imported_modules(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name
        elif isinstance(node, ast.ImportFrom):
            # Import relativo (`from .x import y`) => intra-shared, permitido.
            if node.level > 0:
                continue
            if node.module:
                yield node.lineno, node.module


def test_shared_kernel_imports_stdlib_only():
    violations = []
    for path in _iter_shared_py_files():
        for lineno, module in _imported_modules(path):
            root = _module_root(module)
            if root == "shared" or root in STDLIB:
                continue
            rel = path.relative_to(SHARED_ROOT.parent)
            violations.append(f"{rel}:{lineno}: imports non-stdlib module '{module}'")
    assert not violations, "shared/ purity violations:\n" + "\n".join(violations)
