"""architecture_linter.engine — contexto de repositorio reutilizable.

El engine construye una sola vez el contexto del repositorio (index de
módulos, clases, referencias, imports) para que las reglas no repitan
análisis costosos. Cada regla consume `RepoContext` y devuelve `RuleResult`.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from pathlib import Path
from typing import Optional

from architecture_linter.analyzers.ast_walk import ModuleInfo, analyze_module, iter_py_files
from architecture_linter.models import RuleResult


class RepoContext:
    """Contexto cacheado del repositorio."""

    def __init__(
        self,
        root: Path,
        roots: list[str] | None = None,
        exclude_dirs: frozenset[str] = frozenset({".venv", "__pycache__", ".git"}),
    ) -> None:
        self.root = root
        self.roots = roots or ["packages", "shared", "apps", "ocm"]
        self._modules: dict[Path, ModuleInfo] = {}
        self._text_cache: dict[Path, str] = {}
        self._class_index: dict[str, list[tuple[Path, str, int]]] = defaultdict(list)
        self._ref_index: dict[str, list[tuple[Path, int]]] = defaultdict(list)
        self._dict_returns: list[tuple[Path, str, int, str]] = []
        self.files: list[Path] = []
        self._build()

    def _build(self) -> None:
        for base in self.roots:
            base_path = self.root / base
            if not base_path.is_dir():
                continue
            for p in iter_py_files(base_path):
                self.files.append(p)
                info = analyze_module(p)
                self._modules[p] = info
                for cls in info.classes:
                    self._class_index[cls.name].append((p, cls.name, cls.line))
        self._build_ref_index()

    def _build_ref_index(self) -> None:
        """Indexa referencias por nombre usando AST (Name y Attribute) + anotaciones."""
        for p, info in self._modules.items():
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            self._text_cache[p] = text
            try:
                tree = ast.parse(text)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    self._ref_index[node.id].append((p, node.lineno))
                elif isinstance(node, ast.Attribute):
                    self._ref_index[node.attr].append((p, node.lineno))
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    # definiciones cuentan como presencia del símbolo (p. ej. def fetch_balance)
                    self._ref_index[node.name].append((p, node.lineno))
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.returns is not None:
                        ret = _annotation_plain(node.returns)
                        if ret and "dict[" in ret and any(v in ret for v in ("float", "Decimal")):
                            self._dict_returns.append((p, node.name, node.lineno, ret))
                elif isinstance(node, ast.Assign):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            self._ref_index[t.id].append((p, node.lineno))
                elif isinstance(node, ast.AnnAssign):
                    if isinstance(node.target, ast.Name):
                        self._ref_index[node.target.id].append((p, node.lineno))
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        base = alias.asname or alias.name.split(".")[0]
                        self._ref_index[base].append((p, node.lineno))
                elif isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        self._ref_index[alias.name].append((p, node.lineno))
                    ann_nodes = []
                    if isinstance(node, ast.AnnAssign) and node.annotation is not None:
                        ann_nodes.append(node.annotation)
                    elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if node.returns is not None:
                            ann_nodes.append(node.returns)
                        for arg in node.args.args:
                            if arg.annotation is not None:
                                ann_nodes.append(arg.annotation)
                    for ann in ann_nodes:
                        if isinstance(ann, ast.Constant) and isinstance(ann.value, str):
                            for name in _identifiers_in_string(ann.value):
                                self._ref_index[name].append((p, node.lineno))

    def module(self, path: Path) -> Optional[ModuleInfo]:
        return self._modules.get(path)

    def text(self, path: Path) -> Optional[str]:
        if path not in self._text_cache:
            try:
                self._text_cache[path] = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                return None
        return self._text_cache.get(path)

    def class_defs(self, name: str) -> list[tuple[Path, str, int]]:
        return list(self._class_index.get(name, []))

    def references(self, name: str) -> list[tuple[Path, int]]:
        """Todas las referencias a un nombre (excluyendo la propia definición)."""
        return list(self._ref_index.get(name, []))

    def all_classes(self) -> list[tuple[Path, str, int]]:
        out: list[tuple[Path, str, int]] = []
        for path, classes in self._class_index.items():
            for entry in classes:
                out.append(entry)
        return out

    def all_symbols(self) -> list[str]:
        return list(self._ref_index.keys())

    def dict_returns(self) -> list[tuple[Path, str, int, str]]:
        """Métodos/funciones cuyo return es dict[...] con valor numérico (candidato balance)."""
        return self._dict_returns

    def symbols_in_file(self, path: Path) -> list[tuple[str, int]]:
        """Símbolos referenciados en un archivo, con su línea."""
        out: list[tuple[str, int]] = []
        for name, refs in self._ref_index.items():
            for p, line in refs:
                if p == path:
                    out.append((name, line))
        return out


def _identifiers_in_string(s: str) -> list[str]:
    import re

    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*", s)


def _annotation_plain(node) -> Optional[str]:
    """Serializa una anotación a texto (incl. strings de future annotations)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    try:
        return ast.unparse(node)
    except Exception:
        return None


class LinterEngine:
    """Ejecuta un conjunto de reglas sobre un RepoContext."""

    def __init__(self, ctx: RepoContext) -> None:
        self.ctx = ctx

    def run(self, rules: list) -> list[RuleResult]:
        results: list[RuleResult] = []
        for rule in rules:
            if not rule.enabled:
                continue
            results.append(rule.run(self.ctx))
        return results
