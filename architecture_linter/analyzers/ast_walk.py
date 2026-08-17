"""architecture_linter.analyzers.ast_walk — análisis AST de módulos Python OCM.

El mecanismo principal del linter es AST (no grep de texto). Este módulo
extrae de cada archivo .py un inventario estructural reutilizable:
clases, atributos de instancia, mutaciones, métodos y referencias.

No importa ningún módulo interno de OCM (herramienta stdlib-only).
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class InstanceAttr:
    """Atributo de instancia `self._name` con sus sitios de mutación/lectura."""

    name: str
    line: int
    annotation: Optional[str] = None
    mutations: list[int] = field(default_factory=list)  # líneas de escritura/mutación
    reads: list[int] = field(default_factory=list)  # líneas de lectura


@dataclass
class RaiseSite:
    """Un `raise <Exc>` dentro de un método de la clase."""

    exc_name: str
    line: int
    method: str


@dataclass
class ClassInfo:
    """Información estructural de una clase en un módulo."""

    name: str
    line: int
    bases: list[str] = field(default_factory=list)
    decorators: list[str] = field(default_factory=list)
    is_enum: bool = False
    is_protocol: bool = False
    is_abstract: bool = False
    methods: list[tuple[str, int]] = field(default_factory=list)  # (name, line)
    method_nodes: dict[str, ast.FunctionDef | ast.AsyncFunctionDef] = field(default_factory=dict)
    attrs: dict[str, InstanceAttr] = field(default_factory=dict)
    raise_sites: list[RaiseSite] = field(default_factory=list)
    has_not_implemented: bool = False
    has_stop_async_iteration: bool = False
    has_todo_comment: bool = False
    docstring_markers: list[str] = field(default_factory=list)

    @property
    def capability_methods(self) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
        """Métodos de capacidad prometida: públicos y no dunder (interfaz, no protocolo/dunder)."""
        return [m for m in self.method_nodes.values() if not m.name.startswith("_") and not m.name.startswith("__")]

    @property
    def is_null_object_name(self) -> bool:
        """Null-object intencional por convención de nombre (Null*/Noop*): no es un stub."""
        lowered = self.name.lower()
        return lowered.startswith(("null", "noop")) or "noop" in lowered


@dataclass
class GlobalStore:
    """Contenedor mutable a nivel de módulo (estado global)."""

    name: str
    line: int
    annotation: Optional[str] = None
    mutations: list[int] = field(default_factory=list)  # líneas de escritura/mutación


@dataclass
class ModuleInfo:
    """Inventario estructural de un módulo Python."""

    path: Path
    classes: list[ClassInfo] = field(default_factory=list)
    imports: list[tuple[str, int]] = field(default_factory=list)  # (module, line)
    global_stores: list[GlobalStore] = field(default_factory=list)
    module_docstring_markers: list[str] = field(default_factory=list)


# Mutadores por método Call sobre self.<attr>
_MUTATING_METHODS = {
    "pop",
    "update",
    "setdefault",
    "clear",
    "add",
    "discard",
    "append",
    "extend",
    "remove",
    "insert",
    "__setitem__",
    "__delitem__",
    "popitem",
}


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in {"'", '"'}:
        return s[1:-1]
    return s


def _annotation_str(node: Optional[ast.AST]) -> Optional[str]:
    """Serializa una anotación a texto plano (incl. strings de `from __future__`)."""
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    try:
        return ast.unparse(node)
    except Exception:
        return None


def _markers_from_docstring(docstring: Optional[str]) -> list[str]:
    if not docstring:
        return []
    markers: list[str] = []
    # Solo marcadores fuertes de stub explícito (no TODO/stub genéricos, que
    # producen falsos positivos con comentarios de typing-stubs de librerías).
    for marker in ("NOT_IMPLEMENTED", "NOT IMPLEMENTED", "no implementad"):
        if marker.lower() in docstring.lower():
            markers.append(marker)
    return markers


def analyze_module(path: Path) -> ModuleInfo:
    """Analiza un módulo Python por AST y devuelve su inventario estructural."""
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return ModuleInfo(path=path)

    info = ModuleInfo(path=path)
    info.module_docstring_markers = _markers_from_docstring(ast.get_docstring(tree, clean=False))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                info.imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            info.imports.append((mod, node.lineno))

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            info.classes.append(_analyze_class(node))
        elif isinstance(node, ast.AnnAssign):
            _scan_module_level_annassign(node, info)
        elif isinstance(node, ast.Assign):
            _scan_module_level_assign(node, info)
        elif isinstance(node, (ast.Subscript, ast.Call)):
            _scan_module_level_mutation(node, info)
        elif isinstance(node, ast.Expr) and isinstance(node.value, (ast.Subscript, ast.Call)):
            _scan_module_level_mutation(node.value, info)
    return info


def _is_container_annotation(annotation: Optional[str]) -> bool:
    if not annotation:
        return False
    return any(t in annotation for t in ("dict", "list", "set", "Dict", "List", "Set"))


def _scan_module_level_annassign(node: ast.AnnAssign, info: ModuleInfo) -> None:
    if not isinstance(node.target, ast.Name):
        return
    annotation = _annotation_str(node.annotation)
    if not _is_container_annotation(annotation):
        return
    stores = {s.name: s for s in info.global_stores}
    if node.target.id in stores:
        return
    info.global_stores.append(GlobalStore(name=node.target.id, line=node.lineno, annotation=annotation))


def _scan_module_level_assign(node: ast.Assign, info: ModuleInfo) -> None:
    stores = {s.name: s for s in info.global_stores}
    for target in node.targets:
        if isinstance(target, ast.Name):
            if isinstance(node.value, (ast.Dict, ast.List, ast.Set)):
                if target.id not in stores:
                    info.global_stores.append(GlobalStore(name=target.id, line=node.lineno))
            elif target.id in stores:
                stores[target.id].mutations.append(node.lineno)  # re-inicialización
        elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
            if target.value.id in stores:
                stores[target.value.id].mutations.append(node.lineno)


def _scan_module_level_mutation(node: ast.AST, info: ModuleInfo) -> None:
    """Mutación a nivel de módulo: `X[key] += ...` o `X.pop()/append()/...`."""
    stores = {s.name: s for s in info.global_stores}
    if not stores:
        return
    if (
        isinstance(node, ast.AugAssign)
        and isinstance(node.target, ast.Subscript)
        and isinstance(node.target.value, ast.Name)
    ):
        if node.target.value.id in stores:
            stores[node.target.value.id].mutations.append(node.lineno)
    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
        if node.func.value.id in stores and node.func.attr in _MUTATING_METHODS:
            stores[node.func.value.id].mutations.append(node.lineno)


def _analyze_class(node: ast.ClassDef) -> ClassInfo:
    ci = ClassInfo(name=node.name, line=node.lineno)
    for base in node.bases:
        ci.bases.append(ast.unparse(base) if base else "")
    for dec in node.decorator_list:
        name = dec.id if isinstance(dec, ast.Name) else (ast.unparse(dec) if dec else "")
        ci.decorators.append(name)

    base_names = {b.split(".")[-1] for b in ci.bases}
    ci.is_enum = bool(base_names & {"Enum", "IntEnum", "StrEnum", "Flag", "IntFlag"})
    ci.is_protocol = "Protocol" in base_names
    ci.is_abstract = bool(base_names & {"ABC", "AbstractBaseClass"}) or any(
        d in {"abstractmethod", "abstractclassmethod", "abstractstaticmethod"} for d in ci.decorators
    )

    docstring = ast.get_docstring(node)
    ci.docstring_markers = _markers_from_docstring(docstring)

    for item in node.body:
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
            ci.methods.append((item.name, item.lineno))
            ci.method_nodes[item.name] = item
            _scan_method_body(item, ci)
        elif isinstance(item, ast.Assign):
            _scan_class_level_assign(item, ci)
        elif isinstance(item, ast.AnnAssign):
            _scan_class_level_annassign(item, ci)
    return ci


def _scan_class_level_assign(node: ast.Assign, ci: ClassInfo) -> None:
    for target in node.targets:
        if isinstance(target, ast.Name):
            ci.attrs.setdefault(target.id, InstanceAttr(name=target.id, line=node.lineno))


def _scan_class_level_annassign(node: ast.AnnAssign, ci: ClassInfo) -> None:
    if isinstance(node.target, ast.Name):
        annotation = _annotation_str(node.annotation)
        ci.attrs.setdefault(node.target.id, InstanceAttr(name=node.target.id, line=node.lineno, annotation=annotation))


def _scan_method_body(func: ast.FunctionDef | ast.AsyncFunctionDef, ci: ClassInfo) -> None:
    """Detecta mutaciones/lecturas de `self._attr`, raise sites y marcadores en un método."""
    for node in ast.walk(func):
        if isinstance(node, ast.Raise):
            _track_raise(node, ci, func.name)
        elif isinstance(node, ast.Assign):
            for target in ast.walk(node):
                _track_assignment_target(target, ci)
        elif isinstance(node, ast.AnnAssign):
            _track_assignment_target(node.target, ci)
            if isinstance(node.target, ast.Attribute):
                attr = _self_attr(node.target)
                annotation = _annotation_str(node.annotation)
                if attr and attr[0].startswith("_") and annotation:
                    inst = ci.attrs.get(attr[0]) or ci.attrs.setdefault(
                        attr[0], InstanceAttr(name=attr[0], line=attr[1])
                    )
                    if not inst.annotation:
                        inst.annotation = annotation
        elif isinstance(node, ast.Delete):
            for target in node.targets:
                _track_delete_target(target, ci)
        elif isinstance(node, ast.Call):
            _track_call(node, ci)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            _track_call(node.value, ci)


def _self_attr(node: ast.AST) -> Optional[tuple[str, int]]:
    """Si `node` es `self.<attr>` devuelve (attr, lineno)."""
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self":
        return (node.attr, node.lineno)
    return None


def _track_assignment_target(target: ast.AST, ci: ClassInfo) -> None:
    # self._x = ...   (escritura directa, incluido dentro de Subscript)
    if isinstance(target, ast.Attribute):
        attr = _self_attr(target)
        if attr and attr[0].startswith("_"):
            inst = ci.attrs.setdefault(attr[0], InstanceAttr(name=attr[0], line=attr[1]))
            inst.mutations.append(attr[1])
    elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Attribute):
        attr = _self_attr(target.value)
        if attr and attr[0].startswith("_"):
            inst = ci.attrs.setdefault(attr[0], InstanceAttr(name=attr[0], line=attr[1]))
            inst.mutations.append(attr[1])


def _track_delete_target(target: ast.AST, ci: ClassInfo) -> None:
    if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Attribute):
        attr = _self_attr(target.value)
        if attr and attr[0].startswith("_"):
            inst = ci.attrs.setdefault(attr[0], InstanceAttr(name=attr[0], line=attr[1]))
            inst.mutations.append(attr[1])


def _track_call(node: ast.Call, ci: ClassInfo) -> None:
    """self._x.pop(...) / self._x.update(...) → mutación; self._x.get(...) → lectura."""
    if not isinstance(node.func, ast.Attribute):
        return
    attr = _self_attr(node.func.value)
    if not attr or not attr[0].startswith("_"):
        return
    method = node.func.attr
    inst = ci.attrs.setdefault(attr[0], InstanceAttr(name=attr[0], line=attr[1]))
    if method in _MUTATING_METHODS:
        inst.mutations.append(node.lineno)
    elif method in {"get", "keys", "values", "items", "__contains__", "is_running"}:
        inst.reads.append(node.lineno)


def _track_raise(node: ast.Raise, ci: ClassInfo, method: str = "") -> None:
    if node.exc is None:
        return
    exc_name = ""
    if isinstance(node.exc, ast.Name):
        exc_name = node.exc.id
    elif isinstance(node.exc, ast.Call) and isinstance(node.exc.func, ast.Name):
        exc_name = node.exc.func.id
    if exc_name in {"NotImplementedError", "StopAsyncIteration"}:
        ci.raise_sites.append(RaiseSite(exc_name=exc_name, line=node.lineno, method=method))
    if exc_name == "NotImplementedError":
        ci.has_not_implemented = True
    elif exc_name == "StopAsyncIteration":
        ci.has_stop_async_iteration = True


ANNOTATION_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]*")


def extract_annotation_names(node: Optional[ast.AST]) -> list[str]:
    """Extrae nombres de símbolo de una anotación (incluso strings de future annotations)."""
    ann = _annotation_str(node)
    if not ann:
        return []
    return ANNOTATION_RE.findall(ann)


def iter_py_files(root: Path, exclude_dirs: frozenset[str] = frozenset({".venv", "__pycache__", ".git"})) -> list[Path]:
    """Itera archivos .py bajo root excluyendo directorios no productivos."""
    files: list[Path] = []
    for p in root.rglob("*.py"):
        if any(part in exclude_dirs for part in p.parts):
            continue
        files.append(p)
    return files
