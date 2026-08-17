"""architecture_linter.analyzers.mutable_state — detección de estado mutable.

Correlaciona declaración → mutación → lectura de atributos `self._x` que son
contenedores de estado (posición/orden) sin depender solo del nombre:

  * el atributo debe ser un contenedor mutable (dict/list/set) o tener patrón;
  * debe existir mutación real (subscript write, pop, update, del, setdefault);
  * la evidencia de concepto (position/order) puede venir de:
      - patrón de nombre (POSITION_ATTR_PATTERNS/ORDER_ATTR_PATTERNS);
      - hint de anotación (tuple[float,...], Position, Order, ...);
      - forma del valor asignado (tupla de 2-3 elementos => posición).

Incluye análisis de semántica de escritura por método (AST), que sustituye a
los antiguos detectores textuales (`new_qty`/`remaining`/`.pop(`) por patrones
estructurales (aritmética WAC, reemplazo sin lectura del previo, reducción,
pop/del). Compartido por ARCH-001, ARCH-002, ARCH-003, ARCH-010.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from architecture_linter.analyzers.ast_walk import ClassInfo
from architecture_linter.models import Evidence, OrderStore, PositionStore

# Patrones de nombre que sugieren posición (pero la regla exige más evidencia)
POSITION_ATTR_PATTERNS = ("_positions", "_open_positions", "_entry_positions", "_open", "_position")
ORDER_ATTR_PATTERNS = ("_orders", "_open_orders", "_open")

# Tipos de valor que delatan almacenamiento de posición
POSITION_TYPE_HINTS = ("tuple[float", "Position", "Order", "PositionSnapshot", "tuple[float, float]")
ORDER_TYPE_HINTS = ("Order",)

# Etiquetas de semántica de escritura
SEM_WAC = "wac"
SEM_ACCUMULATE = "accumulate"
SEM_REPLACE = "replace"
SEM_REDUCE = "reduce"
SEM_POP = "pop"


@dataclass
class OwnerSemantics:
    """Semántica de escritura de un owner de posición (por método, AST)."""

    owner_class: str
    file: str
    ops: dict[str, list[Evidence]] = field(default_factory=dict)

    @property
    def tags(self) -> set[str]:
        return set(self.ops.keys())


def _is_container_typed(annotation: Optional[str]) -> bool:
    if not annotation:
        return False
    return any(t in annotation for t in ("dict", "list", "set", "Dict", "List", "Set"))


def _is_self_attr(node: ast.AST, attr: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        and node.attr == attr
    )


def _write_nodes(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> list[ast.AST]:
    """Nodos de escritura directa sobre `self.<attr>` dentro de un método."""
    out: list[ast.AST] = []
    for node in ast.walk(method):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                target = t.value if isinstance(t, ast.Subscript) else t
                if _is_self_attr(target, attr):
                    out.append(node)
        elif isinstance(node, ast.AnnAssign):
            target = node.target.value if isinstance(node.target, ast.Subscript) else node.target
            if _is_self_attr(target, attr):
                out.append(node)
        elif isinstance(node, ast.AugAssign):
            target = node.target.value if isinstance(node.target, ast.Subscript) else node.target
            if _is_self_attr(target, attr):
                out.append(node)
    return out


def _pop_nodes(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> list[tuple[int, str]]:
    """Sitios `self.<attr>.pop(...)` o `del self.<attr>[k]`."""
    out: list[tuple[int, str]] = []
    for node in ast.walk(method):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if _is_self_attr(node.func.value, attr) and node.func.attr in {"pop", "clear", "popitem"}:
                out.append((node.lineno, f"{attr}.{node.func.attr}(...)"))
        elif isinstance(node, ast.Delete):
            for t in node.targets:
                if isinstance(t, ast.Subscript) and _is_self_attr(t.value, attr):
                    out.append((node.lineno, f"del {attr}[...]"))
    return out


def _reads_store_value(value: ast.AST, attr: str) -> bool:
    """¿El valor lee el store en algún punto (Call get/__getitem__ o subíndice `self.attr[...]`)?"""
    for n in ast.walk(value):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
            if n.func.attr in {"get", "__getitem__"} and _is_self_attr(n.func.value, attr):
                return True
        elif isinstance(n, ast.Subscript) and _is_self_attr(n.value, attr):
            return True
    return False


def _fed_names(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> set[str]:
    """Nombres locales derivados del store (punto fijo): directos (`x = self.attr.get(...)`)
    y transitivos (`y = x + 1` donde `x` ya es derivado)."""
    fed: set[str] = set()
    changed = True
    while changed:
        changed = False
        for node in ast.walk(method):
            if not isinstance(node, ast.Assign):
                continue
            value = node.value
            if not _reads_store_value(value, attr):
                names = {n.id for n in ast.walk(value) if isinstance(n, ast.Name)}
                if not (names & fed):
                    continue
            for t in node.targets:
                targets: list[str] = []
                if isinstance(t, ast.Name):
                    targets = [t.id]
                elif isinstance(t, ast.Tuple):
                    targets = [e.id for e in t.elts if isinstance(e, ast.Name)]
                for name in targets:
                    if name not in fed:
                        fed.add(name)
                        changed = True
    return fed


def _write_uses_prev(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> bool:
    """¿El valor escrito usa el previo del store (subíndice directo o nombre alimentado)?"""
    fed = _fed_names(method, attr)
    for node in ast.walk(method):
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for t in targets:
            target = t.value if isinstance(t, ast.Subscript) else t
            if not _is_self_attr(target, attr) or node.value is None:
                continue
            refs_attr = any(isinstance(n, ast.Subscript) and _is_self_attr(n.value, attr) for n in ast.walk(node.value))
            names = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name)}
            if refs_attr or bool(names & fed):
                return True
    return False


def _has_reduce_shape(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> bool:
    """¿Reducción real: Sub cuyo subárbol referencia el store o un nombre derivado de él?"""
    fed = _fed_names(method, attr)
    for node in ast.walk(method):
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub)):
            continue
        names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        reads_store = any(_is_self_attr(n, attr) for n in ast.walk(node))
        if reads_store or bool(names & fed):
            return True
    return False


def _is_wac_formula(method: ast.FunctionDef | ast.AsyncFunctionDef, attr: str) -> bool:
    """Aritmética WAC: Div cuyo subárbol combina Add y Mult y referencia el store (o su lectura)."""
    fed = _fed_names(method, attr)
    for node in ast.walk(method):
        if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div)):
            continue
        has_add = any(isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add) for n in ast.walk(node))
        has_mul = any(isinstance(n, ast.BinOp) and isinstance(n.op, ast.Mult) for n in ast.walk(node))
        names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        reads_store = any(_is_self_attr(n, attr) for n in ast.walk(node)) or bool(names & fed)
        if has_add and has_mul and reads_store:
            return True
    return False


def _attr_value_shapes(cls: ClassInfo, attr: str) -> list[str]:
    """Formas de los valores asignados al store (p. ej. tupla de 2-3 => 'pair')."""
    shapes: list[str] = []
    for method in cls.method_nodes.values():
        for node in ast.walk(method):
            if not isinstance(node, ast.Assign):
                continue
            for t in node.targets:
                if not (isinstance(t, ast.Subscript) and _is_self_attr(t.value, attr)):
                    continue
                if isinstance(node.value, ast.Tuple) and len(node.value.elts) in (2, 3):
                    shapes.append("pair")
    return shapes


def _looks_like_position_attr(name: str, annotation: Optional[str], shapes: list[str]) -> bool:
    if any(p in name for p in POSITION_ATTR_PATTERNS):
        return True
    if annotation and any(h in annotation for h in POSITION_TYPE_HINTS):
        return True
    if "pair" in shapes:
        return True
    return False


def _looks_like_order_attr(name: str, annotation: Optional[str]) -> bool:
    if any(p in name for p in ORDER_ATTR_PATTERNS):
        return True
    if annotation and any(h in annotation for h in ORDER_TYPE_HINTS):
        return True
    return False


def has_container_mutation(cls: ClassInfo, attr: str) -> bool:
    """¿El store recibe escrituras de contenedor (subíndice/pop/update/del)?
    False => contador escalar (`self.x = ...` / `self.x += 1`), no aplica semántica de colección."""
    for method in cls.method_nodes.values():
        for node in ast.walk(method):
            if isinstance(node, ast.Assign):
                if any(isinstance(t, ast.Subscript) and _is_self_attr(t.value, attr) for t in node.targets):
                    return True
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Subscript) and _is_self_attr(node.target.value, attr):
                    return True
            elif isinstance(node, ast.AugAssign):
                if isinstance(node.target, ast.Subscript) and _is_self_attr(node.target.value, attr):
                    return True
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if _is_self_attr(node.func.value, attr) and node.func.attr in {
                    "pop",
                    "update",
                    "setdefault",
                    "clear",
                    "popitem",
                }:
                    return True
            elif isinstance(node, ast.Delete):
                if any(isinstance(t, ast.Subscript) and _is_self_attr(t.value, attr) for t in node.targets):
                    return True
    return False


def find_position_stores(ctx, scope_roots: tuple[str, ...]) -> list[PositionStore]:
    """Detecta almacenes mutables de posición en los roots indicados.

    Criterio (evidencia correlacionada, no solo nombre):
      1. atributo `self._x` anotado como contenedor (dict/list/set) o con patrón de nombre;
      2. mutación real (subscript write, pop, update, del, setdefault);
      3. evidencia de concepto: nombre, anotación o forma de valor asignado (par).
    """
    stores: list[PositionStore] = []
    for path in ctx.files:
        if not any(path.is_relative_to(ctx.root / r) for r in scope_roots):
            continue
        info = ctx.module(path)
        if info is None:
            continue
        text = ctx.text(path)
        for cls in info.classes:
            for attr_name, attr in cls.attrs.items():
                shapes = _attr_value_shapes(cls, attr_name)
                # Contenedor por anotación, patrón de nombre o comportamiento (forma de valor).
                if (
                    not _is_container_typed(attr.annotation)
                    and not any(p in attr_name for p in POSITION_ATTR_PATTERNS)
                    and "pair" not in shapes
                ):
                    continue
                if not attr.mutations:
                    continue
                if not _looks_like_position_attr(attr_name, attr.annotation, shapes):
                    continue
                store = PositionStore(
                    owner_class=f"{path}:{cls.name}",
                    attr=attr_name,
                    file=str(path),
                    line=attr.line,
                    value_type=attr.annotation or "container",
                )
                for ln in attr.mutations:
                    store.mutations.append(Evidence(str(path), ln, attr_name, _line_text(text, ln)))
                for ln in attr.reads:
                    store.reads.append(Evidence(str(path), ln, attr_name, _line_text(text, ln)))
                stores.append(store)
    return stores


def find_order_stores(ctx) -> list[OrderStore]:
    """Detecta almacenes mutables de órdenes (contenedores de Order)."""
    stores: list[OrderStore] = []
    for path in ctx.files:
        if not any(path.is_relative_to(ctx.root / r) for r in ("packages/trading",)):
            continue
        info = ctx.module(path)
        if info is None:
            continue
        text = ctx.text(path)
        for cls in info.classes:
            for attr_name, attr in cls.attrs.items():
                if not _is_container_typed(attr.annotation) and not any(p in attr_name for p in ORDER_ATTR_PATTERNS):
                    continue
                if not attr.mutations:
                    continue
                if not _looks_like_order_attr(attr_name, attr.annotation):
                    continue
                store = OrderStore(
                    owner_class=f"{path}:{cls.name}",
                    attr=attr_name,
                    file=str(path),
                    line=attr.line,
                )
                for ln in attr.mutations:
                    store.mutations.append(Evidence(str(path), ln, attr_name, _line_text(text, ln)))
                stores.append(store)
    return stores


def find_global_mutable_stores(ctx, scope_roots: tuple[str, ...]) -> list[tuple[str, str, int, list[Evidence]]]:
    """Contenedores mutables a nivel de módulo (estado global) en los roots indicados.

    Devuelve tuplas (file, name, line, evidence) de contenedores declarados a nivel
    de módulo (dict/list/set) que además reciben mutaciones.
    """
    out: list[tuple[str, str, int, list[Evidence]]] = []
    for path in ctx.files:
        if not any(path.is_relative_to(ctx.root / r) for r in scope_roots):
            continue
        info = ctx.module(path)
        if info is None:
            continue
        text = ctx.text(path)
        for gs in info.global_stores:
            if not gs.mutations:
                continue
            evidence = [Evidence(str(path), gs.line, gs.name, f"estado global {gs.name}")]
            for ln in gs.mutations[:3]:
                evidence.append(Evidence(str(path), ln, gs.name, _line_text(text, ln)))
            out.append((str(path), gs.name, gs.line, evidence))
    return out


def analyze_owner_semantics(ctx, path: Path, cls: ClassInfo, attr: str) -> OwnerSemantics:
    """Clasifica la semántica de escritura de `attr` en la clase (por método, AST)."""
    owner = OwnerSemantics(owner_class=f"{path}:{cls.name}", file=str(path))
    seen: set[str] = set()

    for method in cls.method_nodes.values():
        if method.name in {"__init__", "__new__"}:
            continue  # inicialización pura: no es divergencia semántica
        writes = _write_nodes(method, attr)
        pops = _pop_nodes(method, attr)

        if writes:
            # ¿el valor escrito usa el previo del store (acumulación) o lo reemplaza sin leerlo?
            if _write_uses_prev(method, attr):
                _add_op(
                    owner,
                    SEM_ACCUMULATE,
                    Evidence(str(path), method.lineno, f"{cls.name}.{method.name}", f"acumula sobre {attr}"),
                    seen,
                )
                if _is_wac_formula(method, attr):
                    _add_op(
                        owner,
                        SEM_WAC,
                        Evidence(str(path), method.lineno, f"{cls.name}.{method.name}", f"aritmética WAC sobre {attr}"),
                        seen,
                    )
            else:
                _add_op(
                    owner,
                    SEM_REPLACE,
                    Evidence(
                        str(path), method.lineno, f"{cls.name}.{method.name}", f"reemplaza {attr} sin leer previo"
                    ),
                    seen,
                )
            if _has_reduce_shape(method, attr):
                _add_op(
                    owner,
                    SEM_REDUCE,
                    Evidence(str(path), method.lineno, f"{cls.name}.{method.name}", f"reducción (Sub) sobre {attr}"),
                    seen,
                )
        for line, desc in pops:
            _add_op(owner, SEM_POP, Evidence(str(path), line, f"{cls.name}.{method.name}", desc), seen)

    return owner


def _add_op(owner: OwnerSemantics, tag: str, evidence: Evidence, seen: set[str]) -> None:
    key = f"{tag}:{evidence.symbol}:{evidence.line}"
    if key in seen:
        return
    seen.add(key)
    owner.ops.setdefault(tag, []).append(evidence)


def _line_text(text: Optional[str], lineno: int) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    if 1 <= lineno <= len(lines):
        return lines[lineno - 1].strip()
    return ""
