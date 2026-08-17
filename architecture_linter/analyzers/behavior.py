"""architecture_linter.analyzers.behavior — análisis de comportamiento para stubs.

Detecta clases de producción que exponen una capacidad sin ejecutarla, por
evidencia estructural de comportamiento (no solo por marcador documental):

  * raise NotImplementedError en un método público/dunder;
  * raise StopAsyncIteration inmediato en un stream (`__anext__`/`__next__`);
  * estado `_running`/`_active`/`_started` que nunca alcanza True (nunca operativo);
  * cáscara: todos los métodos públicos/dunder vacíos (cuerpo sin sentencias).

El marcador documental (NOT_IMPLEMENTED) solo es evidencia de refuerzo, nunca
el único trigger: distinguir documentación legítima de comportamiento ejecutable.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Optional

from architecture_linter.analyzers.ast_walk import ClassInfo
from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence

NEVER_TRUE_STATE_ATTRS = ("_running", "_active", "_started", "_is_running")


class StubEvidence:
    """Razones y evidencia de que una clase es un stub de producción."""

    def __init__(self, file: str, class_name: str, class_line: int) -> None:
        self.file = file
        self.class_name = class_name
        self.class_line = class_line
        self.triggers: list[str] = []
        self.evidence: list[Evidence] = []

    @property
    def is_stub(self) -> bool:
        return bool(self.triggers)


def _method_is_public_or_dunder(name: str) -> bool:
    return not name.startswith("_") or name.startswith("__")


def _is_log_call(call: ast.Call) -> bool:
    """Llamada a logger (`logger.*`, `logging.*`, `self._log.*`): sin efecto observable."""
    func = call.func
    if not isinstance(func, ast.Attribute):
        return False
    if isinstance(func.value, ast.Name) and func.value.id in {"logger", "logging"}:
        return True
    return (
        isinstance(func.value, ast.Attribute)
        and isinstance(func.value.value, ast.Name)
        and func.value.value.id == "self"
        and func.value.attr == "_log"
    )


def _is_noop_body(method: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Cuerpo sin efecto observable: solo docstring, `pass`, `return` nulo o logging."""
    for stmt in method.body:
        if isinstance(stmt, ast.Expr):
            if isinstance(stmt.value, ast.Constant):
                continue  # docstring
            if isinstance(stmt.value, ast.Call) and _is_log_call(stmt.value):
                continue  # solo logging
            return False
        if isinstance(stmt, ast.Pass):
            continue
        if isinstance(stmt, ast.Return) and stmt.value is None:
            continue
        return False
    return True


def _is_null_object_name(name: str) -> bool:
    """Null-object intencional (NoopXxx / XxxNoop / NullXxx): exclusión deliberada documentada."""
    lowered = name.lower()
    return lowered.startswith(("null", "noop")) or "noop" in lowered


def _is_immediate_stop_stream(method: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Stream que termina de inmediato sin producir: `__anext__`/`__next__` sin
    `await`, sin `yield` y sin `return <valor>` antes del StopAsyncIteration."""
    if method.name not in {"__anext__", "__next__"}:
        return False
    has_await = any(isinstance(n, ast.Await) for n in ast.walk(method))
    has_yield = any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(method))
    returns_value = any(isinstance(n, ast.Return) and n.value is not None for n in ast.walk(method))
    return not has_await and not has_yield and not returns_value


def _never_true_state(cls: ClassInfo) -> list[tuple[str, int]]:
    """Atributos de estado (`_running`, ...) inicializados False y nunca asignados True."""
    out: list[tuple[str, int]] = []
    for attr, inst in cls.attrs.items():
        if not any(a in attr for a in NEVER_TRUE_STATE_ATTRS):
            continue
        init_false = False
        set_true = False
        for method in cls.method_nodes.values():
            for node in ast.walk(method):
                if isinstance(node, ast.Assign):
                    for t in node.targets:
                        if _is_self_attr_ast(t, attr):
                            init_false = (
                                init_false or isinstance(node.value, ast.Constant) and node.value.value is False
                            )
                            set_true = set_true or isinstance(node.value, ast.Constant) and node.value.value is True
                elif isinstance(node, ast.AnnAssign) and _is_self_attr_ast(node.target, attr):
                    init_false = init_false or isinstance(node.value, ast.Constant) and node.value.value is False
                    set_true = set_true or isinstance(node.value, ast.Constant) and node.value.value is True
        if init_false and not set_true:
            out.append((attr, inst.line))
    return out


def _is_self_attr_ast(node, attr: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        and node.attr == attr
    )


def analyze_stub_class(ctx: RepoContext, path: Path, cls: ClassInfo) -> Optional[StubEvidence]:
    """Devuelve StubEvidence si la clase es un stub; None si es una implementación real."""
    if cls.is_protocol or cls.is_abstract or cls.is_enum:
        return None

    stub = StubEvidence(str(path), cls.name, cls.line)

    # 1) raise NotImplementedError en método público/dunder
    for site in cls.raise_sites:
        if site.exc_name == "NotImplementedError":
            stub.evidence.append(
                Evidence(str(path), site.line, f"{cls.name}.{site.method}", "raise NotImplementedError")
            )
            if _method_is_public_or_dunder(site.method):
                stub.triggers.append(f"raise NotImplementedError en {cls.name}.{site.method}")

    # 2) StopAsyncIteration en un stream que termina de inmediato sin producir
    for site in cls.raise_sites:
        if site.exc_name != "StopAsyncIteration":
            continue
        method_node = cls.method_nodes.get(site.method)
        if method_node is not None and _is_immediate_stop_stream(method_node):
            stub.evidence.append(
                Evidence(str(path), site.line, f"{cls.name}.{site.method}", "raise StopAsyncIteration")
            )
            stub.triggers.append(f"{cls.name}.{site.method} termina de inmediato (StopAsyncIteration)")

    # 3) estado de ciclo de vida que nunca alcanza operativo (True)
    for attr, line in _never_true_state(cls):
        stub.evidence.append(Evidence(str(path), line, f"{cls.name}.{attr}", f"{attr} nunca True (nunca operativo)"))
        stub.triggers.append(f"{cls.name}.{attr} nunca True")

    # 4) cáscara: todos los métodos de capacidad públicos (no dunder) sin efecto observable y hay ≥2.
    #    Se excluyen los null-objects intencionales (Null*/Noop*) — patrón deliberado, no stub.
    if cls.is_null_object_name:
        pass
    elif len(cls.capability_methods) >= 2 and all(_is_noop_body(m) for m in cls.capability_methods):
        for m in cls.capability_methods:
            stub.evidence.append(
                Evidence(str(path), m.lineno, f"{cls.name}.{m.name}", "método de capacidad sin efecto observable")
            )
        stub.triggers.append(f"{cls.name}: {len(cls.capability_methods)} métodos de capacidad no-op")

    # Marcador documental: solo refuerzo, nunca trigger
    markers = [m for m in cls.docstring_markers if "not_implemented" in m.lower() or "not implemented" in m.lower()]
    if markers:
        stub.evidence.append(Evidence(str(path), cls.line, cls.name, f"docstring: {', '.join(markers)}"))

    return stub if stub.is_stub else None
