"""ARCH-007 — Duplicate / Homonymous Contracts.

Detecta conceptos duplicados con el mismo nombre en distintos módulos.
No basta el mismo nombre: compara módulo, semántica (miembros), valores
(enums) y consumidores para clasificar duplicate vs homonymous.
"""

from __future__ import annotations

import ast
from pathlib import Path

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule


class Arch007Rule(Rule):
    rule_id = "ARCH-007"
    rule_name = "Duplicate / Homonymous Contracts"
    description = (
        "Detecta clases/enums/DTOs/ports con el mismo nombre en módulos distintos y compara "
        "semántica (miembros, valores, bases) para clasificar duplicación real."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        # Agrupar definiciones por nombre
        by_name: dict[str, list[tuple[Path, str, int]]] = {}
        for path, name, line in ctx.all_classes():
            by_name.setdefault(name, []).append((path, name, line))

        findings: list[Finding] = []
        for name, defs in sorted(by_name.items()):
            if len(defs) < 2:
                continue
            if name in self.allow:
                continue  # excepción justificada (config), p. ej. CompositionRoot por BC (ADR-0003)
            # Agrupar definiciones reales (distintos archivos)
            unique_files = {p for (p, _n, _l) in defs}
            if len(unique_files) < 2:
                continue
            if _is_mirror_pattern(defs):
                continue  # patrón documentado: Hydra Structured Config ↔ Pydantic SSOT (ocm/config)

            members_by_file: dict[Path, set[str]] = {}
            for p, _n, _l in defs:
                members_by_file[p] = _class_members(ctx, p, name)

            semantics_differ = len({tuple(sorted(v)) for v in members_by_file.values()}) > 1
            consumers_by_file = {p: _consumers(ctx, p, name) for (p, _n, _l) in defs}
            both_consumed = sum(1 for c in consumers_by_file.values() if c) >= 2

            evidence = []
            for p, _n, line in defs:
                evidence.append(Evidence(str(p), line, name, f"definición de {name} en {p.name}"))
            for p, _n, _l in defs:
                evidence.append(Evidence(str(p), None, None, f"consumidores: {len(consumers_by_file[p])}"))

            # Clasificación
            kind = "homónimos (semántica distinta)" if semantics_differ else "duplicados (misma semántica)"
            if both_consumed or (not semantics_differ and len(unique_files) >= 2):
                findings.append(
                    self.finding(
                        Status.FAIL,
                        f"{name} definido en {len(unique_files)} módulos — {kind}: "
                        + ", ".join(sorted(str(p) for p in unique_files)),
                        file=str(defs[0][0]),
                        line=defs[0][2],
                        symbol=name,
                        evidence=evidence,
                        related_files=sorted(str(p) for p in unique_files),
                        related_symbols=[name],
                        confidence=0.9,
                        concept="contract",
                    )
                )
        return findings or [
            self.finding(
                Status.PASS,
                "Sin contratos duplicados/homónimos detectados.",
                confidence=0.9,
                concept="contract",
            )
        ]


def _class_members(ctx: RepoContext, path: Path, name: str) -> set[str]:
    info = ctx.module(path)
    if not info:
        return set()
    for cls in info.classes:
        if cls.name == name:
            members = set()
            for mname, _line in cls.methods:
                members.add(mname)
            for attr in cls.attrs:
                members.add(attr)
            members.update(cls.bases)
            # valores de constantes/enums a nivel de clase (PENDING = 'pending')
            members.update(_class_constant_values(ctx, path, name))
            return members
    return set()


def _class_constant_values(ctx: RepoContext, path: Path, name: str) -> set[str]:
    """Valores de asignaciones a nivel de clase (`PENDING = 'pending'`)."""
    text = ctx.text(path)
    if not text:
        return set()
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()
    out: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != name:
            continue
        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        try:
                            out.add(f"{target.id}={ast.unparse(item.value)}")
                        except Exception:
                            out.add(f"{target.id}=?")
            elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                try:
                    out.add(f"{item.target.id}={ast.unparse(item.value) if item.value else ':'}")
                except Exception:
                    out.add(f"{item.target.id}=?")
        break
    return out


def _consumers(ctx: RepoContext, path: Path, name: str) -> list:
    return [(p, line) for (p, line) in ctx.references(name) if p != path]


def _is_mirror_pattern(defs: list[tuple[Path, str, int]]) -> bool:
    """Detecta el patrón espejo documentado de ocm/config:
    Hydra Structured Config (ocm/config/structured/*.py) ↔ Pydantic SSOT
    (ocm/config/schema.py o ocm/observability/config.py). Los docstrings lo
    declaran ('modelo Pydantic espejo'). No es una duplicación accidental."""
    rel = {str(p).replace("\\", "/") for (p, _n, _l) in defs}
    has_structured = any("/ocm/config/structured/" in r for r in rel)
    has_pydantic_ssot = any(
        r.endswith("ocm/config/schema.py") or r.endswith("ocm/observability/config.py") for r in rel
    )
    return has_structured and has_pydantic_ssot
