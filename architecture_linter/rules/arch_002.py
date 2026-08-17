"""ARCH-002 — Position Semantic Divergence.

Detecta operaciones semánticamente incompatibles sobre el mismo concepto
(posición): acumulación/WAC (lee el previo) vs reemplazo sin lectura (pyramid),
reducción vs pop incondicional. La semántica se obtiene por AST (patrones de
escritura/lectura/aritmética), no por nombres de variables (`new_qty`/`remaining`).
No marca FAIL solo por existir varias estructuras: exige divergencia real.
"""

from __future__ import annotations

from pathlib import Path

from architecture_linter.analyzers.mutable_state import (
    SEM_ACCUMULATE,
    SEM_POP,
    SEM_REDUCE,
    SEM_REPLACE,
    SEM_WAC,
    analyze_owner_semantics,
    find_position_stores,
    has_container_mutation,
)
from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule

# El SSOT de posición (portfolio/infra, portfolio/ports) reemplaza entradas por
# diseño (API de store) — no es divergencia semántica.
SSOT_HINTS = ("portfolio/infra", "portfolio/ports", "InMemoryPositionStore", "RedisPositionStore")


class Arch002Rule(Rule):
    rule_id = "ARCH-002"
    rule_name = "Position Semantic Divergence"
    description = (
        "Compara la semántica de escritura de cada owner de posición (acumulación/WAC, "
        "reemplazo, reducción, pop) mediante análisis AST de los métodos, y FAIL solo "
        "si hay operaciones incompatibles sobre el mismo concepto."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        stores = find_position_stores(ctx, scope_roots=("packages/trading", "packages/portfolio"))
        if not stores:
            return []

        # Semántica por owner (no por nombre de variable): acumulación/WAC vs reemplazo
        accumulate_owners: list[tuple[str, str, list[Evidence]]] = []
        replace_owners: list[tuple[str, str, list[Evidence]]] = []
        reduce_owners: list[tuple[str, str, list[Evidence]]] = []
        pop_owners: list[tuple[str, str, list[Evidence]]] = []

        for s in stores:
            if any(h in s.file for h in SSOT_HINTS):
                continue  # store API por diseño
            path = Path(s.file)
            info = ctx.module(path)
            if not info:
                continue
            cls = next((c for c in info.classes if f"{path}:{c.name}" == s.owner_class), None)
            if cls is None:
                continue
            if not has_container_mutation(cls, s.attr):
                continue  # contador escalar (ej. RiskManager._open_positions: int): semántica no aplica
            owner = analyze_owner_semantics(ctx, path, cls, s.attr)
            tags = owner.tags
            ev = list(owner.ops.get(tag, []) for tag in tags)
            flat = [e for lst in ev for e in lst]
            if SEM_WAC in tags or SEM_ACCUMULATE in tags:
                accumulate_owners.append((s.file, f"{cls.name}.{s.attr}", flat))
            if SEM_REPLACE in tags:
                replace_owners.append((s.file, f"{cls.name}.{s.attr}", flat))
            if SEM_REDUCE in tags:
                reduce_owners.append((s.file, f"{cls.name}.{s.attr}", flat))
            if SEM_POP in tags:
                pop_owners.append((s.file, f"{cls.name}.{s.attr}", flat))

        findings: list[Finding] = []
        # Divergencia 1: acumulación/WAC vs reemplazo sin leer el previo (pyramid).
        # Precedencia a nivel de fichero: un fichero con acumulación (WAC) no se
        # reporta a la vez como "reemplazo" del mismo concepto (mitiga el ruido de
        # stores espejo como OMS._open junto al store WAC OMS._entry_positions).
        accum_files = {f for f, _, _ in accumulate_owners}
        replace_owners = [(f, o, ev) for f, o, ev in replace_owners if f not in accum_files]
        if accumulate_owners and replace_owners:
            evidence = [e for _, _, evs in accumulate_owners + replace_owners for e in evs]
            findings.append(
                self.finding(
                    Status.FAIL,
                    "Divergencia semántica: algunos owners acumulan/WAC (leen el previo) y otros "
                    "reemplazan la entrada sin leerla. "
                    f"WAC/acumulación: {', '.join(o for _, o, _ in accumulate_owners)}; "
                    f"reemplazo: {', '.join(o for _, o, _ in replace_owners)}.",
                    file=replace_owners[0][0],
                    symbol="position",
                    evidence=evidence,
                    related_files=sorted({f for f, _, _ in (accumulate_owners + replace_owners)}),
                    related_symbols=[o for _, o, _ in (accumulate_owners + replace_owners)],
                    confidence=0.85,
                    concept="position",
                )
            )
        # Divergencia 2: reducción vs pop incondicional. Requiere owners DISTINTOS:
        # el mismo store que reduce y hace pop es lógica SELL coherente (cierre parcial
        # vs cierre total), no una divergencia entre responsables.
        reduce_ids = {o for _, o, _ in reduce_owners}
        pop_ids = {o for _, o, _ in pop_owners}
        if reduce_owners and pop_owners and reduce_ids != pop_ids:
            evidence = [e for _, _, evs in reduce_owners + pop_owners for e in evs]
            findings.append(
                self.finding(
                    Status.FAIL,
                    "Divergencia semántica: SELL reduce (Sub sobre el previo) en unos owners y se hace "
                    f"`pop` incondicional en otros. reduce: {', '.join(o for _, o, _ in reduce_owners)}; "
                    f"pop: {', '.join(o for _, o, _ in pop_owners)}.",
                    file=pop_owners[0][0],
                    symbol="position",
                    evidence=evidence,
                    related_files=sorted({f for f, _, _ in (reduce_owners + pop_owners)}),
                    related_symbols=[o for _, o, _ in (reduce_owners + pop_owners)],
                    confidence=0.85,
                    concept="position",
                )
            )
        if not findings:
            findings.append(
                self.finding(
                    Status.PASS,
                    "No se detectó divergencia semántica entre owners de posición (WAC/replace/reduce/pop coherentes).",
                    related_files=sorted({s.file for s in stores}),
                    confidence=0.8,
                    concept="position",
                )
            )
        return findings
