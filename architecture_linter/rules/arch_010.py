"""ARCH-010 — Duplicated Mutable State.

Detecta estado mutable duplicado de conceptos críticos (position, order) y
estado mutable global a nivel de módulo. La detección correlaciona
declaración → mutación → lectura y forma del valor (no solo el nombre):
`_book[sym] = (qty, price)` cuenta igual que `_positions[sym] = ...`.
Diferencia cache / projection / snapshot / SSOT / derived / mutable owner.
"""

from __future__ import annotations

from architecture_linter.analyzers.mutable_state import (
    find_global_mutable_stores,
    find_order_stores,
    find_position_stores,
)
from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule


class Arch010Rule(Rule):
    rule_id = "ARCH-010"
    rule_name = "Duplicated Mutable State"
    description = (
        "Detecta estado mutable duplicado de conceptos críticos y estado global mutable, "
        "clasificándolo por evidencia estructural (declaración→mutación→lectura→forma)."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        findings: list[Finding] = []

        positions = find_position_stores(ctx, scope_roots=("packages/trading", "packages/portfolio"))
        orders = find_order_stores(ctx)

        # Clasificación por concepto
        concept_stores: dict[str, list] = {
            "position": positions,
            "order": orders,
        }

        for concept, stores in concept_stores.items():
            if not stores:
                continue
            if len(stores) < 2:
                # un solo almacén mutable → no duplicado (puede ser SSOT o mirror)
                findings.append(
                    self.finding(
                        Status.PASS,
                        f"Concepto '{concept}': un solo almacén mutable (no duplicado).",
                        related_files=sorted({s.file for s in stores}),
                        confidence=0.85,
                        concept=concept,
                    )
                )
                continue

            # múltiples almacenes: clasificar SSOT vs mirrors vs owners
            evidence: list[Evidence] = []
            for s in stores:
                evidence.append(
                    Evidence(
                        s.file,
                        s.line,
                        f"{s.owner_class.split(':')[-1]}.{s.attr}",
                        f"almacén mutable de {concept} (mutaciones: {len(s.mutations)})",
                    )
                )
            findings.append(
                self.finding(
                    Status.FAIL,
                    f"Estado mutable de '{concept}' duplicado en {len(stores)} almacenes: "
                    + ", ".join(f"{s.owner_class.split(':')[-1]}.{s.attr}" for s in stores),
                    file=stores[0].file,
                    line=stores[0].line,
                    symbol=concept,
                    evidence=evidence,
                    related_files=sorted({s.file for s in stores}),
                    related_symbols=[f"{s.owner_class.split(':')[-1]}.{s.attr}" for s in stores],
                    confidence=0.9,
                    concept=concept,
                )
            )

        # Estado mutable global a nivel de módulo (contenedores mutados en módulo)
        global_stores = find_global_mutable_stores(ctx, scope_roots=("packages/trading", "packages/portfolio"))
        if global_stores:
            evidence = [e for _, _, _, evs in global_stores for e in evs]
            findings.append(
                self.finding(
                    Status.FAIL,
                    "Estado mutable global a nivel de módulo en trading/portfolio: "
                    + ", ".join(f"{name} ({file})" for file, name, _, _ in global_stores),
                    file=global_stores[0][0],
                    line=global_stores[0][2],
                    symbol=global_stores[0][1],
                    evidence=evidence,
                    related_files=sorted({f for f, _, _, _ in global_stores}),
                    related_symbols=[n for _, n, _, _ in global_stores],
                    confidence=0.8,
                    concept="state",
                )
            )

        return findings or [
            self.finding(
                Status.PASS,
                "Sin estado mutable duplicado de conceptos críticos ni estado global.",
                confidence=0.9,
                concept="state",
            )
        ]
