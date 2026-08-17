"""ARCH-003 — Order State Without Reconciliation.

Distinguishes PASS / FAIL / PARTIAL / UNKNOWN for the in-memory order state
reconciliation responsibility (ADR-0029):

  * no order stores in trading/execution → UNKNOWN (no se puede auditar);
  * stores + gestión periódica de órdenes abiertas → PASS;
  * stores + reconciliación puntual (submit-time fetch_state/OrderTransport) → PARTIAL;
  * stores + ningún mecanismo → FAIL.

La ausencia de un método con determinado nombre no demuestra por sí sola
ausencia de reconciliación: se analizan callers, implementaciones y mutación
estructural del almacén.
"""

from __future__ import annotations

import ast

from architecture_linter.analyzers.mutable_state import find_order_stores
from architecture_linter.engine import RepoContext
from architecture_linter.models import Finding, Status
from architecture_linter.rules.base import Rule

RECONCILIATION_MECHANISMS = (
    "fetch_open_orders",
    "manage_open_orders",
)
PUNTUAL_RECONCILIATION = ("fetch_state", "reconcile", "rehydrat", "startup_recovery")

SCOPE_ROOTS = ("packages/trading",)


class Arch003Rule(Rule):
    rule_id = "ARCH-003"
    rule_name = "Order State Without Reconciliation"
    description = (
        "Distingue PASS/FAIL/PARTIAL/UNKNOWN para la reconciliación del estado de órdenes "
        "en memoria (ADR-0029), analizando callers/implementaciones/mutación del almacén."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        stores = find_order_stores(ctx)
        if not stores:
            return [
                self.finding(
                    Status.UNKNOWN,
                    "No se detectó almacén mutable de órdenes en trading/execution: no se "
                    "puede auditar la responsabilidad de reconciliación.",
                    confidence=0.5,
                    concept="order",
                )
            ]

        # Mecanismos de gestión/reconciliación dentro de trading (no contaminar con otros BCs)
        present = {
            mech: _refs_in(ctx, mech, SCOPE_ROOTS) for mech in RECONCILIATION_MECHANISMS + PUNTUAL_RECONCILIATION
        }
        management = [mech for mech in RECONCILIATION_MECHANISMS if present[mech]]
        puntual = [mech for mech in PUNTUAL_RECONCILIATION if present[mech]]

        # Reconciliación estructural: un método del owner que consulta `.fetch_*` en otro
        # objeto (transport/executor) mientras muta el almacén de órdenes.
        structural_puntual = _structural_reconciliation_callers(ctx, stores)

        evidence = []
        for s in stores:
            for m in s.mutations[:3]:
                evidence.append(m)

        oms_stores = [s for s in stores if "execution/oms.py" in s.file]
        if oms_stores and management:
            return [
                self.finding(
                    Status.PASS,
                    f"Existe gestión de órdenes abiertas en trading: {', '.join(management)}.",
                    related_files=sorted({s.file for s in stores}),
                    confidence=0.9,
                    concept="order",
                )
            ]
        if oms_stores and (puntual or structural_puntual):
            return [
                self.finding(
                    Status.PARTIAL,
                    "Existe reconciliación puntual (submit-time) del estado de órdenes "
                    f"({', '.join(puntual or ['fetch_* en caller del almacén'])}), pero no un "
                    "loop periódico de gestión de órdenes abiertas (fetch_open_orders/"
                    "manage_open_orders ausentes). Órdenes sin fill durante el downtime solo se "
                    "recuperan en el siguiente submit del mismo símbolo.",
                    file=oms_stores[0].file,
                    line=oms_stores[0].line,
                    symbol=oms_stores[0].attr,
                    evidence=evidence,
                    related_files=sorted({s.file for s in stores}),
                    related_symbols=["OMS._orders", "OMS._open", "OrderTransport.fetch_state"],
                    confidence=0.9,
                    concept="order",
                )
            ]
        if oms_stores:
            return [
                self.finding(
                    Status.FAIL,
                    "Órdenes almacenadas solo en memoria (OMS._orders/_open) sin gestión de "
                    "órdenes abiertas ni reconciliación puntual detectada (ADR-0029).",
                    file=oms_stores[0].file,
                    line=oms_stores[0].line,
                    symbol=oms_stores[0].attr,
                    evidence=evidence,
                    related_files=sorted({s.file for s in stores}),
                    related_symbols=["OMS._orders", "OMS._open"],
                    confidence=0.85,
                    concept="order",
                )
            ]
        return [
            self.finding(
                Status.UNKNOWN,
                "Almacenes de órdenes detectados fuera de trading/execution: responsabilidad "
                "de reconciliación no adscrita a una capa auditable.",
                related_files=sorted({s.file for s in stores}),
                confidence=0.5,
                concept="order",
            )
        ]


def _refs_in(ctx: RepoContext, symbol: str, roots: tuple[str, ...]) -> list:
    out = []
    for path, line in ctx.references(symbol):
        if any(path.is_relative_to(ctx.root / r) for r in roots):
            out.append((path, line))
    return out


def _structural_reconciliation_callers(ctx: RepoContext, stores) -> list[str]:
    """Métodos del owner que llaman `.fetch_*` en otro objeto mientras mutan el almacén."""
    out: list[str] = []
    for s in stores:
        info = ctx.module(s.file)
        if not info:
            continue
        owner_name = s.owner_class.split(":")[-1]
        mutation_lines = {m.line for m in s.mutations}
        for cls in info.classes:
            if cls.name != owner_name:
                continue
            for method in cls.method_nodes.values():
                calls_fetch = any(
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and isinstance(n.func.value, ast.Attribute)
                    and n.func.attr.startswith("fetch_")
                    for n in ast.walk(method)
                )
                mutates_store = any(n.lineno in mutation_lines for n in ast.walk(method) if isinstance(n, ast.stmt))
                if calls_fetch and mutates_store:
                    out.append(f"{cls.name}.{method.name}")
    return sorted(set(out))
