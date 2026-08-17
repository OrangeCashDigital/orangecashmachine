"""ARCH-001 — Multiple Position State Owners.

Detecta ownership mutable de posición distribuido entre componentes
correlacionando declaración → mutación → lecturas (no solo el nombre).
Distingue SSOT configurado de owners adicionales mutables.
"""

from __future__ import annotations

from architecture_linter.analyzers.mutable_state import find_position_stores
from architecture_linter.engine import RepoContext
from architecture_linter.models import Finding, Status
from architecture_linter.rules.base import Rule

# SSOT de posición por diseño (ADR-0006/BC-43) — instanciable solo en portfolio.
SSOT_HINTS = ("portfolio/infra", "portfolio/ports", "InMemoryPositionStore", "RedisPositionStore")


class Arch001Rule(Rule):
    rule_id = "ARCH-001"
    rule_name = "Multiple Position State Owners"
    description = (
        "Detecta almacenes mutables de posición distribuidos entre componentes; "
        "correlaciona declaración → mutación → lecturas y distingue SSOT (portfolio) de owners."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        stores = find_position_stores(ctx, scope_roots=("packages/trading", "packages/portfolio"))
        if not stores:
            return []

        # Clasificar SSOT vs owners adicionales
        owners = [s for s in stores if not any(h in s.file for h in SSOT_HINTS)]

        findings: list[Finding] = []
        if len(owners) > 1:
            evidence = []
            for s in owners:
                for m in s.mutations[:3]:
                    evidence.append(m)
            related = sorted({s.file for s in stores})
            related_syms = sorted({f"{s.owner_class}:{s.attr}" for s in owners})
            findings.append(
                self.finding(
                    Status.FAIL,
                    f"Posición gestionada por {len(owners)} owners mutables (además del SSOT "
                    f"portfolio): {', '.join(f'{s.owner_class}.{s.attr}' for s in owners)}",
                    file=owners[0].file,
                    line=owners[0].line,
                    symbol=owners[0].attr,
                    evidence=evidence,
                    related_files=related,
                    related_symbols=related_syms,
                    confidence=0.95,
                    concept="position",
                    owner="trading+portfolio (múltiple)",
                    producer="OMS/Risk/TradeTracker",
                )
            )
        else:
            findings.append(
                self.finding(
                    Status.PASS,
                    "Un solo owner mutable de posición (o ninguno) — sin duplicación de ownership.",
                    related_files=sorted({s.file for s in stores}),
                    related_symbols=sorted({f"{s.owner_class}:{s.attr}" for s in stores}),
                    confidence=0.9,
                    concept="position",
                )
            )
        return findings
