# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_d15_boundary.py
===============================================

Frontera D15 — BLOCKING boundary (ADR-0049, G1-D15).

D15 (2026-09-29, decisión humana): NO existe relación económica entre
Listing Identities distintas; EconomicGroup LATER; execution
interchangeability SEPARATE FUTURE DECISION. D19 BLOCKED (representación
física del ID).

Este test escanea el código REAL del paquete de identidad implementado en
GATE B en busca de tokens prohibidos y de abstracciones funcionalmente
equivalentes. NO es un test que comprueba nombres de clases: verifica que
ninguna fuente del paquete modela equivalencia económica o representación
física del ID.

ALCANCE: packages/market_data/domain/entities/listing_identity.py,
packages/market_data/domain/value_objects/{relations,symbol_binding,
contract_terms,instrument_type,lifecycle,provenance}.py,
packages/market_data/domain/policies/identity_resolution.py,
packages/market_data/ports/outbound/instrument_identity_registry.py,
packages/market_data/application/use_cases/resolve_instrument_identity.py
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Tokens prohibidos por D15 (relación económica / equivalencia cross-venue)
# y por D19 (representación física del ID). Cualquier aparición en el
# código del paquete es un fallo de frontera.
FORBIDDEN_TOKENS = [
    "economic group",
    "economicgroup",
    "economic equivalence",
    "equivalence",
    "cross_venue",
    "interchangeability",
    "interchangeable",
    "execution equivalence",
    "economic relationship",
    "fungibility",
    "uuid",
    "wire format",
]

# Archivos en los que se exige ausencia absoluta de estos tokens.
IMPLEMENTED_FILES = [
    "packages/market_data/domain/entities/listing_identity.py",
    "packages/market_data/domain/value_objects/symbol_binding.py",
    "packages/market_data/domain/value_objects/relations.py",
    "packages/market_data/domain/value_objects/contract_terms.py",
    "packages/market_data/domain/value_objects/instrument_type.py",
    "packages/market_data/domain/value_objects/lifecycle.py",
    "packages/market_data/domain/value_objects/provenance.py",
    "packages/market_data/domain/policies/identity_resolution.py",
    "packages/market_data/ports/outbound/instrument_identity_registry.py",
    "packages/market_data/application/use_cases/resolve_instrument_identity.py",
]


class TestD15Boundary:
    def test_no_prohibited_concepts_in_identity_code(self):
        violations = []
        for rel in IMPLEMENTED_FILES:
            path = REPO_ROOT / rel
            if not path.exists():
                violations.append(f"MISSING FILE: {rel}")
                continue
            lines = path.read_text(encoding="utf-8").splitlines()
            for line_no, line in enumerate(lines, 1):
                low = line.lower()
                # Tolerancia de docstrings normativos: "NO decide UUID /
                # wire format / ... D19 BLOCKED" son referencias negadas, no
                # la introducción del concepto. Solo falla si el token
                # aparece sin negación en la misma línea.
                negated = any(m in low for m in ("no ", "not ", "blocked", "no decide", "nunca"))
                for token in FORBIDDEN_TOKENS:
                    if token in low and not negated:
                        violations.append(f"{rel}:{line_no}: token prohibido {token!r}")
        assert violations == [], "D15/D19 footprint detectado:\n" + "\n".join(violations)

    def test_no_economic_group_abstraction(self):
        """No existe clase/clase-equivalente con nombre económico."""
        imported = __import__("market_data.domain.value_objects.relations", fromlist=["RelationType"])
        names = [m for m in dir(imported.RelationType) if not m.startswith("_")]
        assert "ECONOMIC" not in [n.upper() for n in names]
