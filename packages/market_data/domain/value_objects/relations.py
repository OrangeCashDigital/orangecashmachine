# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/relations.py
===============================================

Relaciones entre Listing Identities (G1-D13/D14, ADR-0049).

D13: las relaciones entre identidades son TIPADAS y DIRECCIONALES, y
conservan su origen/provenancia conceptual. La resolución debe ser
determinista y cualquier ambigüedad relevante → FAIL-CLOSED.

D14: SUCCESSOR_OF y REPLACED_BY son las ÚNICAS relaciones
identity-to-identity declaradas. Ambas operan exclusivamente entre
Listing Identities y son semánticamente distintas:
  · SUCCESSOR_OF — sucesión/continuidad de LINAJE entre identidades
    distintas; NO implica que ambas sean la misma entidad (guarda D09).
  · REPLACED_BY  — sustitución de una identidad por otra.

La clasificación entre SUCCESSOR_OF y REPLACED_BY NO puede inferirse
silenciosamente por símbolo, fecha, venue o lifecycle (D14 R2) — requiere
evidencia y autoridad (D11). ante ambigüedad → UNKNOWN_CONFLICT/FAIL-CLOSED.

SYMBOL_CHANGED_TO NO es relación identity-to-identity: pertenece al nivel
de SymbolBinding (D10/D14) — por eso NO existe aquí.

Principios
----------
DDD   — VO puro: frozen dataclass, definido por valor
SSOT  — única definición de relaciones identity-to-identity en el dominio
FAIL-CLOSED — clasificación ambigua no se inventa; solo se declara
D13/D14 — tipadas, direccionales, con provenance
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource

# ===========================================================================
# RelationType — catálogo cerrado (D13/D14)
# ===========================================================================


class RelationType(str, Enum):
    """
    Relaciones identity-to-identity declaradas (D14): únicamente estas dos.

    No se agregan tipos nuevos aquí: el catálogo está cerrado por D13/D14
    (SYMBOL_CHANGED_TO vive en SymbolBinding, no en identidad).
    """

    SUCCESSOR_OF = "successor_of"
    REPLACED_BY = "replaced_by"


# ===========================================================================
# IdentityRelation
# ===========================================================================


@dataclass(frozen=True)
class IdentityRelation:
    """
    Relación explícita y direccional entre dos Listing Identities.

    source → target: la identidad source tiene la relación `relation_type`
    respecto a target (p. ej. source SUCCESSOR_OF target). Las relaciones
    son asimétricas (direccionales, D13 regla 4).

    provenance es OBLIGATORIO (D11/D13 regla 5): una relación sin origen
    declarado no es admisible. UNKNOWN_CONFLICT no puede generar una
    relación válida — el conflicto debe resolverse formalmente primero.
    """

    source: ListingId
    target: ListingId
    relation_type: RelationType
    provenance: Provenance

    def __post_init__(self) -> None:
        if self.source == self.target:
            raise ValueError("A Listing Identity cannot relate to itself (G1-D13)")
        if self.provenance.source is ProvenanceSource.UNKNOWN_CONFLICT:
            raise ValueError(
                "IdentityRelation cannot carry UNKNOWN_CONFLICT provenance — "
                "resolve the conflict before declaring a relation (G1-D11)"
            )


__all__ = ["RelationType", "IdentityRelation"]
