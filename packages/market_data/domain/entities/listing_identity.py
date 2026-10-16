# -*- coding: utf-8 -*-
"""
market_data/domain/entities/listing_identity.py
=================================================

ListingIdentity — entidad de identidad canónica de instrumentos (G1).

DDD: una Entity tiene identidad explícita que persiste en el tiempo. Por
eso ListingIdentity vive en domain/entities/ (no en value_objects/): es la
misma entidad negociable con independencia de su símbolo, su lifecycle o
los términos administrativos que la describen (G1-D01/D09).

ListingId (VO opaco)
--------------------
El identificador opaco vive en domain/value_objects/listing_identity.py
(ListingId) y ListingIdentity depende de él (dirección DDD: entities
dependen de value_objects). Este módulo NO redefine el VO; solo lo importa.

Gobierno normativo
------------------
G1-D01  Identity ≠ Symbol; la identidad representa la entidad negociable.
G1-D02  la unidad conceptual es la Listing Identity (estable, con
        continuidad histórica aunque cambien representaciones).
G1-D03  venue-scoped: dos listings del mismo producto en venues distintos
        son identidades distintas (no existe equivalencia cross-venue).
G1-D04  el instrument type es parte del contexto identitario.
G1-D05/06/07/08  los términos identity-defining (underlying, settlement,
        margin convention, expiry, strike/right, multiplier) son parte de
        la definición identitaria cuando aplican.
G1-D09  identidad estable + lifecycle + relaciones explícitas.
G1-D12  DefinitionVersion: versión de la definición de la misma entidad
        negociable; una entidad negociable distinta → identidad nueva.

Principios
----------
DDD   — entidad con identidad persistente; VOs inmutables definidos por valor
SSOT  — única definición de ListingIdentity en el bounded context
DIP   — dominio puro: solo stdlib, sin infraestructura/frameworks
D19   — representación física de la identidad NO decidida (BLOCKED)
D15   — sin relación económica entre identidades distintas
"""

from __future__ import annotations

from dataclasses import dataclass

from market_data.domain.value_objects.contract_terms import ContractTerms
from market_data.domain.value_objects.instrument_type import InstrumentType
from market_data.domain.value_objects.lifecycle import (
    LifecycleState,
    assert_valid_transition,
)
from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource

# ===========================================================================
# ListingIdentity — entidad
# ===========================================================================


@dataclass(frozen=True)
class ListingIdentity:
    """
    Entidad negociable canónica, estable durante su ciclo de vida (G1).

    Attributes
    ----------
    listing_id        : identificador opaco (D19).
    venue             : exchange/venue que origina el listing (G1-D03).
    instrument_type   : categoría fundamental (G1-D04).
    terms             : términos identity-defining (G1-D05..D08).
    lifecycle         : estado actual del ciclo de vida (G1-D09).
    definition_version: versión de la definición (G1-D12). Un cambio que
                        describe la MISMA entidad negociable incrementa la
                        versión; un cambio que define otra entidad negociable
                        crea una identidad nueva (nunca esta versión).
    """

    listing_id: ListingId
    venue: str
    instrument_type: InstrumentType
    terms: ContractTerms
    lifecycle: LifecycleState = LifecycleState.ANNOUNCED
    definition_version: int = 1

    def __post_init__(self) -> None:
        # Coherencia venue/instrument type con los terms (igual que ContractTerms).
        if self.instrument_type is not self.terms.instrument_type:
            raise ValueError(
                "instrument_type y terms.instrument_type deben coincidir "
                f"(got {self.instrument_type.value!r} vs {self.terms.instrument_type.value!r})"
            )

    # ── Lifecycle (G1-D09) ────────────────────────────────────────────────

    def transition(self, target: LifecycleState) -> "ListingIdentity":
        """
        Devuelve la MISMA identidad con un nuevo lifecycle state.

        ACTIVE→EXPIRED y EXPIRED→DELISTED NO crean identidad nueva (G1-D09):
        el listing_id, venue, instrument_type y terms permanecen idénticos.

        Fail-Fast: transición no autorizada lanza
        InvalidLifecycleTransitionError (nunca se infiere).
        """
        assert_valid_transition(self.lifecycle, target)
        return ListingIdentity(
            listing_id=self.listing_id,
            venue=self.venue,
            instrument_type=self.instrument_type,
            terms=self.terms,
            lifecycle=target,
            definition_version=self.definition_version,
        )

    # ── Definitions (G1-D12) ──────────────────────────────────────────────

    def new_definition_version(self, terms: ContractTerms, provenance: Provenance) -> "ListingIdentity":
        """
        NUEVA versión de la definición de la MISMA entidad negociable.

        G1-D12: un cambio que continúa describiendo la misma entidad
        negociable incrementa definition_version sobre el mismo
        listing_id. Si el cambio define otra entidad negociable distinta,
        NO usar este método — crear una identidad nueva con relación.

        provenance es obligatorio para rastrear el origen del cambio
        (G1-D11/D12); su ausencia es un error de contrato.

        Nota: provenance se acepta pero no se persiste en la entidad (la
        persistencia del histórico de versiones pertenece a la capa de
        registry/DefinitionVersion en un gate posterior). Se valida aquí
        que source no sea UNKNOWN_CONFLICT: un conflicto no puede generar
        una versión admisible (FAIL-CLOSED G1-D11).
        """
        if provenance.source is ProvenanceSource.UNKNOWN_CONFLICT:
            raise ValueError("A DefinitionVersion cannot originate from UNKNOWN_CONFLICT (G1-D11)")
        return ListingIdentity(
            listing_id=self.listing_id,
            venue=self.venue,
            instrument_type=self.instrument_type,
            terms=terms,
            lifecycle=self.lifecycle,
            definition_version=self.definition_version + 1,
        )


# ===========================================================================
# __all__
# ===========================================================================
__all__ = ["ListingId", "ListingIdentity"]
