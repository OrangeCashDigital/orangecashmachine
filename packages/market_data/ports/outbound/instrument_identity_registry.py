# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/instrument_identity_registry.py
============================================================

Puerto OUTBOUND (driven): acceso a registros de identidad canónica de
instrumentos (G1/ADR-0049). Definido como Protocol: los adapters concretos
(Iceberg/Kafka/registry) implementan el contrato en un gate posterior;
esta capa de GATE B NUNCA conoce la implementación (DIP).

Qué expone
----------
InstrumentIdentityRegistryPort — búsqueda de SymbolBindings por
  (native_symbol, context) y acceso a una Listing Identity por su
  ListingId opaco.

Qué NO expone (límites)
-----------------------
· No decide representación física de ListingId (D19 BLOCKED).
· No modela relación económica cross-venue (D15) — el registry solo
  resuelve identidades venue-scoped (D03).
· Los métodos de escritura/gestión (register, record_relation, versionado
  de Definition) pertenecen a la fase de implementación/persistencia que
  extiende este port en el gate posterior — aquí solo la resolución de
  lectura necesaria por el use case actual.

Responsabilidad del use case
----------------------------
La FILTRADO temporal (valid_from ≤ t < valid_to) y el FAIL-CLOSED lo
aplica domain/policies/identity_resolution.resolve_unique sobre el
material devuelto por `find_bindings`. El registry devuelve bindings
(no decide ambigüedad); la política decide.

Principios: BC-04 (ports → solo domain) · DIP · ISP · KISS · D19 · D15
"""

from __future__ import annotations

from typing import List, Protocol, runtime_checkable

from market_data.domain.entities.listing_identity import ListingIdentity
from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.symbol_binding import SymbolBinding


@runtime_checkable
class InstrumentIdentityRegistryPort(Protocol):
    """
    Acceso de lectura a registros de identidad canónica (G1).

    Implementado por: adapters en gate posterior (fuera de GATE B).
    Consumido por   : use case ResolveInstrumentIdentityUseCase.

    Contrato de resolución (delegado a la política del dominio):
        find_bindings(native_symbol, context)
            → bindings que asocian native_symbol en ese contexto,
              SIN filtrar por tiempo. El filtrado temporal y el
              FAIL-CLOSED (0/1/>1) los aplica identity_resolution.
    """

    def find_bindings(self, native_symbol: str, context: str) -> List[SymbolBinding]:
        """Bindings de native_symbol en un contexto (todos los periodos)."""
        ...

    def get_identity(self, listing_id: ListingId) -> ListingIdentity:
        """Listing Identity identificada por listing_id (opaco, D19)."""
        ...


__all__ = ["InstrumentIdentityRegistryPort"]
