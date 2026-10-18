# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/resolve_instrument_identity.py
==================================================================

ResolveInstrumentIdentityUseCase — resolución determinista y FAIL-CLOSED
de una Listing Identity (G1/ADR-0049, D10/D11).

Responsabilidad
---------------
Dado un (native_symbol, context, time), devolver la ÚNICA Listing Identity
válida:

    · 0 candidatos activos  → UnknownIdentityError      (FAIL-CLOSED)
    · 1 candidato activo    → devolver la identidad     (RESOLVE)
    · >1 candidatos activos → AmbiguousResolutionError  (FAIL-CLOSED)

Nunca selecciona silenciosamente (G1-D10): la ambigüedad y la ausencia
son fallos de diseño, no desempates por heurística.

Cómo funciona
-------------
1. Consulta al port InstrumentIdentityRegistryPort.find_bindings para
   obtener los SymbolBindings de (native_symbol, context) — sin filtrar
   por tiempo (DIP: el port no decide resolución).
2. domain/policies/identity_resolution aplica el filtrado temporal
   (valid_from ≤ t < valid_to) y el FAIL-CLOSED.
3. get_identity(listing_id) materializa la identidad.

Alcance
-------
GATE B: dominio + ports + aplicación. Sin persistencia, sin Kafka/Iceberg,
sin wiring de adapters — la implementación concreta del registry es un
gate posterior (DIP conservado).

Fronteras
---------
· D15/D19: identidad venue-scoped opaca (D03), sin relación económica ni
  representación física decidida.
· La resolución NO crea agrupación económica ni equivalencia cross-venue.

Principios: SRP · DIP · FAIL-CLOSED · KISS · Clean Architecture (BC-05/BC-08)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_data.domain.entities.listing_identity import ListingIdentity
from market_data.domain.policies.identity_resolution import resolve_unique
from market_data.ports.outbound.instrument_identity_registry import (
    InstrumentIdentityRegistryPort,
)


@dataclass(frozen=True)
class ResolveIdentityRequest:
    """
    DTO de entrada: qué resolución se quiere.

    native_symbol : representación nativa (D10), p. ej. "BTCUSDT".
    context       : contexto de integración (venue/adapter), p. ej. "bybit".
    at            : instante en el que se resuelve (valid_from ≤ at < valid_to).
    """

    native_symbol: str
    context: str
    at: datetime


class ResolveInstrumentIdentityUseCase:
    """
    Caso de uso puro de resolución de identidad (G1-D10).

    Recibe el port (Protocol) por constructor — nunca instancia la
    implementación (DIP). Sin estado interno: cada ejecución es
    independiente y determinista para la misma entrada.
    """

    def __init__(self, registry: InstrumentIdentityRegistryPort) -> None:
        self._registry = registry

    def execute(self, request: ResolveIdentityRequest) -> ListingIdentity:
        """
        Resuelve la Listing Identity válida o eleva FAIL-CLOSED.

        Raises
        ------
        UnknownIdentityError     — 0 bindings activos (D10).
        AmbiguousResolutionError — >1 bindings activos (D10/D13).
        """
        bindings = self._registry.find_bindings(request.native_symbol, request.context)
        binding = resolve_unique(bindings, request.at)
        return self._registry.get_identity(binding.listing_id)


__all__ = ["ResolveIdentityRequest", "ResolveInstrumentIdentityUseCase"]
