# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/symbol_binding.py
====================================================

SymbolBinding — asociación contextual-temporal entre una Listing Identity
y una representación nativa (G1-D10, ADR-0049).

D10: una Listing Identity puede asociarse a múltiples SymbolBindings. Cada
binding representa una asociación contextual y temporal entre la identidad
y un native_symbol usado por un venue/adapter/contexto de integración
determinado. Resolución para (contexto + tiempo + native_symbol):
  · 0 matches → FAIL-CLOSED
  · 1 match   → RESOLVE
  · >1 match  → FAIL-CLOSED (nunca se elige silenciosamente)

Semántica temporal
------------------
valid_from ≤ t < valid_to  (inclusive inferior, exclusivo superior).
Un binding abierto (valid_to is None) vale hasta +∞.

Un cambio de símbolo NO modifica la Listing Identity: se registra un nuevo
SymbolBinding (D10) — la identidad continúa siendo la misma (misma
ListingIdentity referenciada). SymbolBinding NO crea equivalencia
cross-venue (D03/D15).

Principios
----------
DDD   — VO puro: frozen dataclass, definido por valor
SSOT  — única definición del binding en el dominio
FAIL-CLOSED — resolución ambigua/ausente nunca inventa identidad
D3    — venue-scoped: native_symbol + context no implican otro venue
D19   — sin schema físico ni encoding (corresponde a gates posteriores)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.provenance import Provenance

# ===========================================================================
# SymbolBinding
# ===========================================================================


@dataclass(frozen=True)
class SymbolBinding:
    """
    Binding contextual-temporal de un native_symbol a una Listing Identity.

    Fields
    ------
    native_symbol : representación nativa (p. ej. "BTCUSDT") tal como la usa
                    el contexto/adapter. NO es identidad (G1-D01).
    context       : contexto de integración (venue/adapter), p. ej. "bybit".
    listing_id    : Listing Identity referenciada (G1-D02/D10).
    valid_from    : inicio de validez (inclusive).
    valid_to      : fin de validez (exclusivo); None = abierto (+∞).
    provenance    : origen del binding (G1-D11) — obligatorio; un cambio de
                    símbolo debe ser trazable.
    """

    native_symbol: str
    context: str
    listing_id: ListingId
    valid_from: datetime
    valid_to: Optional[datetime]
    provenance: Provenance

    def covers(self, t: datetime) -> bool:
        """
        ¿El binding está activo en el instante t?

        Rango semi-abierto D10: valid_from ≤ t < valid_to.
        Un binding abierto (valid_to is None) cubre hasta +∞.
        """
        if t < self.valid_from:
            return False
        return self.valid_to is None or t < self.valid_to


__all__ = ["SymbolBinding"]
