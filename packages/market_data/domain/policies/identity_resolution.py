# -*- coding: utf-8 -*-
"""
market_data/domain/policies/identity_resolution.py
=====================================================

Política de dominio: resolución determinista y FAIL-CLOSED de una
Listing Identity a partir de SymbolBindings (G1-D10/D11/D13, ADR-0049).

Modelo (G1-D10)
---------------
    resolve(native_symbol, context, time)
        candidates = bindings(native_symbol, context)
        active     = bindings cuyo periodo satisface: valid_from <= time < valid_to

        · 0 matches → UnknownIdentityError        (FAIL-CLOSED)
        · 1 match   → devolver la Listing Identity
        · >1 match  → AmbiguousResolutionError    (FAIL-CLOSED)

NUNCA se selecciona un candidato silenciosamente. La resolución es
determinista para cualquier (contexto + tiempo + native_symbol);
la ambigüedad relevante → FAIL-CLOSED (D13 regla 7).

Esta policy es pura y sin I/O: recibe bindings ya obtenidos del registry
y devuelve el único válido o eleva error. La orquestación del registry
(qué bindings obtener) vive en el use case de aplicación.

Principios
----------
SRP   — solo decide resolución; no consulta, no persiste
DIP   — no importa ports/ ni infraestructura
FAIL-CLOSED — ausencia o ambigüedad elevan error; nunca heurística
KISS  — función pura sin estado
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List

from market_data.domain.exceptions import (
    AmbiguousResolutionError,
    UnknownIdentityError,
)
from market_data.domain.value_objects.symbol_binding import SymbolBinding

# ===========================================================================
# Resolución (G1-D10)
# ===========================================================================


def select_active_bindings(
    bindings: Iterable[SymbolBinding],
    at: datetime,
) -> List[SymbolBinding]:
    """
    Bindings activos en `at` para una resolución concreta.

    Periodo semi-abierto (G1-D10): valid_from ≤ at < valid_to.
    Filtro puro; no decide nada sobre ambigüedad — es el material de
    entrada para `resolve_unique`.
    """
    return [b for b in bindings if b.covers(at)]


def resolve_unique(bindings: Iterable[SymbolBinding], at: datetime) -> SymbolBinding:
    """
    Resolución determinista y FAIL-CLOSED sobre bindings preseleccionados.

    · 0 activos → UnknownIdentityError (FAIL-CLOSED, G1-D10)
    · 1 activo  → devuelve el binding (RESOLVE)
    · >1 activo → AmbiguousResolutionError (FAIL-CLOSED, G1-D10/D13)

    Nunca selecciona un candidato: la ambigüedad es un fallo, no un
    desempate por heurística.
    """
    active = select_active_bindings(bindings, at)
    if not active:
        raise UnknownIdentityError(f"Unknown identity at {at.isoformat()} — 0 active bindings (FAIL-CLOSED, G1-D10)")
    if len(active) > 1:
        raise AmbiguousResolutionError(
            f"Ambiguous identity at {at.isoformat()} — {len(active)} active bindings "
            "for the same (native_symbol, context, time); ambiguity is FAIL-CLOSED (G1-D10/D13)"
        )
    return active[0]


__all__ = ["select_active_bindings", "resolve_unique"]
