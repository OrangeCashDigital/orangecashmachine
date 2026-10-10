# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/lifecycle.py
===============================================

LifecycleState — estados de ciclo de vida de una Listing Identity.

G1-D09 (ADR-0049): la Listing Identity permanece estable durante su ciclo
de vida. La evolución de la entidad se representa con estados de lifecycle
y relaciones explícitas; un cambio de estado NO crea una nueva identidad.

Estados (G1-D09)
----------------
ANNOUNCED, ACTIVE, SUSPENDED, EXPIRED, DELISTED.

Transiciones autorizadas (declaradas; ninguna se infiere):
    ANNOUNCED → ACTIVE
    ACTIVE    → SUSPENDED, EXPIRED
    SUSPENDED → ACTIVE
    EXPIRED   → DELISTED

ACTIVE → EXPIRED y EXPIRED → DELISTED NO crean identidad nueva (G1-D06).
Toda otra transición queda fuera de la tabla y lanza
InvalidLifecycleTransitionError (Fail-Fast, no heurística).

Principios
----------
DDD   — VO puro: Enum inmutable, definido por valor
SSOT  — única definición del lifecycle en el dominio
FAIL-CLOSED — transición no declarada → excepción; nunca se infiere
KISS  — tabla de transiciones como mapping estático
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, FrozenSet

from market_data.domain.exceptions import InvalidLifecycleTransitionError

# ===========================================================================
# LifecycleState
# ===========================================================================


class LifecycleState(str, Enum):
    """Estado de ciclo de vida de una Listing Identity (G1-D09)."""

    ANNOUNCED = "announced"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    EXPIRED = "expired"
    DELISTED = "delisted"


# ===========================================================================
# Tabla de transiciones autorizadas (G1-D09)
# ===========================================================================
_ALLOWED_TRANSITIONS: Dict[LifecycleState, FrozenSet[LifecycleState]] = {
    LifecycleState.ANNOUNCED: frozenset({LifecycleState.ACTIVE}),
    LifecycleState.ACTIVE: frozenset({LifecycleState.SUSPENDED, LifecycleState.EXPIRED}),
    LifecycleState.SUSPENDED: frozenset({LifecycleState.ACTIVE}),
    LifecycleState.EXPIRED: frozenset({LifecycleState.DELISTED}),
    LifecycleState.DELISTED: frozenset(),
}


def is_valid_transition(current: LifecycleState, target: LifecycleState) -> bool:
    """
    ¿La transición current → target está autorizada por G1-D09?

    Fail-CLOSED: solo las transiciones declaradas son válidas.
    """
    return target in _ALLOWED_TRANSITIONS.get(current, frozenset())


def assert_valid_transition(current: LifecycleState, target: LifecycleState) -> None:
    """
    Valida la transición; lanza InvalidLifecycleTransitionError si no existe.

    Fail-Fast: nunca se infiere una transición no declarada (G1-D09).
    """
    if not is_valid_transition(current, target):
        raise InvalidLifecycleTransitionError(
            f"Transition {current.value} → {target.value} no está autorizada en G1-D09"
        )


__all__ = ["LifecycleState", "is_valid_transition", "assert_valid_transition"]
