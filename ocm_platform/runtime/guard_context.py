"""
market_data/safety/guard_context.py
=====================================

Contexto de proceso para ExecutionGuard.

Problema que resuelve
---------------------
Prefect no puede serializar ExecutionGuard como parámetro de @flow
(contiene threading.Lock y threading.Event). El guard debe vivir en
el proceso, no en la firma de Prefect.

Solución
--------
Un módulo con estado de proceso: el guard se registra antes de
asyncio.run() y se consume desde cualquier coroutine en el mismo
proceso sin pasar por la firma de Prefect.

Garantías
---------
- Un solo guard activo por proceso en cualquier momento.
- get_guard() retorna None si no hay guard registrado (SafeOps).
- No es un singleton global permanente: se resetea en cada llamada
  a set_guard(None) desde el finally de entrypoint.run().

Uso
---
    # entrypoint.py — antes de asyncio.run()
    guard_context.set_guard(guard)

    # batch_flow.py — dentro del flow, sin parámetros Prefect
    guard = guard_context.get_guard()
    if guard:
        guard.record_error("pipeline_failures")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ocm_platform.runtime.guard import ExecutionGuard

__all__ = ["set_guard", "get_guard"]

_current_guard: Optional["ExecutionGuard"] = None


def set_guard(guard: Optional["ExecutionGuard"]) -> None:
    """
    Registra el guard activo para este proceso.

    Llamar con None en el finally de entrypoint.run() para limpiar.
    Thread-safe en CPython por el GIL (asignación de referencia atómica).
    """
    global _current_guard
    _current_guard = guard


def get_guard() -> Optional["ExecutionGuard"]:
    """
    Retorna el guard activo, o None si no hay ninguno registrado.

    SafeOps: nunca lanza. Los callers deben verificar None antes de usar.
    """
    return _current_guard
