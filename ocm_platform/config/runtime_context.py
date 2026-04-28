from __future__ import annotations

"""
core/config/runtime_context.py — SHIM DE COMPATIBILIDAD
========================================================

Este módulo re-exporta desde core/runtime/context.py.
La ubicación canónica es core/runtime/context.py.

Mantenido para compatibilidad hacia atrás — los imports existentes
``from ocm_platform.config.runtime_context import RuntimeContext`` siguen funcionando.

Migración: actualizar imports a ``from core.runtime import RuntimeContext``
o ``from ocm_platform.runtime.context import RuntimeContext``.
"""

from ocm_platform.runtime.context import RuntimeContext  # noqa: F401

__all__ = ["RuntimeContext"]
