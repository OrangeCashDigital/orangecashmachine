from __future__ import annotations

"""
core/config/runtime.py — SHIM DE COMPATIBILIDAD
================================================

Este módulo re-exporta desde core/runtime/run_config.py.
La ubicación canónica es core/runtime/run_config.py.

Mantenido para compatibilidad hacia atrás — los imports existentes
``from platform.config.runtime import RunConfig`` siguen funcionando.

Migración: actualizar imports a ``from core.runtime import RunConfig``
o ``from platform.runtime.run_config import RunConfig``.
"""

from platform.runtime.run_config import RunConfig  # noqa: F401

__all__ = ["RunConfig"]
