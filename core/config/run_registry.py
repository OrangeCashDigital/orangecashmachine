from __future__ import annotations

"""
core/config/run_registry.py — SHIM DE COMPATIBILIDAD
=====================================================

Este módulo re-exporta desde core/runtime/registry.py.
La ubicación canónica es core/runtime/registry.py.

Mantenido para compatibilidad hacia atrás — los imports existentes
``from core.config.run_registry import record_run`` siguen funcionando.

Migración: actualizar imports a ``from core.runtime import record_run``
o ``from core.runtime.registry import record_run``.
"""

from core.runtime.registry import record_run, query_runs  # noqa: F401

__all__ = ["record_run", "query_runs"]
