from __future__ import annotations

"""
core/config/lineage.py — SHIM DE COMPATIBILIDAD
================================================

Este módulo re-exporta desde core/runtime/lineage.py.
La ubicación canónica es core/runtime/lineage.py.

Mantenido para compatibilidad hacia atrás — los imports existentes
``from ocm_platform.config.lineage import get_git_hash`` siguen funcionando.

Migración: actualizar imports a ``from core.runtime import get_git_hash``
o ``from ocm_platform.runtime.lineage import get_git_hash``.
"""

from ocm_platform.runtime.lineage import (  # noqa: F401
    get_git_hash,
    build_lineage,
    LineageRecord,
)

__all__ = ["get_git_hash", "build_lineage", "LineageRecord"]
