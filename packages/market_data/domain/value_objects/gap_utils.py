# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/gap_utils.py
================================================

SHIM DE COMPATIBILIDAD — deprecado.

Este módulo re-exporta desde gap_scanner.py para no romper callers
durante la transición. Eliminar en el siguiente commit de limpieza
una vez actualizados todos los imports.

Acción: grep -rn "gap_utils" y migrar a "gap_scanner".
"""

from __future__ import annotations

from market_data.domain.value_objects.gap_scanner import (  # noqa: F401
    GapRange,
    scan_gaps,
)

__all__ = ["GapRange", "scan_gaps"]
