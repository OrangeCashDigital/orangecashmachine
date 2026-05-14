# -*- coding: utf-8 -*-
"""
domain/ — STUB DE COMPATIBILIDAD (DEPRECATED)
==============================================

Este módulo fue renombrado a shared/ (refactor DDD, commit 5a2db4c).

ACCIÓN REQUERIDA: actualizar todos los imports:
    from domain.boundaries    → from shared.contracts.boundaries
    from domain.value_objects → from shared.types
    from domain.entities      → from shared.types
    from domain.events        → from shared.types

Este stub será ELIMINADO en el próximo release.
BC-09 en pyproject.toml detecta violaciones automáticamente:
    uv run lint-imports
"""
import warnings as _warnings

_warnings.warn(
    "El paquete 'domain' está deprecado. "
    "Usar 'shared.contracts' y 'shared.types' en su lugar. "
    "Será eliminado en el próximo release.",
    DeprecationWarning,
    stacklevel=2,
)

from shared.contracts.boundaries import (  # noqa: E402, F401
    FeatureSource,
    SignalProtocol,
    FillHandler,
    TradeHistory,
    RiskGate,
)

__all__ = [
    "FeatureSource",
    "SignalProtocol",
    "FillHandler",
    "TradeHistory",
    "RiskGate",
]
