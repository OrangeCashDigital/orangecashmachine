# -*- coding: utf-8 -*-
"""Shim temporal: contratos y servicio de fill_sync reexportados."""

from __future__ import annotations

# Contratos (ports)
from trading.ports.execution.fill_sync import SupportsOnFill  # noqa: F401  # TODO: eliminar shim
from trading.ports.execution.fill_sync import SupportsPositionSync  # noqa: F401  # TODO: eliminar shim

# Servicio (implementation)
from trading.services.execution.fill_sync import build_fill_sync  # noqa: F401  # TODO: eliminar shim

# Domain models
from trading.domain.models.order import Order  # noqa: F401  # TODO: eliminar shim

__all__ = [
    "SupportsOnFill",
    "SupportsPositionSync",
    "build_fill_sync",
    "Order",
]
