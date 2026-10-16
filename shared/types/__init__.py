# -*- coding: utf-8 -*-
"""
shared/types/
=============

Value objects, entities y domain events compartidos entre bounded contexts.

Regla de oro: este módulo NO importa desde ningún bounded context del proyecto
(market_data, trading, portfolio, ocm, infrastructure, apps).
Solo stdlib y third-party permitidos.

Contrato: BC-09 en pyproject.toml lo hace cumplir automáticamente.
"""

# SSOT: OrderSide/PositionSide viven en shared.enums (BC-45). Se importan
# desde la fuente directa para evitar reexport implícito (requisito mypy strict).
from shared.enums import OrderSide, PositionSide
from shared.types.order_events import (
    OrderFilled,
    OrderRejected,
)
from shared.types.position_events import (
    PositionClosed,
    PositionOpened,
)
from shared.types.signal import Signal, SignalType
from shared.types.timeframe import Timeframe

__all__ = [
    "Timeframe",
    "Signal",
    "SignalType",
    "OrderSide",
    "OrderFilled",
    "OrderRejected",
    "PositionSide",
    "PositionOpened",
    "PositionClosed",
]
