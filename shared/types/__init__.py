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

from shared.types.order_events import (
    OrderFilled,
    OrderRejected,
    OrderSide,
)
from shared.types.position_events import (
    PositionClosed,
    PositionOpened,
    PositionSide,
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
