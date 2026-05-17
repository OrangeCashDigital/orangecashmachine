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
from shared.types.timeframe import Timeframe
from shared.types.signal import Signal, SignalType
from shared.types.ohlcv import OHLCVBar
from shared.types.order_events import (
    OrderSide,
    OrderFilled,
    OrderRejected,
    OrderCancelled,
)
from shared.types.position_events import (
    PositionSide,
    PositionOpened,
    PositionClosed,
)
from shared.types.rebalance_events import RebalanceTriggered, RebalanceCompleted

__all__ = [
    "Timeframe",
    "Signal",
    "SignalType",
    "OHLCVBar",
    "OrderSide",
    "OrderFilled",
    "OrderRejected",
    "OrderCancelled",
    "PositionSide",
    "PositionOpened",
    "PositionClosed",
    "RebalanceTriggered",
    "RebalanceCompleted",
]
