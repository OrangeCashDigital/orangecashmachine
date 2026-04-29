# -*- coding: utf-8 -*-
"""
domain/events/
==============

Domain events — hechos de negocio que ocurrieron e son inmutables.

Convención
----------
- Nombre en pasado: OrderFilled, PositionOpened, RebalanceTriggered
- Frozen dataclass — nunca se modifican tras construcción
- Sin dependencias de infraestructura — solo stdlib
- Publicados entre BCs vía callbacks, streaming o event bus

Importar desde aquí (SSOT):
    from domain.events import OrderFilled, PositionOpened
"""
from domain.events.order_events import OrderFilled, OrderRejected, OrderCancelled
from domain.events.position_events import PositionOpened, PositionClosed
from domain.events.rebalance_events import RebalanceTriggered, RebalanceCompleted

__all__ = [
    "OrderFilled",
    "OrderRejected",
    "OrderCancelled",
    "PositionOpened",
    "PositionClosed",
    "RebalanceTriggered",
    "RebalanceCompleted",
]
