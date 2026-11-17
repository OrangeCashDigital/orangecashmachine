# -*- coding: utf-8 -*-
from typing import Protocol, runtime_checkable
from trading.domain.models.order import Order

@runtime_checkable
class EventBusPort(Protocol):
    """Contrato para la publicación de eventos de dominio."""
    def publish(self, topic: str, order: Order) -> None: ...
