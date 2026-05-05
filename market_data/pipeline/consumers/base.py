# -*- coding: utf-8 -*-
"""
market_data/pipeline/consumers/base.py
========================================

BaseConsumer — clase base para todos los consumers del event bus.

Responsabilidad
---------------
Encapsular el ciclo de vida del consumer (start/stop) y el contrato
que cada subclase debe cumplir (handle).

Principios
----------
OCP   — subclases añaden comportamiento, base nunca cambia
DIP   — depende de EventBusPort (Protocol), no de InMemoryEventBus
SRP   — cada consumer subclase tiene una sola razón de existir
LSP   — start/stop/handle tienen contratos estables, subclases no los violan
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Type

from market_data.domain.events.ingestion import DomainEvent
from market_data.ports.event_bus import EventBusPort


class BaseConsumer(ABC):
    """
    Clase base para consumers del event bus.

    Ciclo de vida
    -------------
    consumer = MyConsumer(bus)
    consumer.start()   # registra handle() en el bus → empieza a recibir eventos
    ...
    consumer.stop()    # deregistra handle() del bus → shutdown limpio

    Contrato para subclases
    -----------------------
    1. Declarar `event_type: ClassVar[Type[DomainEvent]]`
    2. Implementar `handle(event)` — DEBE ser fail-soft

    Ejemplo
    -------
    class MyConsumer(BaseConsumer):
        event_type = OHLCVBatchReceived

        def handle(self, event: DomainEvent) -> None:
            ...
    """

    event_type: ClassVar[Type[DomainEvent]]   # subclases deben declarar esto

    def __init__(self, bus: EventBusPort) -> None:
        self._bus = bus

    def start(self) -> None:
        """
        Registra handle() en el bus.
        Llamar antes de que los adapters empiecen a publicar eventos.
        Idempotente: registrar dos veces → ejecuta una sola vez (set semántic del bus).
        """
        self._bus.subscribe(self.event_type, self.handle)

    def stop(self) -> None:
        """
        Deregistra handle() del bus.
        Llamar en shutdown para evitar procesamiento de eventos huérfanos.
        Fail-soft: no lanza si no estaba registrado.
        """
        self._bus.unsubscribe(self.event_type, self.handle)

    @abstractmethod
    def handle(self, event: DomainEvent) -> None:
        """
        Procesa un evento entrante.

        Contrato
        --------
        • DEBE ser fail-soft: no propagar excepciones al bus
        • Verificar `isinstance(event, self.event_type)` como guard defensivo
        • Loguear errores internos con logger.error(), no raise
        """
        ...


__all__ = ["BaseConsumer"]
