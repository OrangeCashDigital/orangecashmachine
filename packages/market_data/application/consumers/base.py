# -*- coding: utf-8 -*-
"""
market_data/application/consumers/base.py
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
from market_data.ports.outbound.event_bus import EventBusPort
from ocm.observability import trace_consumer_event


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

    Tracing (B-17/H-18)
    -------------------
    ``start()`` registra un wrapper que abre un span OTel por evento y propaga
    ``event.event_id`` como request-id (SSOT: ``trace_consumer_event`` en
    ``ocm/observability/tracing.py``). Ningún consumer necesita instrumentar
    su propio ``handle`` — la propagación vive aquí, en el punto único.

    Ejemplo
    -------
    class MyConsumer(BaseConsumer):
        event_type = OHLCVBatchReceived

        def handle(self, event: DomainEvent) -> None:
            ...
    """

    event_type: ClassVar[Type[DomainEvent]]  # subclases deben declarar esto

    def __init__(self, bus: EventBusPort) -> None:
        self._bus = bus

    def start(self) -> None:
        """
        Registra handle() en el bus envuelto en tracing por evento.

        Llamar antes de que los adapters empiecen a publicar eventos.
        Idempotente: registrar dos veces → ejecuta una sola vez (set semántic del bus).

        El wrapper crea un span ``<Consumer>.handle`` con el event_id como
        request-id (B-17/G11). Fail-soft: si el tracing falla, el evento se
        procesa igual (el wrapper nunca propaga errores de instrumentación).
        """
        self._bus.subscribe(self.event_type, self._traced_handle)

    def _traced_handle(self, event: DomainEvent) -> None:
        """Wrapper de ``handle`` con span OTel + request-id propagado (B-17).

        Enriquecimiento por duck-typing: si el evento (o su ``batch``) expone
        exchange/symbol/timeframe, se fijan como atributos del span. No hay
        acoplamiento a la forma concreta de los eventos (ISP/DIP).
        """
        batch = getattr(event, "batch", None)
        span_attrs: dict[str, str] = {}
        for field in ("exchange", "symbol", "timeframe"):
            value = getattr(batch, field, None) if batch is not None else None
            if value is None:
                value = getattr(event, field, None)
            if isinstance(value, str) and value:
                span_attrs[field] = value

        with trace_consumer_event(
            f"{self.__class__.__name__}.handle",
            event,
            **span_attrs,
        ):
            self.handle(event)

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
