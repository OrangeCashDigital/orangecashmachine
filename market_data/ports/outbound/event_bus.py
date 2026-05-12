# -*- coding: utf-8 -*-
"""
market_data/ports/event_bus.py
================================

Puerto del event bus — contrato DIP entre ingestión e infraestructura.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de event bus debe cumplir.
Los adapters de ingestión y los consumers dependen de este protocolo,
nunca de Redis, Kafka, ni InMemoryEventBus directamente.

Principios
----------
DIP    — ingestión y pipeline dependen de abstracción, no de infraestructura
OCP    — InMemoryEventBus, RedisStreams, Kafka → sin tocar este contrato
ISP    — interfaz mínima: publish · subscribe · unsubscribe

Implementaciones de referencia
-------------------------------
market_data.infrastructure.event_bus.in_memory.InMemoryEventBus
market_data.infrastructure.event_bus.redis_streams.RedisStreamsEventBus  (futuro)
market_data.infrastructure.event_bus.kafka.KafkaEventBus                 (futuro)
"""
from __future__ import annotations

from typing import Callable, Protocol, Type, runtime_checkable

from market_data.domain.events.ingestion import DomainEvent


# ---------------------------------------------------------------------------
# Type alias — callable que acepta un DomainEvent
# ---------------------------------------------------------------------------
Handler = Callable[[DomainEvent], None]


# ===========================================================================
# EventBusPort — Protocol (structural subtyping via runtime_checkable)
# ===========================================================================

@runtime_checkable
class EventBusPort(Protocol):
    """
    Contrato mínimo para un event bus de domain events.

    SafeOps
    -------
    publish() DEBE ser fail-soft:
      - errores en handlers individuales → loguear, no propagar
      - un handler defectuoso no bloquea a los demás

    Thread-safety
    -------------
    Implementaciones deben ser seguras para uso concurrente.
    publish() puede llamarse desde múltiples threads (REST + WS simultáneos).
    """

    def publish(self, event: DomainEvent) -> None:
        """
        Despacha el evento a todos los handlers suscritos para su tipo.

        Fail-soft: errores en handlers individuales se loguean, no propagan.
        """
        ...

    def subscribe(
        self,
        event_type: Type[DomainEvent],
        handler: Handler,
    ) -> None:
        """
        Registra un handler para un tipo de evento.

        Idempotente: registrar el mismo handler dos veces → ejecuta una sola vez.
        """
        ...

    def unsubscribe(
        self,
        event_type: Type[DomainEvent],
        handler: Handler,
    ) -> None:
        """Elimina un handler. No lanza error si no estaba registrado (fail-soft)."""
        ...


# ===========================================================================
# __all__
# ===========================================================================

__all__ = [
    "Handler",
    "EventBusPort",
]
