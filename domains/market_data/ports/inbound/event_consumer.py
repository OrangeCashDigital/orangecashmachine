# -*- coding: utf-8 -*-
"""
market_data/ports/inbound/event_consumer.py
=============================================

Puerto INBOUND: contrato que expone la aplicación para ser conducida
por eventos externos (inbound adapters → application).

Responsabilidad
---------------
Definir la interfaz que los use cases usan para suscribirse al stream
de domain events. La aplicación no conoce qué bus hay detrás.

¿Por qué inbound?
-----------------
En hexagonal, el lado IZQUIERDO (inbound/primary) es lo que CONDUCE
la aplicación. Los adapters REST y WS publican eventos que CONDUCEN
los pipelines del application layer. Este puerto es lo que la aplicación
expone para recibir esa conducción.

Principios
----------
DIP  — use cases dependen de este protocolo, nunca de InMemoryEventBus
ISP  — solo subscribe/unsubscribe; publish vive en su propio puerto
OCP  — Kafka, Redis Streams, SQS implementan este contrato sin tocar callers
"""
from __future__ import annotations

from typing import Callable, Protocol, Type, runtime_checkable

from market_data.domain.events.ingestion import DomainEvent


Handler = Callable[[DomainEvent], None]


@runtime_checkable
class EventConsumerPort(Protocol):
    """
    Contrato de suscripción a domain events.

    Implementado por: InMemoryEventBus, KafkaConsumerAdapter (futuro)
    Usado por: application/pipelines/* (use cases que consumen eventos)

    SafeOps
    -------
    Implementaciones no deben lanzar en subscribe/unsubscribe.
    Si el handler falla, el bus lo captura (fail-soft per-handler).
    """

    def subscribe(
        self,
        event_type: Type[DomainEvent],
        handler:    Handler,
    ) -> None:
        """
        Registra un handler para un tipo de evento.

        Idempotente: registrar el mismo handler dos veces → ejecuta una vez.
        Thread-safe: seguro llamar desde múltiples threads.
        """
        ...

    def unsubscribe(
        self,
        event_type: Type[DomainEvent],
        handler:    Handler,
    ) -> None:
        """
        Elimina un handler.

        Fail-soft: no lanza si el handler no estaba registrado.
        """
        ...


__all__ = [
    "Handler",
    "EventConsumerPort",
]
