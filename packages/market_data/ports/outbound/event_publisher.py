# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/event_publisher.py
==============================================

Puerto OUTBOUND: contrato que usan los inbound adapters para publicar
domain events hacia el interior de la aplicación.

Responsabilidad
---------------
Definir la interfaz que los adapters REST, WebSocket y Replay usan
para empujar datos normalizados al event bus.

¿Por qué outbound desde el adapter?
-------------------------------------
El adapter REST necesita llamar publish() — eso es una dependencia
HACIA el bus. El bus es la infraestructura que el adapter usa.
Este puerto es lo que el adapter ve: una abstracción de "algo que acepta
mis eventos", sin conocer si es InMemory, Kafka o Redis.

Principios
----------
DIP  — adapters inbound dependen de este protocolo, no del bus concreto
ISP  — solo publish; subscribe vive en EventConsumerPort
OCP  — Kafka, Redis, InMemory implementan este contrato
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.domain.events.ingestion import DomainEvent


@runtime_checkable
class EventPublisherPort(Protocol):
    """
    Contrato de publicación de domain events.

    Implementado por: InMemoryEventBus, KafkaPublisherAdapter (futuro)
    Usado por: adapters/inbound/rest/*, adapters/inbound/websocket/*

    SafeOps
    -------
    publish() DEBE ser fail-soft:
      - errores en handlers individuales → loguear, no propagar
      - el adapter no debe saber si algún consumer falló
    """

    def publish(self, event: DomainEvent) -> None:
        """
        Publica un domain event a todos los handlers suscritos.

        Fail-soft: el adapter llama esto y sigue; no le importa qué pasa después.
        Thread-safe: seguro llamar desde REST y WS threads simultáneamente.
        """
        ...


__all__ = ["EventPublisherPort"]
