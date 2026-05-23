# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/gap_event_publisher.py
==================================================

Puerto de publicación de eventos del ciclo de vida de gaps.

Responsabilidad
---------------
Declarar el contrato que cualquier sink de eventos de gap debe cumplir.
RepairStrategy depende de este puerto — no de Kafka, no de Redis.

Por qué existe este puerto (DIP)
---------------------------------
Sin este puerto, RepairStrategy importaría KafkaGapPublisher directamente:
  application → infrastructure   ← viola DIP y BC boundaries

Con este puerto:
  application → port (abstracción)
  infrastructure/adapter → port (implementación)

El flujo de dependencias es correcto:
  RepairStrategy ──▶ GapEventPublisherPort ◀── KafkaGapPublisher

Implementaciones previstas
--------------------------
  KafkaGapPublisher   : adapter/outbound/kafka_gap_publisher.py
                        publica en topic market.gaps (aiokafka)
  NoopGapPublisher    : tests — descarta silenciosamente
  InMemoryGapPublisher: tests de integración — acumula en lista

SafeOps
-------
Las implementaciones DEBEN ser fail-soft:
  - fallos de broker no deben propagar excepción a RepairStrategy
  - loguear el error y continuar (el gap sigue marcado en gap_registry)

Principios
----------
DIP    — application depende de abstracción, nunca de infra concreta
OCP    — nuevos sinks (SNS, PubSub) no modifican este contrato
KISS   — interfaz mínima: un solo método publish_gap_event
runtime_checkable — permite isinstance() en composition root
"""

from __future__ import annotations

from typing import Protocol, Union, runtime_checkable

from market_data.domain.events.gap_events import (
    GapDetectedEvent,
    GapFailedEvent,
    GapHealedEvent,
)

# Tipo union para el método genérico — evita overloads innecesarios (KISS)
GapEvent = Union[GapDetectedEvent, GapHealedEvent, GapFailedEvent]


@runtime_checkable
class GapEventPublisherPort(Protocol):
    """
    Contrato mínimo para publicar eventos del control plane de gaps.

    Un publisher puede ser síncrono o asíncrono. Para mantener el
    contrato simple (KISS), se define como async — el adapter Kafka
    requiere await, y un NoopPublisher puede implementarlo trivialmente
    con `async def publish_gap_event(...): pass`.
    """

    async def publish_gap_event(self, event: GapEvent) -> None:
        """
        Publica un evento de gap al sink configurado.

        Contrato semántico
        ------------------
        - Idempotente: publicar el mismo event_id dos veces es seguro.
        - Fail-soft: nunca propaga excepciones a RepairStrategy.
          Logear internamente y retornar normalmente ante cualquier fallo.
        - Fire-and-forget: no garantiza entrega; el caller no espera ack.

        Parámetros
        ----------
        event : GapDetectedEvent | GapHealedEvent | GapFailedEvent
        """
        ...


__all__ = [
    "GapEvent",
    "GapEventPublisherPort",
]
