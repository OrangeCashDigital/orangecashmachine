# -*- coding: utf-8 -*-
"""
market_data/infrastructure/event_bus/
=======================================

Event bus in-process para DomainEvents.

Responsabilidad
---------------
Pub/sub sincrónico entre adapters inbound y application consumers
dentro del mismo proceso. Transporta DomainEvents (OHLCVBatchReceived,
LineageEvent, etc.) — no wire payloads.

Nota de arquitectura
--------------------
Este bus es distinto del pipeline Kafka:
  - EventBus (este módulo) → DomainEvents in-process (mismo proceso)
  - Kafka                  → EventPayload cross-process (source of truth)

Implementaciones
----------------
InMemoryEventBus — in-process, thread-safe. Único backend necesario
                   para uso single-process (desarrollo, tests, producción).

Principios: SRP · DIP · OCP · thread-safe · fail-soft
"""
from market_data.infrastructure.event_bus.in_memory import InMemoryEventBus  # noqa: F401

#: Singleton de proceso — compartido por adapters y consumers del mismo proceso.
event_bus: InMemoryEventBus = InMemoryEventBus()

__all__ = [
    "InMemoryEventBus",
    "event_bus",
]
