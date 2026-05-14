# -*- coding: utf-8 -*-
"""
market_data/infrastructure/event_bus/
=======================================

Event bus infrastructure — implementaciones concretas del EventBusPort.

Exports
-------
InMemoryEventBus : implementación in-process, thread-safe (desarrollo + tests)
event_bus        : singleton global para uso en single-process production

Swap a producción distribuida
-----------------------------
Sustituir event_bus por RedisStreamsEventBus o KafkaEventBus
sin cambiar ningún caller — solo el wiring en bootstrap/.

Principios: OCP · DIP · SSOT (singleton declarado aquí, no en callers)
"""
from market_data.infrastructure.event_bus.in_memory import InMemoryEventBus  # noqa: F401

#: Singleton global — único event bus del proceso.
#: Los adapters y consumers lo inyectan via bootstrap/; nunca lo importan directo.
#: En producción distribuida: reemplazar por RedisStreamsEventBus en bootstrap/.
event_bus: InMemoryEventBus = InMemoryEventBus()

__all__ = [
    "InMemoryEventBus",
    "event_bus",
]
