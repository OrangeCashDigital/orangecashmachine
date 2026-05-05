# -*- coding: utf-8 -*-
"""
market_data/infrastructure/event_bus/in_memory.py
===================================================

Implementación in-process del EventBusPort.

Responsabilidad
---------------
Pub/sub sincrónico en memoria para uso en desarrollo, tests,
y producción monolítica single-process.

Para producción distribuida / multi-proceso, reemplazar con:
  RedisStreamsEventBus  (market_data.infrastructure.event_bus.redis_streams)
  KafkaEventBus        (market_data.infrastructure.event_bus.kafka)
  ─── mismo contrato EventBusPort, swap transparente para callers ───

Principios
----------
OCP         — mismo contrato que EventBusPort; callers no cambian al hacer swap
DIP         — callers dependen de EventBusPort Protocol, no de esta clase
Thread-safe — RLock en todas las mutaciones de _handlers
Fail-soft   — errores en handlers individuales → log + continuar
KISS        — defaultdict[Type → set[Handler]], O(1) dispatch por tipo

Limitaciones conocidas
----------------------
• In-process only: no cruza fronteras de proceso ni red
• Sincrónico: handlers bloquean publish() — usar threads si hace falta async
• Sin persistencia: eventos no sobreviven reinicios del proceso
• Sin replay: eventos perdidos si el consumer no estaba suscrito al momento
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import DefaultDict, Set, Type

from loguru import logger

from market_data.domain.events.ingestion import DomainEvent
from market_data.ports.event_bus import Handler


class InMemoryEventBus:
    """
    Event bus in-process, thread-safe.

    Uso típico
    ----------
    bus = InMemoryEventBus()                           # o usar singleton event_bus

    bus.subscribe(OHLCVBatchReceived, consumer.handle) # registrar consumer
    bus.publish(OHLCVBatchReceived(...))               # disparar desde adapter

    # Al cerrar:
    bus.unsubscribe(OHLCVBatchReceived, consumer.handle)
    """

    def __init__(self) -> None:
        # dict[EventType → set[Handler]] — no list, para idempotencia O(1)
        self._handlers: DefaultDict[Type[DomainEvent], Set[Handler]] = defaultdict(set)
        self._lock = threading.RLock()   # RLock: permite re-entrar desde el mismo thread

    # ----------------------------------------------------------
    # EventBusPort implementation
    # ----------------------------------------------------------

    def publish(self, event: DomainEvent) -> None:
        """
        Despacha el evento a todos los handlers del tipo exacto.

        Complejidad: O(k) donde k = handlers registrados para ese tipo.
        Snapshot del set antes del loop → safe si un handler se desuscribe durante dispatch.
        Fail-soft: cada handler en try/except independiente.
        """
        event_type = type(event)

        with self._lock:
            handlers = frozenset(self._handlers.get(event_type, set()))  # snapshot inmutable

        if not handlers:
            logger.debug(
                "EventBus.publish: no handlers | type={} event_id={}",
                event_type.__name__,
                event.event_id[:8],
            )
            return

        logger.debug(
            "EventBus.publish: dispatching | type={} handlers={} event_id={}",
            event_type.__name__,
            len(handlers),
            event.event_id[:8],
        )

        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                # Fail-soft: un handler roto no contamina los demás
                logger.error(
                    "EventBus: handler crashed | handler={} event={} event_id={} err={}",
                    getattr(handler, "__qualname__", repr(handler)),
                    event_type.__name__,
                    event.event_id[:8],
                    exc,
                )

    def subscribe(
        self,
        event_type: Type[DomainEvent],
        handler: Handler,
    ) -> None:
        """Registra handler. Idempotente — registrar dos veces ejecuta una."""
        with self._lock:
            self._handlers[event_type].add(handler)
        logger.debug(
            "EventBus.subscribe: {} → {}",
            event_type.__name__,
            getattr(handler, "__qualname__", repr(handler)),
        )

    def unsubscribe(
        self,
        event_type: Type[DomainEvent],
        handler: Handler,
    ) -> None:
        """Elimina handler. Fail-soft: no lanza si no estaba registrado."""
        with self._lock:
            self._handlers[event_type].discard(handler)

    # ----------------------------------------------------------
    # Observability
    # ----------------------------------------------------------

    def stats(self) -> dict[str, int]:
        """
        Retorna {EventTypeName: handler_count} para debugging.

        Omite tipos sin handlers activos.
        """
        with self._lock:
            return {
                t.__name__: len(h)
                for t, h in self._handlers.items()
                if h
            }


# ===========================================================================
# __all__
# ===========================================================================

__all__ = ["InMemoryEventBus"]
