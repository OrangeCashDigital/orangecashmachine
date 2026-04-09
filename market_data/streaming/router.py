from __future__ import annotations

"""
market_data/streaming/router.py
================================

EventRouter — fan-out de eventos a handlers registrados.

Responsabilidad
---------------
Recibir un EventPayload (o dict serializado), normalizarlo,
y despacharlo a todos los handlers registrados en orden.

Desacoplamiento
---------------
El router NO importa PrefectTriggerHandler directamente.
Los handlers se inyectan en el constructor — el caller decide
qué handlers activar. Esto permite:
  - tests sin Prefect
  - múltiples handlers (Prefect + alertas + ML) sin cambiar el router
  - sustitución de handlers en Fase 3 sin tocar este módulo

Contrato
--------
  router = EventRouter(handlers=[PrefectTriggerHandler()])
  ok: bool = router.route(event)

Principios: SRP · DI · OCP · SafeOps
"""

from typing import List

from loguru import logger

from market_data.streaming.payloads import EventPayload
from market_data.streaming.consumer import EventHandler


class EventRouter:
    """
    Enruta eventos a una lista de handlers registrados.

    Parámetros
    ----------
    handlers : list[EventHandler]
        Lista de handlers que implementan el protocolo EventHandler.
        El router los llama en orden. Si un handler falla (retorna False
        o lanza), el router loguea y continúa — no aborta el fan-out.

    Uso
    ---
        from market_data.streaming.router import EventRouter
        from market_data.streaming.consumer import PrefectTriggerHandler

        router = EventRouter(handlers=[PrefectTriggerHandler()])
        ok = router.route(event_payload)
    """

    def __init__(self, handlers: List[EventHandler]) -> None:
        if not handlers:
            raise ValueError("EventRouter requiere al menos un handler")
        self._handlers = handlers
        self._log = logger.bind(component="EventRouter", handlers=len(handlers))

    def route(self, event: EventPayload | dict) -> bool:
        """
        Normaliza el evento y lo despacha a todos los handlers.

        Retorna True si al menos un handler lo aceptó (bool OR).
        Retorna False solo si TODOS los handlers fallaron.

        SafeOps: nunca lanza al caller.
        """
        try:
            payload = self._normalize(event)
        except Exception as exc:
            self._log.bind(error=str(exc)).error("route: payload normalization failed")
            return False

        any_ok = False
        for handler in self._handlers:
            try:
                result = handler.handle(payload)
                if result:
                    any_ok = True
                else:
                    self._log.bind(
                        event_id=payload.event_id,
                        handler=type(handler).__name__,
                    ).warning("handler returned False")
            except Exception as exc:
                self._log.bind(
                    event_id=payload.event_id,
                    handler=type(handler).__name__,
                    error=str(exc),
                ).error("handler raised — continuing fan-out")

        return any_ok

    # --------------------------------------------------
    # Internals
    # --------------------------------------------------

    @staticmethod
    def _normalize(event: EventPayload | dict) -> EventPayload:
        """Convierte dict a EventPayload si es necesario."""
        if isinstance(event, EventPayload):
            return event
        if isinstance(event, dict):
            return EventPayload.from_dict(event)
        raise TypeError(f"route() esperaba EventPayload o dict, recibió {type(event)}")
