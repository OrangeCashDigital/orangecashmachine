from __future__ import annotations

"""
market_data/streaming/consumer.py
===================================

PrefectTriggerHandler — handler que dispara un Prefect flow run
a partir de un EventPayload.

Responsabilidad
---------------
Recibir un EventPayload normalizado y disparar el flow de ingesta
vía Prefect API. NO contiene lógica de negocio — solo orquestación.

Contrato
--------
  handler = PrefectTriggerHandler()
  ok: bool = handler.handle(event)

Principios: SRP · DI · SafeOps (nunca lanza al caller)
"""

from typing import Protocol, runtime_checkable

from loguru import logger

from market_data.streaming.payloads import EventPayload


# --------------------------------------------------
# Contrato público
# --------------------------------------------------

@runtime_checkable
class EventHandler(Protocol):
    """Contrato mínimo que cualquier handler de eventos debe cumplir."""

    def handle(self, event: EventPayload) -> bool:
        """Procesa el evento. Retorna True si fue aceptado, False si falló."""
        ...


# --------------------------------------------------
# Implementación: PrefectTriggerHandler
# --------------------------------------------------

class PrefectTriggerHandler:
    """
    Handler que dispara un Prefect flow run para el evento dado.

    Fase 1: implementación stub — loguea y retorna True.
    Fase 3: se reemplaza el body de _dispatch() con la llamada real
    a prefect.deployments.run_deployment() o prefect.flow.run().

    El caller (EventRouter) no necesita cambios entre fases.
    """

    def __init__(self, deployment_name: str = "market_data_ingestion/default") -> None:
        self._deployment = deployment_name
        self._log = logger.bind(handler="PrefectTriggerHandler", deployment=deployment_name)

    def handle(self, event: EventPayload) -> bool:
        """
        Dispara el flow. SafeOps: nunca lanza al caller.

        Returns
        -------
        bool
            True  — trigger emitido (o simulado en stub)
            False — fallo no recuperable al emitir el trigger
        """
        try:
            return self._dispatch(event)
        except Exception as exc:
            self._log.bind(
                event_id=event.event_id,
                exchange=event.exchange,
                symbol=event.symbol,
                error=str(exc),
            ).error("PrefectTriggerHandler.handle failed")
            return False

    def _dispatch(self, event: EventPayload) -> bool:
        """
        Lógica de dispatch real.

        Fase 1 (stub): loguea el intento y retorna True.
        Fase 3: reemplazar con:

            from prefect.deployments import run_deployment
            run_deployment(
                name=self._deployment,
                parameters={"event": event.to_dict()},
            )
        """
        self._log.bind(
            event_id=event.event_id,
            exchange=event.exchange,
            symbol=event.symbol,
            timeframe=event.timeframe,
            bars=len(event.bars),
        ).info("prefect_trigger_dispatched [stub]")
        # TODO Fase 3: llamada real a Prefect API
        return True
