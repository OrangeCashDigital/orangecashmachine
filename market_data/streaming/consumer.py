# -*- coding: utf-8 -*-
from __future__ import annotations

"""
market_data/streaming/consumer.py
===================================

DispatchHandler — handler que despacha un EventPayload al orquestador activo.

Responsabilidad
---------------
Recibir un EventPayload normalizado y disparar la ingesta en el
orquestador activo (Dagster desde v0.3). NO contiene lógica de negocio.

Historial
---------
v0.1–0.2 : stub nombrado PrefectTriggerHandler (orquestador: Prefect).
v0.3+    : renombrado a DispatchHandler. Prefect eliminado como dependencia.
           PrefectTriggerHandler eliminado en v0.4 — usar DispatchHandler.

Contrato
--------
  handler = DispatchHandler()
  handler = DispatchHandler(context=streaming_ctx)
  ok: bool = handler.handle(event)

Fases de implementación
-----------------------
  Fase 1/2 (actual): _dispatch() loguea y retorna True (stub seguro).
  Fase 3: reemplazar _dispatch() con llamada real a Dagster run API:

      from dagster import DagsterRunStatus
      # via dagster-graphql o REST API:
      # POST /graphql → launchRun mutation con asset selection

El caller (EventRouter) no cambia entre fases — SRP garantizado.

Principios: SRP · DI · SafeOps (nunca lanza al caller) · OCP
"""

from typing import Optional, Protocol, runtime_checkable

from loguru import logger

from market_data.streaming.payloads import EventPayload


# ---------------------------------------------------------------------------
# Contrato público — EventHandler protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class EventHandler(Protocol):
    """Contrato mínimo que cualquier handler de eventos debe cumplir.

    Cualquier clase con un método handle(EventPayload) -> bool
    satisface este protocolo (structural subtyping).
    No hay herencia requerida — DIP puro.
    """

    def handle(self, event: EventPayload) -> bool:
        """Procesa el evento. Retorna True si fue aceptado, False si falló."""
        ...


# ---------------------------------------------------------------------------
# DispatchHandler — implementación activa
# ---------------------------------------------------------------------------

class DispatchHandler:
    """
    Handler que despacha un EventPayload al orquestador activo.

    Parámetros
    ----------
    run_name : str
        Nombre del job/run Dagster a disparar. Usado en Fase 3.
        En el stub actual solo aparece en los logs.
    context : StreamingContext | None
        Contexto ligero de streaming. Inyectado por DI — el handler
        no construye contexto internamente (DIP).
        Si se provee, sus valores tienen prioridad sobre run_name.

    SafeOps
    -------
    handle() captura cualquier excepción en _dispatch() y retorna False.
    El caller (EventRouter) nunca recibe una excepción de este handler.
    """

    _DEFAULT_RUN_NAME: str = "ocm_bronze_only_job"

    def __init__(
        self,
        run_name: str = _DEFAULT_RUN_NAME,
        context: Optional["StreamingContext"] = None,  # type: ignore[name-defined]  # noqa: F821
    ) -> None:
        if context is not None:
            from market_data.streaming.context import StreamingContext
            if not isinstance(context, StreamingContext):
                raise TypeError(
                    f"context debe ser StreamingContext, recibió {type(context)}"
                )
        self._run_name = (
            context.deployment if context is not None else run_name
        )
        self._context = context
        self._log = logger.bind(
            handler  = "DispatchHandler",
            run_name = self._run_name,
        )

    def handle(self, event: EventPayload) -> bool:
        """
        Despacha el evento. SafeOps: nunca lanza al caller.

        Returns
        -------
        bool
            True  — dispatch emitido (o simulado en stub)
            False — fallo no recuperable
        """
        try:
            return self._dispatch(event)
        except Exception as exc:
            self._log.bind(
                event_id = event.event_id,
                exchange = event.exchange,
                symbol   = event.symbol,
                error    = str(exc),
            ).error("DispatchHandler.handle failed")
            return False

    def _dispatch(self, event: EventPayload) -> bool:
        """
        Lógica de dispatch — stub de Fase 1/2.

        Fase 3: reemplazar con llamada real a Dagster API.
        Ejemplo usando dagster-graphql (launchRun mutation):

            import httpx
            resp = httpx.post(
                f"{dagster_url}/graphql",
                json={
                    "query": LAUNCH_RUN_MUTATION,
                    "variables": {
                        "jobName": self._run_name,
                        "runConfigData": {"ops": {}},
                        "tags": [
                            {"key": "exchange", "value": event.exchange},
                            {"key": "symbol",   "value": event.symbol},
                        ],
                    },
                },
                timeout=5.0,
            )
            resp.raise_for_status()

        El caller no cambia — solo esta función interna.
        """
        run_id = self._context.run_id if self._context is not None else "no-context"
        self._log.bind(
            event_id  = event.event_id,
            exchange  = event.exchange,
            symbol    = event.symbol,
            timeframe = event.timeframe,
            bars      = len(event.bars),
            run_id    = run_id,
        ).info("dispatch_triggered [stub]")
        # TODO Fase 3: llamada real a Dagster run API
        return True


