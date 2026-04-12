from __future__ import annotations

"""
market_data/streaming/consumer.py
===================================

PrefectTriggerHandler — handler que dispara un Prefect flow run
a partir de un EventPayload + StreamingContext opcional.

Responsabilidad
---------------
Recibir un EventPayload normalizado y disparar el flow de ingesta
vía Prefect API. NO contiene lógica de negocio — solo orquestación.

Contrato
--------
  handler = PrefectTriggerHandler()
  handler = PrefectTriggerHandler(context=streaming_ctx)
  ok: bool = handler.handle(event)

Cambios Fase 2
--------------
- Acepta StreamingContext opcional en constructor (DI).
- Si se provee, usa ctx.deployment y ctx.run_id en el dispatch.
- Si no, usa defaults seguros (comportamiento Fase 1 preservado).
- Backward compatible: tests de Fase 1 pasan sin modificación.

Principios: SRP · DI · SafeOps (nunca lanza al caller)
"""

from typing import Optional, Protocol, runtime_checkable

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

    Parámetros
    ----------
    deployment_name : str
        Nombre del deployment Prefect. Fallback si no hay StreamingContext.
    context : StreamingContext | None
        Contexto ligero de streaming. Si se provee, sus valores tienen
        prioridad sobre deployment_name. Inyectado por DI — el handler
        no lo construye internamente.

    Fases
    -----
    Fase 1/2 (stub): _dispatch() loguea y retorna True.
    Fase 3: reemplazar _dispatch() con llamada real a Prefect API.
    El caller (EventRouter) no necesita cambios entre fases.
    """

    def __init__(
        self,
        deployment_name: str = "market_data_ingestion/default",
        context: Optional["StreamingContext"] = None,  # type: ignore[name-defined]  # noqa: F821
    ) -> None:
        # Importación local — evita circular en módulos que importan
        # consumer antes que context esté disponible en el paquete.
        if context is not None:
            from market_data.streaming.context import StreamingContext
            if not isinstance(context, StreamingContext):
                raise TypeError(
                    f"context debe ser StreamingContext, recibió {type(context)}"
                )
        self._deployment = (
            context.deployment if context is not None else deployment_name
        )
        self._context    = context
        self._log = logger.bind(
            handler     = "PrefectTriggerHandler",
            deployment  = self._deployment,
        )

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
                event_id = event.event_id,
                exchange = event.exchange,
                symbol   = event.symbol,
                error    = str(exc),
            ).error("PrefectTriggerHandler.handle failed")
            return False

    def _dispatch(self, event: EventPayload) -> bool:
        """
        Lógica de dispatch.

        Fase 1/2 (stub): loguea y retorna True.
        Fase 3: reemplazar con:

            from prefect.deployments import run_deployment
            run_deployment(
                name=self._deployment,
                parameters={
                    "event":   event.to_dict(),
                    "context": self._context.to_dict() if self._context else {},
                },
            )
        """
        run_id = self._context.run_id if self._context is not None else "no-context"
        self._log.bind(
            event_id  = event.event_id,
            exchange  = event.exchange,
            symbol    = event.symbol,
            timeframe = event.timeframe,
            bars      = len(event.bars),
            run_id    = run_id,
        ).info("prefect_trigger_dispatched [stub]")
        # TODO Fase 3: llamada real a Prefect API
        return True
