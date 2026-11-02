# -*- coding: utf-8 -*-
"""
market_data/ports/inbound/pipeline_factory.py
==============================================

Puerto de fábrica de pipelines — DIP · SRP.

PipelineOrchestrator (application) depende de este protocolo.
ConcretePipelineFactory (factories/) lo implementa.
Así application nunca toca adapters ni infrastructure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from market_data.ports.inbound.pipeline_trigger import PipelineTriggerPort


@runtime_checkable
class PipelineFactoryPort(Protocol):
    """Construye pipelines concretos a partir de una solicitud."""

    def build(self, request: object) -> "PipelineTriggerPort":
        """
        Recibe un PipelineRequest (o duck-type equivalente) y retorna
        un PipelineTriggerPort listo para ejecutarse.

        La implementación concreta (ConcretePipelineFactory) vive en
        market_data.factories y tiene licencia para importar adapters +
        infrastructure + application.
        """
        ...

    async def release(self) -> None:
        """Libera los recursos de proceso creados por `build()`.

        El orquestador la invoca desde un `finally`, incluso cuando la
        construcción falla después de haber instanciado un adaptador.

        Contrato
        --------
        - Idempotente: llamarla dos veces seguidas no tiene efecto adicional.
        - SafeOps: no propaga excepciones. Un fallo de cierre no puede
          enmascarar el error original del pipeline.
        """
        ...
