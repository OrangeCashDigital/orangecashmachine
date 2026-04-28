from __future__ import annotations

"""
market_data/ports/observability.py
====================================
Puerto de métricas para el dominio market_data.

El dominio declara QUÉ quiere observar (este protocolo).
infra decide CÓMO se exporta: Prometheus, StatsD, Datadog, noop.

Regla: los flujos de orquestación (batch_flow, resample_flow)
inyectan un MetricsPusher; NO importan infra.observability directamente.
"""

from typing import Any, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class MetricsPusherPort(Protocol):
    """Contrato de empuje de métricas al backend de observabilidad.

    Implementación de referencia: infra.observability.server.PrometheusPusher
    Implementación noop (tests / entornos sin Prometheus): infra.observability.noop.NoopPusher
    """

    def push(
        self,
        labels: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Envía las métricas actualmente registradas al gateway.

        Parameters
        ----------
        labels:
            Etiquetas adicionales de contexto (job, environment, etc.).
            None → usa las etiquetas por defecto del pusher.
        """
        ...
