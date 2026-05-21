# -*- coding: utf-8 -*-
"""
market_data/infrastructure/observability/pipeline_metrics.py
=============================================================

Adaptador de métricas de pipeline para inyección en PipelineContext.

Responsabilidad
---------------
Implementar el duck-typing "record_error(exchange, error_type)" esperado
por StrategyMixin.execute_pair(), desacoplando domain/policies/base.py
de PIPELINE_ERRORS (Prometheus Counter).

Principios
----------
DIP    — domain depende de duck typing (Any), no de este módulo
SRP    — única responsabilidad: adaptar PIPELINE_ERRORS al protocolo esperado
SafeOps — record_error nunca lanza; fallo de métricas no interrumpe pipeline
SSOT   — PIPELINE_ERRORS sigue siendo SSOT en metrics.py; aquí solo delegamos
"""

from __future__ import annotations

from market_data.infrastructure.observability.metrics import PIPELINE_ERRORS


class PipelineMetricsAdapter:
    """
    Adaptador liviano que implementa el protocolo de métricas de StrategyMixin.

    Protocolo duck-typed esperado por PipelineContext.metrics:
        .record_error(exchange: str, error_type: str) -> None

    Uso:
        ctx = PipelineContext(..., metrics=PipelineMetricsAdapter())
    """

    __slots__ = ()  # sin estado — todas las métricas son globales (Prometheus registry)

    def record_error(self, exchange: str, error_type: str) -> None:
        """
        Incrementa PIPELINE_ERRORS con los labels dados.

        SafeOps: captura toda excepción para no interrumpir el pipeline
        si Prometheus no está disponible (ej: tests sin registry).
        """
        try:
            PIPELINE_ERRORS.labels(
                exchange=exchange,
                error_type=error_type,
            ).inc()
        except Exception:  # noqa: BLE001 — fallo de métricas es no-fatal
            pass


__all__ = ["PipelineMetricsAdapter"]
