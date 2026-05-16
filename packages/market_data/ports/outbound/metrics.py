"""
market_data/ports/outbound/metrics.py
======================================

Puerto OUTBOUND: contrato de telemetría de pipeline.

Desacopla application/domain de Prometheus concreto.
Cualquier capa que emita métricas recibe MetricsPort por DI.

Implementaciones:
  infrastructure.observability.prometheus_metrics.PrometheusMetrics
  ports.outbound.metrics.NullMetrics  (tests / modo degradado)

Principios: DIP · ISP · Protocol (structural subtyping).
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsPort(Protocol):
    """
    Contrato de observabilidad para pipelines de market data.

    Fire-and-forget: ningún método lanza excepciones.
    Las implementaciones capturan internamente errores de emisión.
    """

    def rows_ingested_inc(
        self,
        exchange:  str,
        timeframe: str,
        delta:     int = 1,
    ) -> None:
        """Incrementa contador de candles almacenados con éxito."""
        ...

    def pipeline_errors_inc(
        self,
        exchange:   str,
        error_type: str,
    ) -> None:
        """Incrementa contador de errores de pipeline."""
        ...

    def active_pairs_set(
        self,
        exchange: str,
        count:    int,
    ) -> None:
        """Actualiza gauge de pares activos en un exchange."""
        ...

    def fetch_aborts_inc(self, exchange: str) -> None:
        """Incrementa contador de fetches abortados por throttle/circuit."""
        ...

    def circuit_open_set(self, exchange: str, value: float) -> None:
        """Gauge de circuit breaker: 0.0 cerrado / 1.0 abierto."""
        ...


class NullMetrics:
    """
    Implementación vacía de MetricsPort.

    Uso: tests, entornos sin Prometheus, pipelines degradados.
    Satisface el Protocol sin herencia explícita (duck typing).
    """

    def rows_ingested_inc(self, exchange: str, timeframe: str, delta: int = 1) -> None:
        pass

    def pipeline_errors_inc(self, exchange: str, error_type: str) -> None:
        pass

    def active_pairs_set(self, exchange: str, count: int) -> None:
        pass

    def fetch_aborts_inc(self, exchange: str) -> None:
        pass

    def circuit_open_set(self, exchange: str, value: float) -> None:
        pass


__all__ = ["MetricsPort", "NullMetrics"]
