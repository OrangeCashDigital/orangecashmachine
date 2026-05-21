# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/metrics.py
======================================

Puerto OUTBOUND: contrato de telemetría de pipeline.

Desacopla application/domain de Prometheus concreto.
Cualquier capa que emita métricas recibe MetricsPort por DI.

Implementaciones:
  infrastructure.observability.prometheus_metrics.PrometheusMetrics
  ports.outbound.metrics.NullMetrics  (tests / modo degradado)

Principios: DIP · ISP · Protocol (structural subtyping) · SafeOps
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

    # ── Ingesta ──────────────────────────────────────────────────────────────

    def rows_ingested_inc(
        self,
        exchange: str,
        timeframe: str,
        delta: int = 1,
    ) -> None:
        """Incrementa contador de candles almacenados con éxito."""
        ...

    def pipeline_errors_inc(
        self,
        exchange: str,
        error_type: str,
    ) -> None:
        """Incrementa contador de errores de pipeline. error_type: transient|fatal"""
        ...

    # ── Concurrencia / pares activos ─────────────────────────────────────────

    def active_pairs_inc(self, exchange: str) -> None:
        """Incrementa gauge de pares activos en un exchange."""
        ...

    def active_pairs_dec(self, exchange: str) -> None:
        """Decrementa gauge de pares activos en un exchange."""
        ...

    def fetch_aborts_inc(self, exchange: str) -> None:
        """Incrementa contador de fetches abortados por throttle/circuit."""
        ...

    # ── Circuit breaker ───────────────────────────────────────────────────────

    def circuit_open_set(self, exchange: str, value: float) -> None:
        """Gauge de circuit breaker: 0.0 cerrado / 1.0 abierto."""
        ...

    # ── Latencia y calidad ────────────────────────────────────────────────────

    def pair_duration_observe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        seconds: float,
    ) -> None:
        """Observa la duración de procesamiento de un par (histogram)."""
        ...

    def quality_decisions_inc(
        self,
        exchange: str,
        market_type: str,
        symbol: str,
        timeframe: str,
        decision: str,
    ) -> None:
        """Incrementa contador de decisiones del quality gate."""
        ...

    # ── Repair ───────────────────────────────────────────────────────────────

    def repair_gaps_found_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        count: int = 1,
    ) -> None:
        """Incrementa contador de gaps detectados en repair."""
        ...

    def repair_gaps_healed_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        """Incrementa contador de gaps reparados exitosamente."""
        ...

    def repair_gaps_skipped_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        """Incrementa contador de gaps omitidos (demasiado grandes / irrecuperables)."""
        ...

    # ── Drift de timestamps ───────────────────────────────────────────────────

    def timestamp_drift_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        count: int = 1,
    ) -> None:
        """Incrementa contador de timestamps corregidos al grid (align_to_grid)."""
        ...

    def timestamp_collisions_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        count: int = 1,
    ) -> None:
        """Incrementa contador de colisiones post-floor (align_to_grid)."""
        ...


class NullMetrics:
    """
    Implementación vacía de MetricsPort.

    Uso: tests, entornos sin Prometheus, pipelines degradados.
    Satisface el Protocol sin herencia explícita (duck typing).
    Todos los métodos son no-op — SafeOps garantizado.
    """

    def rows_ingested_inc(self, exchange: str, timeframe: str, delta: int = 1) -> None:
        pass

    def pipeline_errors_inc(self, exchange: str, error_type: str) -> None:
        pass

    def active_pairs_inc(self, exchange: str) -> None:
        pass

    def active_pairs_dec(self, exchange: str) -> None:
        pass

    def fetch_aborts_inc(self, exchange: str) -> None:
        pass

    def circuit_open_set(self, exchange: str, value: float) -> None:
        pass

    def pair_duration_observe(self, exchange: str, symbol: str, timeframe: str, seconds: float) -> None:
        pass

    def quality_decisions_inc(
        self, exchange: str, market_type: str, symbol: str, timeframe: str, decision: str
    ) -> None:
        pass

    def repair_gaps_found_inc(self, exchange: str, symbol: str, timeframe: str, count: int = 1) -> None:
        pass

    def repair_gaps_healed_inc(self, exchange: str, symbol: str, timeframe: str) -> None:
        pass

    def repair_gaps_skipped_inc(self, exchange: str, symbol: str, timeframe: str) -> None:
        pass

    def timestamp_drift_inc(self, exchange: str, symbol: str, timeframe: str, count: int = 1) -> None:
        pass

    def timestamp_collisions_inc(self, exchange: str, symbol: str, timeframe: str, count: int = 1) -> None:
        pass


__all__ = ["MetricsPort", "NullMetrics"]


class RepairMetricsPort(Protocol):
    """
    Contrato de métricas de repair de gaps.
    Implementación: PrometheusRepairMetrics (infrastructure.observability.metrics_adapter).
    DIP: RepairStrategy depende de este port, no de Prometheus directamente.
    """

    # -- Métodos de escritura (API preferida) ---------------------
    def pipeline_errors_inc(self, exchange: str, error_type: str) -> None: ...

    def repair_gaps_found_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        count: int = 1,
    ) -> None: ...

    def repair_gaps_healed_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None: ...

    def repair_gaps_skipped_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None: ...

    def rows_ingested_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        count: int = 1,
    ) -> None: ...

    # -- Properties legacy (compatibilidad con Prometheus directo) --
    @property
    def pipeline_errors(self) -> object: ...

    @property
    def repair_gaps_found(self) -> object: ...

    @property
    def repair_gaps_healed(self) -> object: ...

    @property
    def repair_gaps_skipped(self) -> object: ...

    @property
    def rows_ingested(self) -> object: ...


class QualityMetricsPort(Protocol):
    """
    Contrato de métricas del pipeline de calidad de datos.

    Implementación de referencia
    ----------------------------
    market_data.infrastructure.observability.metrics_adapter.PrometheusQualityMetrics

    DIP: QualityPipeline inyecta este port — nunca importa Prometheus directamente.
    ISP: interfaz mínima — solo los 2 métodos que QualityPipeline necesita.
    SafeOps: implementaciones nunca propagan excepciones al caller.
    """

    def quality_gaps_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        severity: str,
        count: int = 1,
    ) -> None:
        """Incrementa contador de gaps detectados por severidad."""
        ...

    def pipeline_errors_inc(
        self,
        exchange: str,
        error_type: str,
    ) -> None:
        """Incrementa contador de errores de pipeline. error_type: quality_reject|fatal"""
        ...


class NullQualityMetrics:
    """
    Implementación vacía de QualityMetricsPort.

    Uso: tests, entornos sin Prometheus, pipelines degradados.
    Todos los métodos son no-op — SafeOps garantizado por diseño.
    """

    def quality_gaps_inc(self, exchange: str, symbol: str, timeframe: str, severity: str, count: int = 1) -> None:
        pass

    def pipeline_errors_inc(self, exchange: str, error_type: str) -> None:
        pass
