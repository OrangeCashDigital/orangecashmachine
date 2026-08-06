# -*- coding: utf-8 -*-
"""
market_data/infrastructure/observability/metrics_adapter.py
============================================================

Adaptadores concretos de métricas Prometheus que implementan los
puertos definidos en market_data.ports.outbound.metrics.

Separación deliberada: los contadores/histogramas Prometheus se
instancian en el módulo metrics.py (singleton proceso). Aquí solo
se envuelven en objetos que satisfacen los protocolos de puerto.

SRP: un adaptador por puerto de métricas.
DIP: application/quality dependen de los puertos, nunca de estos adaptadores.
"""

from __future__ import annotations


class PrometheusQualityMetrics:
    """
    Implementa QualityMetricsPort delegando a los contadores Prometheus.
    Instancia lazy: el import de metrics ocurre solo cuando se construye.
    """

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            PIPELINE_ERRORS,
            QUALITY_GAPS_TOTAL,
        )

        self._quality_gaps_total = QUALITY_GAPS_TOTAL
        self._pipeline_errors = PIPELINE_ERRORS

    @property
    def quality_gaps_total(self) -> object:
        return self._quality_gaps_total

    @property
    def pipeline_errors(self) -> object:
        return self._pipeline_errors


class PrometheusResampleMetrics:
    """
    Implementa ResampleMetricsPort delegando a contadores Prometheus.

    DIP: ResamplePipeline y ResampleUseCase dependen del port, no de esta clase.
    SRP: adapta la API de métodos del port a la API .labels().inc() de Prometheus.
    SafeOps: nunca propaga excepciones al caller — el bloque try/except
             está en el caller (ResamplePipeline._resample_pair).
    """

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            PIPELINE_ERRORS,
            RESAMPLE_DURATION_MS,
            RESAMPLE_ROWS_TOTAL,
        )

        self._resample_rows_total = RESAMPLE_ROWS_TOTAL
        self._resample_duration_ms = RESAMPLE_DURATION_MS
        self._pipeline_errors = PIPELINE_ERRORS

    def resample_rows_inc(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str,
        count: int = 1,
    ) -> None:
        """Incrementa contador de filas resampled escritas."""
        self._resample_rows_total.labels(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            market_type=market_type,
        ).inc(count)

    def resample_duration_observe(
        self,
        exchange: str,
        timeframe: str,
        market_type: str,
        duration_ms: int,
    ) -> None:
        """Observa duración de resampling de un par en ms."""
        self._resample_duration_ms.labels(
            exchange=exchange,
            timeframe=timeframe,
            market_type=market_type,
        ).observe(duration_ms)

    def pipeline_errors_inc(
        self,
        exchange: str,
        error_type: str,
    ) -> None:
        """Incrementa contador de errores de pipeline."""
        self._pipeline_errors.labels(
            exchange=exchange,
            error_type=error_type,
        ).inc()


class PrometheusPipelineMetrics:
    """Implementa PipelineMetricsPort — todos los métodos del contrato."""

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            ACTIVE_PAIRS,
            CANDLE_DELAY_MS,
            FETCH_ABORTS_TOTAL,
            FETCH_CHUNK_DURATION,
            FETCH_CHUNK_ERRORS_TOTAL,
            FETCH_CHUNKS_TOTAL,
            PIPELINE_ERRORS,
        )

        self._active_pairs = ACTIVE_PAIRS
        self._candle_delay_ms = CANDLE_DELAY_MS
        self._fetch_aborts_total = FETCH_ABORTS_TOTAL
        self._fetch_chunk_duration = FETCH_CHUNK_DURATION
        self._fetch_chunks_total = FETCH_CHUNKS_TOTAL
        self._fetch_chunk_errors_total = FETCH_CHUNK_ERRORS_TOTAL
        self._pipeline_errors = PIPELINE_ERRORS

    # ── PipelineMetricsPort: propiedades raw (usadas por ohlcv_fetcher) ─────

    @property
    def fetch_chunk_duration(self) -> object:
        return self._fetch_chunk_duration

    @property
    def fetch_chunks_total(self) -> object:
        return self._fetch_chunks_total

    @property
    def fetch_chunk_errors_total(self) -> object:
        return self._fetch_chunk_errors_total

    @property
    def candle_delay_ms(self) -> object:
        return self._candle_delay_ms

    # ── PipelineMetricsPort: métodos con semántica de dominio ───────────────

    def active_pairs_inc(self, exchange: str) -> None:
        self._active_pairs.labels(exchange=exchange).inc()

    def active_pairs_dec(self, exchange: str) -> None:
        self._active_pairs.labels(exchange=exchange).dec()

    def fetch_aborts_inc(self, exchange: str) -> None:
        self._fetch_aborts_total.labels(exchange=exchange).inc()

    def pipeline_errors_inc(self, exchange: str, error_type: str) -> None:
        self._pipeline_errors.labels(
            exchange=exchange,
            error_type=error_type,
        ).inc()

    def record_error(self, exchange: str, error_type: str) -> None:
        """Alias de pipeline_errors_inc — contrato usado por base.py (ctx.metrics)."""
        self.pipeline_errors_inc(exchange, error_type)

    def circuit_open_set(self, exchange: str, value: float) -> None:
        pass  # gauge opcional — no todos los deployments lo exponen

    def pair_duration_observe(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        seconds: float,
    ) -> None:
        pass  # histogram opcional — pendiente de añadir a metrics.py

    def quality_decisions_inc(self, exchange: str, market_type: str, **kwargs: object) -> None:
        pass  # opcional — implementar cuando se añada el counter a metrics.py


class PrometheusRepairMetrics:
    """Implementa RepairMetricsPort con contadores Prometheus reales."""

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            PIPELINE_ERRORS,
            REPAIR_GAPS_FOUND,
            REPAIR_GAPS_HEALED,
            REPAIR_GAPS_SKIPPED,
            ROWS_INGESTED,
        )

        self._pipeline_errors = PIPELINE_ERRORS
        self._repair_gaps_found = REPAIR_GAPS_FOUND
        self._repair_gaps_healed = REPAIR_GAPS_HEALED
        self._repair_gaps_skipped = REPAIR_GAPS_SKIPPED
        self._rows_ingested = ROWS_INGESTED

    @property
    def pipeline_errors(self) -> object:
        return self._pipeline_errors

    @property
    def repair_gaps_found(self) -> object:
        return self._repair_gaps_found

    @property
    def repair_gaps_healed(self) -> object:
        return self._repair_gaps_healed

    @property
    def repair_gaps_skipped(self) -> object:
        return self._repair_gaps_skipped

    @property
    def rows_ingested(self) -> object:
        return self._rows_ingested


class PrometheusExternalMetrics:
    """Implementa ExternalMetricsPort con contadores Prometheus reales.

    Instancia lazy: el import de metrics ocurre solo cuando se construye
    (evita coste de import prometheus_client en tests/tools).
    SafeOps: los métodos delegan a .labels().inc()/.observe() — si un
    contador fallara, el orquestador no debe romperse (no propagamos errores).
    """

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            EXTERNAL_CYCLE_DURATION_MS,
            EXTERNAL_CYCLES_TOTAL,
            EXTERNAL_ERRORS_TOTAL,
            EXTERNAL_EVENTS_PUBLISHED_TOTAL,
            EXTERNAL_FETCHES_TOTAL,
            EXTERNAL_HEALTH,
        )

        self._cycles_total = EXTERNAL_CYCLES_TOTAL
        self._fetches_total = EXTERNAL_FETCHES_TOTAL
        self._events_published_total = EXTERNAL_EVENTS_PUBLISHED_TOTAL
        self._cycle_duration_ms = EXTERNAL_CYCLE_DURATION_MS
        self._errors_total = EXTERNAL_ERRORS_TOTAL
        self._health = EXTERNAL_HEALTH

    def cycles_total_inc(self, source_id: str, metric: str) -> None:
        self._cycles_total.labels(source_id=source_id, metric=metric).inc()

    def fetches_total_inc(self, source_id: str, metric: str) -> None:
        self._fetches_total.labels(source_id=source_id, metric=metric).inc()

    def events_published_inc(self, source_id: str, metric: str, count: int = 1) -> None:
        self._events_published_total.labels(source_id=source_id, metric=metric).inc(count)

    def cycle_duration_observe(self, source_id: str, metric: str, duration_ms: int) -> None:
        self._cycle_duration_ms.labels(source_id=source_id, metric=metric).observe(duration_ms)

    def errors_total_inc(self, source_id: str, metric: str, error_type: str) -> None:
        self._errors_total.labels(source_id=source_id, metric=metric, error_type=error_type).inc()

    def health_observed(self, source_id: str, ok: bool) -> None:
        self._health.labels(source_id=source_id).set(1.0 if ok else 0.0)
