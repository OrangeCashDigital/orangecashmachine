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
        self._quality_gaps_total  = QUALITY_GAPS_TOTAL
        self._pipeline_errors     = PIPELINE_ERRORS

    @property
    def quality_gaps_total(self) -> object:
        return self._quality_gaps_total

    @property
    def pipeline_errors(self) -> object:
        return self._pipeline_errors


class PrometheusResampleMetrics:
    """Implementa ResampleMetricsPort."""

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            RESAMPLE_DURATION_MS,
            RESAMPLE_ROWS_TOTAL,
        )
        self._resample_rows_total = RESAMPLE_ROWS_TOTAL
        self._resample_duration_ms = RESAMPLE_DURATION_MS

    @property
    def resample_rows_total(self) -> object:
        return self._resample_rows_total

    @property
    def resample_duration_ms(self) -> object:
        return self._resample_duration_ms


class PrometheusPipelineMetrics:
    """Implementa PipelineMetricsPort."""

    def __init__(self) -> None:
        from market_data.infrastructure.observability.metrics import (
            CANDLE_DELAY_MS,
            FETCH_CHUNK_DURATION,
            FETCH_CHUNK_ERRORS_TOTAL,
            FETCH_CHUNKS_TOTAL,
        )
        self._fetch_chunk_duration    = FETCH_CHUNK_DURATION
        self._fetch_chunks_total      = FETCH_CHUNKS_TOTAL
        self._fetch_chunk_errors_total = FETCH_CHUNK_ERRORS_TOTAL
        self._candle_delay_ms         = CANDLE_DELAY_MS

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
        self._pipeline_errors      = PIPELINE_ERRORS
        self._repair_gaps_found    = REPAIR_GAPS_FOUND
        self._repair_gaps_healed   = REPAIR_GAPS_HEALED
        self._repair_gaps_skipped  = REPAIR_GAPS_SKIPPED
        self._rows_ingested        = ROWS_INGESTED

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

