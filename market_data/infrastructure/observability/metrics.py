"""
market_data/observability/metrics.py
=====================================

Contadores y métricas Prometheus de dominio para OrangeCashMachine.

Responsabilidad
---------------
Definir y registrar métricas de negocio (pipeline, exchange, storage).
NO gestiona el servidor HTTP ni el push al Pushgateway —
eso es responsabilidad de infra.observability.server.

Métricas disponibles
--------------------
• ocm_pipeline_rows_ingested_total      — filas OHLCV ingestadas
• ocm_pipeline_pair_duration_seconds    — duración por par símbolo/timeframe
• ocm_pipeline_errors_total             — errores por exchange/tipo
• ocm_pipeline_active_pairs             — pares en procesamiento (gauge)
• ocm_pipeline_quality_decisions_total  — decisiones de calidad
• ocm_exchange_latency_ms               — latencia del exchange
• ocm_exchange_clock_drift_ms           — drift de reloj
• ocm_exchange_rate_limit_ms            — rate limit configurado
• ocm_fetch_chunk_duration_seconds      — duración de fetch por chunk
• ocm_fetch_chunks_total                — chunks fetched por status
• ocm_fetch_chunk_errors_total          — chunks con error
• ocm_storage_write_duration_seconds    — duración de escritura Silver
• ocm_storage_partition_size_rows       — tamaño de partición en filas
• ocm_repair_gaps_found/healed/skipped  — gaps detectados/reparados
• ocm_manifest_checksum_failures_total  — checksums inválidos
• ocm_write_lock_*                      — contención de write lock Redis
• ocm_timestamp_drift_corrected_total   — timestamps corregidos al grid
• ocm_timestamp_grid_collisions_total   — colisiones post-floor
• ocm_fetch_aborts_total                — aborts por circuit breaker
• ocm_exchange_circuit_open_total       — rechazos por circuit breaker
• ocm_candle_delay_ms                   — delay real entre cierre y fetch del candle
• ocm_quality_gaps_total                — gaps detectados post-ingesta por severidad
• ocm_silver_gaps_total                 — gaps activos en Silver por serie
• ocm_silver_gap_max_candles            — tamaño del gap más grande por serie
• ocm_silver_series_coverage_ratio      — fracción de velas presentes vs esperadas
• ocm_silver_freshness_seconds          — segundos desde último candle Silver (SLA)
"""

from __future__ import annotations

# Servidor de métricas y push → infra.observability.server

from loguru import logger as _log

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
)


# ==========================================================
# Métricas de pipeline
# ==========================================================

ROWS_INGESTED = Counter(
    "ocm_pipeline_rows_ingested_total",
    "Total de filas OHLCV ingestadas",
    ["exchange", "symbol", "timeframe"],
)

PAIR_DURATION = Histogram(
    "ocm_pipeline_pair_duration_seconds",
    "Duración de procesamiento por par símbolo/timeframe",
    ["exchange", "symbol", "timeframe"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

PIPELINE_ERRORS = Counter(
    "ocm_pipeline_errors_total",
    "Total de errores en el pipeline",
    ["exchange", "error_type"],
)

ACTIVE_PAIRS = Gauge(
    "ocm_pipeline_active_pairs",
    "Pares siendo procesados actualmente",
    ["exchange"],
)

QUALITY_DECISIONS = Counter(
    "ocm_pipeline_quality_decisions_total",
    "Decisiones de calidad por par (clean/flagged/rejected)",
    ["exchange", "market_type", "symbol", "timeframe", "decision"],
)

# ==========================================================
# Métricas de exchange
# ==========================================================

EXCHANGE_LATENCY = Histogram(
    "ocm_exchange_latency_ms",
    "Latencia del exchange en validación (ms)",
    ["exchange"],
    buckets=[50, 100, 200, 500, 1000, 2000, 5000],
)

EXCHANGE_CLOCK_DRIFT = Gauge(
    "ocm_exchange_clock_drift_ms",
    "Drift de reloj del exchange vs local (ms)",
    ["exchange"],
)

EXCHANGE_RATE_LIMIT = Gauge(
    "ocm_exchange_rate_limit_ms",
    "Rate limit configurado del exchange (ms)",
    ["exchange"],
)

# ==========================================================
# Métricas de storage y repair
# ==========================================================

FETCH_CHUNK_DURATION = Histogram(
    "ocm_fetch_chunk_duration_seconds",
    "Duración de fetch de un chunk del exchange",
    ["exchange", "symbol", "timeframe"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)

FETCH_CHUNKS_TOTAL = Counter(
    "ocm_fetch_chunks_total",
    "Total de chunks fetched del exchange",
    ["exchange", "timeframe", "status", "mode"],
    # symbol eliminado — evita cardinalidad explosiva (20 symbols × 3 exchanges × 10 TF)
    # status: success | empty | circuit_open | stale | regression
    # mode: backfill | incremental — separa distribuciones para calibración
    # Para debug por symbol usar logs (ya instrumentados en _download_chunked)
)

FETCH_CHUNK_ERRORS_TOTAL = Counter(
    "ocm_fetch_chunk_errors_total",
    "Chunks que terminaron en error (excepción no recuperada)",
    ["exchange", "symbol", "timeframe", "error_type"],
    # error_type: ChunkFetchError | ExchangeCircuitOpenError | Exception
)

STORAGE_WRITE_DURATION = Histogram(
    "ocm_storage_write_duration_seconds",
    "Duración de escritura de partición en Silver",
    ["exchange", "symbol", "timeframe"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
)

STORAGE_PARTITION_SIZE_ROWS = Histogram(
    "ocm_storage_partition_size_rows",
    "Tamaño de partición en filas al escribir",
    ["exchange", "symbol", "timeframe"],
    buckets=[100, 500, 1000, 5000, 10000, 50000, 100000],
)

REPAIR_GAPS_FOUND = Counter(
    "ocm_repair_gaps_found_total",
    "Total de gaps detectados por repair",
    ["exchange", "symbol", "timeframe"],
)

REPAIR_GAPS_HEALED = Counter(
    "ocm_repair_gaps_healed_total",
    "Total de gaps reparados exitosamente",
    ["exchange", "symbol", "timeframe"],
)

REPAIR_GAPS_SKIPPED = Counter(
    "ocm_repair_gaps_skipped_total",
    "Gaps skipeados por guardrail (demasiado grandes)",
    ["exchange", "symbol", "timeframe"],
)

MANIFEST_CHECKSUM_FAILURES = Counter(
    "ocm_manifest_checksum_failures_total",
    "Particiones con checksum inválido detectadas en lectura",
    ["exchange", "symbol", "timeframe"],
)

WRITE_LOCK_WAIT_DURATION = Histogram(
    "ocm_write_lock_wait_seconds",
    "Tiempo esperando el write lock Redis por dataset",
    ["exchange", "symbol", "timeframe"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
)

WRITE_LOCK_CONFLICTS = Counter(
    "ocm_write_lock_conflicts_total",
    "Intentos de adquirir lock ya tomado por otro writer",
    ["exchange", "symbol", "timeframe"],
)

WRITE_LOCK_STARVATION = Counter(
    "ocm_write_lock_starvation_total",
    "Escrituras que agotaron _LOCK_MAX_WAIT y procedieron sin lock",
    ["exchange", "symbol", "timeframe"],
)


TIMESTAMP_DRIFT_CORRECTED = Counter(
    "ocm_timestamp_drift_corrected_total",
    "Velas con timestamp desalineado corregidas al grid del timeframe",
    ["exchange", "symbol", "timeframe"],
)

TIMESTAMP_GRID_COLLISIONS = Counter(
    "ocm_timestamp_grid_collisions_total",
    "Colisiones post-floor: múltiples velas colapsaron al mismo bucket temporal",
    ["exchange", "symbol", "timeframe"],
)

FETCH_ABORTS_TOTAL = Counter(
    "ocm_fetch_aborts_total",
    "Veces que el circuit breaker aborto el pipeline por exchange",
    ["exchange"],
)


# Lateness real observado por candle (ms entre close esperado y momento de fetch).
# Fuente de verdad para calibrar _ALLOWED_LATENESS_MS_BY_EXCHANGE con p99.
CANDLE_DELAY_MS = Histogram(
    "ocm_candle_delay_ms",
    "Delay entre cierre esperado del candle y momento de fetch (ms)",
    ["exchange", "timeframe", "mode"],
    # Sin symbol — cardinalidad controlada: exchange × TF × mode = ~60 series
    # mode: backfill | incremental — crítico para p99 útil:
    # backfill tiene delays de días, incremental de segundos.
    # Mezclarlos produce un p99 inútil para calibración de lateness.
    # Con buckets: ~60 × 9 = ~540 series Prometheus. Manejable.
    buckets=[1000, 5000, 10000, 30000, 60000, 300000, 600000, 900000],
)

# Gaps detectados por QualityPipeline post-ingesta, por severidad.
QUALITY_GAPS_TOTAL = Counter(
    "ocm_quality_gaps_total",
    "Gaps temporales detectados post-ingesta por QualityPipeline",
    ["exchange", "symbol", "timeframe", "severity"],
)

RESAMPLE_ROWS_TOTAL = Counter(
    "ocm_resample_rows_total",
    "Filas OHLCV producidas por ResamplePipeline",
    ["exchange", "symbol", "timeframe", "market_type"],
)
RESAMPLE_DURATION_MS = Histogram(
    "ocm_resample_duration_ms",
    "Duracion de resampling por par (ms)",
    ["exchange", "timeframe", "market_type"],
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500],
)
EXCHANGE_CIRCUIT_OPEN = Counter(
    "ocm_exchange_circuit_open_total",
    "Veces que el circuit breaker rechazó una llamada al exchange",
    ["exchange", "operation"],
)

# ==========================================================
# Helpers de observabilidad
# ==========================================================


def record_pipeline_pair_metrics(
    exchange:    str,
    symbol:      str,
    timeframe:   str,
    market_type: str,
    rows:        int,
    duration_ms: int,
    error_type:  str = "",
    quality_decision: str = "",
) -> None:
    """
    Registra métricas Prometheus de un par procesado por el pipeline.

    Desacopla historical_pipeline.py de los objetos Prometheus directos.
    SafeOps: nunca lanza excepción al caller.
    """
    try:
        if error_type:
            PIPELINE_ERRORS.labels(
                exchange=exchange, error_type=error_type
            ).inc()
            return
        if quality_decision:
            QUALITY_DECISIONS.labels(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe,
                decision=quality_decision,
            ).inc()
        if rows:
            ROWS_INGESTED.labels(
                exchange=exchange, symbol=symbol, timeframe=timeframe
            ).inc(rows)
        if duration_ms:
            PAIR_DURATION.labels(
                exchange=exchange, symbol=symbol, timeframe=timeframe
            ).observe(duration_ms / 1000)
    except Exception as exc:
        _log.warning(
            "record_pipeline_pair_metrics failed | exchange={} symbol={} error={}",
            exchange, symbol, exc,
        )


def record_exchange_probe_metrics(probe) -> None:
    """
    Registra métricas Prometheus de un ExchangeProbe.

    Desacopla exchange_tasks.py de los objetos Prometheus directos —
    las tasks llaman esta función en lugar de conocer EXCHANGE_LATENCY,
    EXCHANGE_CLOCK_DRIFT y EXCHANGE_RATE_LIMIT individualmente.

    SafeOps: nunca lanza excepción al caller.
    """
    try:
        if probe.latency_ms is not None:
            EXCHANGE_LATENCY.labels(exchange=probe.exchange).observe(probe.latency_ms)
        if probe.clock_drift_ms is not None:
            EXCHANGE_CLOCK_DRIFT.labels(exchange=probe.exchange).set(probe.clock_drift_ms)
        if probe.rate_limit_ms is not None:
            EXCHANGE_RATE_LIMIT.labels(exchange=probe.exchange).set(probe.rate_limit_ms)
    except Exception as exc:
        _log.warning("record_exchange_probe_metrics failed | exchange={} error={}", probe.exchange, exc)


