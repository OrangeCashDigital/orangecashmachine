"""
services/observability/metrics.py
==================================

Métricas Prometheus para OrangeCashMachine.

Métricas disponibles
--------------------
• pipeline_rows_ingested_total      — filas ingestadas por exchange/timeframe
• pipeline_pair_duration_seconds    — duración de procesamiento por par
• pipeline_errors_total             — errores por exchange/tipo
• exchange_latency_ms               — latencia del exchange en validación
• exchange_clock_drift_ms           — drift de reloj por exchange
• pipeline_active_pairs             — pares procesándose actualmente (gauge)
• pipeline_last_run_timestamp       — timestamp Unix del último run exitoso
• pipeline_heartbeat_total          — counter que incrementa cada run (deadman)

Notas de diseño
---------------
• push_metrics(exchange=) usa job distinto por exchange para evitar
  last-write-wins collision en Pushgateway cuando corren en paralelo.
• NO se hace delete después del push — los counters deben persistir
  entre runs para que PipelineStalled pueda calcular time() - max(counter).
• El heartbeat es un counter que solo crece: Prometheus lo ve siempre,
  y si deja de crecer → el pipeline dejó de correr.
"""

from __future__ import annotations

from loguru import logger as _log
import time as _time

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
    REGISTRY,
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

PIPELINE_LAST_RUN = Gauge(
    "ocm_pipeline_last_run_timestamp",
    "Timestamp Unix del último run completado (exitoso o parcial)",
    ["exchange"],
)

PIPELINE_HEARTBEAT = Counter(
    "ocm_pipeline_heartbeat_total",
    "Incrementa en cada run — usado como deadman switch",
    ["exchange"],
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
    ["exchange", "symbol", "timeframe"],
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

EXCHANGE_CIRCUIT_OPEN = Counter(
    "ocm_exchange_circuit_open_total",
    "Veces que el circuit breaker rechazó una llamada al exchange",
    ["exchange", "operation"],
)

# ==========================================================
# Servidor de métricas
# ==========================================================

def start_metrics_server(port: int = 8000) -> None:
    """Levanta el servidor HTTP de métricas en el puerto indicado."""
    start_http_server(port)


# ==========================================================
# Push hacia Pushgateway
# ==========================================================



# ==========================================================
# Helpers de observabilidad
# ==========================================================

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


def push_metrics(
    exchange: str = "local",
    gateway: str = "localhost:9091",
    registry: CollectorRegistry = REGISTRY,
) -> None:
    """
    Empuja métricas al Pushgateway al finalizar el pipeline.

    Diseño
    ------
    • job=ocm_pipeline_{exchange} — un job por exchange evita
      last-write-wins cuando exchanges corren en paralelo.
    • NO se hace delete — los counters deben persistir entre runs
      para que PipelineStalled calcule correctamente el delta temporal.
    • Actualiza PIPELINE_LAST_RUN e incrementa PIPELINE_HEARTBEAT
      antes del push para que el estado final quede registrado.

    SafeOps: nunca lanza excepción al caller.
    """
    job = f"ocm_pipeline_{exchange}"
    try:
        PIPELINE_LAST_RUN.labels(exchange=exchange).set(_time.time())
        PIPELINE_HEARTBEAT.labels(exchange=exchange).inc()
        push_to_gateway(gateway, job=job, registry=registry)
        _log.bind(job=job, gateway=gateway).debug("metrics_pushed")
    except Exception as exc:
        _log.bind(job=job, gateway=gateway).warning("metrics_push_failed | error={}", exc)
