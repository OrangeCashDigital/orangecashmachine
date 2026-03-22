"""
services/observability/metrics.py
==================================

Métricas Prometheus para OrangeCashMachine.

Expone un endpoint HTTP /metrics en el puerto 8000
que Prometheus scrapea periódicamente.

Métricas disponibles
--------------------
• pipeline_rows_ingested_total      — filas ingestadas por exchange/timeframe
• pipeline_pair_duration_seconds    — duración de procesamiento por par
• pipeline_errors_total             — errores por exchange/tipo
• exchange_latency_ms               — latencia del exchange en validación
• exchange_clock_drift_ms           — drift de reloj por exchange
• pipeline_active_pairs             — pares procesándose actualmente (gauge)
"""

from __future__ import annotations

from prometheus_client import (
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
    ["exchange", "symbol", "timeframe", "decision"],
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
# Servidor de métricas
# ==========================================================

def start_metrics_server(port: int = 8000) -> None:
    """
    Levanta el servidor HTTP de métricas en el puerto indicado.
    Prometheus hace scraping a http://host:8000/metrics
    """
    start_http_server(port)


def push_metrics(job: str = "ocm_pipeline", gateway: str = "localhost:9091") -> None:
    """
    Empuja métricas al Pushgateway al finalizar el pipeline.
    Correcto para procesos batch que no corren continuamente.
    SafeOps: nunca lanza excepción al caller.
    """
    try:
        push_to_gateway(gateway, job=job, registry=REGISTRY)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning("Metrics push failed | error=%s", exc)
