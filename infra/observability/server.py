"""
infra/observability/server.py
==============================
Infraestructura de observabilidad: servidor HTTP de métricas y push al Pushgateway.

Responsabilidad única: ciclo de vida del servidor de métricas.
No contiene contadores de dominio — esos viven en market_data/observability/metrics.py
"""
from __future__ import annotations

import time as _time
from loguru import logger as _log
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway,
    start_http_server,
    REGISTRY,
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


def start_metrics_server(port: int = 8000) -> None:
    """Levanta el servidor HTTP de métricas en el puerto indicado."""
    start_http_server(port)


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
    • NO se hace delete — los counters deben persistir entre runs.
    • Actualiza PIPELINE_LAST_RUN e incrementa PIPELINE_HEARTBEAT antes del push.

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


def push_silver_quality_metrics(
    results: list,
    gateway: str = "localhost:9091",
    registry: CollectorRegistry = REGISTRY,
) -> None:
    """
    Empuja métricas de calidad Silver al Pushgateway.

    Diseño
    ------
    • job=ocm_validate_silver — separado de los jobs de pipeline.
    • Para cada SeriesResult, setea ocm_silver_gaps_total con el
      número de gaps activos. 0 = limpio, >0 = requiere repair.
    • SafeOps: nunca lanza excepción al caller.

    Parámetros
    ----------
    results : list[SeriesResult]
        Output de validate_silver.main() — lista de SeriesResult.
    """
    from market_data.observability.metrics import SILVER_GAPS_TOTAL
    job = "ocm_validate_silver"
    try:
        for r in results:
            if r.skipped or r.error:
                continue
            SILVER_GAPS_TOTAL.labels(
                exchange=r.exchange,
                symbol=r.symbol,
                market_type=r.market_type,
                timeframe=r.timeframe,
            ).set(len(r.gaps))
        push_to_gateway(gateway, job=job, registry=registry)
        _log.bind(job=job, gateway=gateway).debug("silver_quality_metrics_pushed | series={}", len(results))
    except Exception as exc:
        _log.bind(job=job, gateway=gateway).warning("silver_quality_push_failed | error={}", exc)
