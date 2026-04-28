from __future__ import annotations

"""
core/observability/sinks.py
=====================

Sinks remotos para el sistema de logging de OrangeCashMachine.

Sinks implementados
-------------------
LokiSink
    Envía logs estructurados (JSON) al endpoint push de Loki via HTTP.
    Estrategia: best-effort con reintentos exponenciales (3 intentos).
    Fail-soft: nunca propaga excepciones al caller — un fallo de Loki
    no debe interrumpir el pipeline.

PrometheusLogSink
    Incrementa contadores Prometheus por nivel de log.
    Permite alertas en Grafana sobre tasas de ERROR/CRITICAL.
    Usa el registro global de prometheus_client (compatible con
    el MetricsRuntime existente).

Integración con Loguru
----------------------
Ambos sinks son callables que aceptan el ``message`` de Loguru
(objeto con atributos .record) y se registran con ``logger.add()``.

Ejemplo::

    from ocm_platform.observability.sinks import LokiSink, PrometheusLogSink

    loki = LokiSink(url="http://loki:3100/loki/api/v1/push",
                    labels={"env": "production"})
    logger.add(loki, level="INFO", serialize=True)

    prom = PrometheusLogSink()
    logger.add(prom, level="DEBUG")
"""

import json
import time
import sys
from typing import Any, Optional

import httpx
from prometheus_client import Counter

# ── Prometheus counter ────────────────────────────────────────────────────────

_LOG_COUNTER: Counter = Counter(
    "ocm_log_events_total",
    "Total log events emitted by OrangeCashMachine",
    labelnames=["level", "service"],
)


# ── LokiSink ─────────────────────────────────────────────────────────────────

class LokiSink:
    """Sink Loguru → Loki via HTTP push (Protobuf-free, JSON snappy-free).

    Loki acepta el endpoint ``/loki/api/v1/push`` con Content-Type
    ``application/json``. Este sink usa ese formato para evitar
    dependencias de protobuf o snappy.

    Reintentos
    ----------
    Hasta ``max_retries`` intentos con backoff exponencial base 2s.
    Si todos fallan, el error se imprime a stderr y se descarta.

    Args:
        url:        Endpoint push de Loki.
        labels:     Labels estáticos adicionales (env, run_id, etc.).
        timeout:    Timeout HTTP en segundos (default 5).
        max_retries: Número máximo de reintentos (default 3).
    """

    def __init__(
        self,
        url: str,
        labels: Optional[dict[str, str]] = None,
        *,
        timeout: float = 5.0,
        max_retries: int = 3,
    ) -> None:
        self._url        = url
        self._labels     = labels or {}
        self._timeout    = timeout
        self._max_retries = max_retries
        self._client     = httpx.Client(timeout=timeout)

    def __call__(self, message: Any) -> None:
        """Recibe un mensaje de Loguru y lo envía a Loki."""
        record: dict[str, Any] = message.record

        # Extraer campos del record de Loguru
        level    = record["level"].name.lower()
        ts_ns    = str(int(record["time"].timestamp() * 1e9))
        extra    = record.get("extra", {})

        # Construir labels: base + extra relevante + estáticos del config
        labels = {
            "level":   level,
            "service": extra.get("service", "orangecashmachine"),
            "env":     extra.get("env", "unknown"),
            "run_id":  extra.get("run_id", "-"),
            **self._labels,
        }

        # Payload formato Loki JSON push
        payload = {
            "streams": [
                {
                    "stream": labels,
                    "values": [[ts_ns, record["message"]]],
                }
            ]
        }

        self._push(payload)

    def _push(self, payload: dict[str, Any]) -> None:
        """Envía el payload a Loki con reintentos exponenciales."""
        body = json.dumps(payload)
        for attempt in range(self._max_retries):
            try:
                resp = self._client.post(
                    self._url,
                    content=body,
                    headers={"Content-Type": "application/json"},
                )
                if resp.status_code < 300:
                    return
                # Loki retornó error HTTP — reintentamos
                print(
                    f"[LokiSink] HTTP {resp.status_code}: {resp.text[:200]}",
                    file=sys.stderr,
                )
            except Exception as exc:
                print(f"[LokiSink] attempt={attempt+1} error={exc}", file=sys.stderr)

            if attempt < self._max_retries - 1:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s

    def close(self) -> None:
        """Cierra el cliente HTTP. Llamado al remover el sink."""
        self._client.close()


# ── PrometheusLogSink ─────────────────────────────────────────────────────────

class PrometheusLogSink:
    """Sink Loguru → Prometheus counter por nivel de log.

    Incrementa ``ocm_log_events_total{level, service}`` en cada log.
    Permite alertas en Grafana: e.g. tasa de ERROR > umbral → alerta.

    Args:
        service: Valor del label ``service`` (default "orangecashmachine").
    """

    def __init__(self, service: str = "orangecashmachine") -> None:
        self._service = service

    def __call__(self, message: Any) -> None:
        """Incrementa el counter para el nivel del mensaje."""
        try:
            level = message.record["level"].name.lower()
            _LOG_COUNTER.labels(level=level, service=self._service).inc()
        except Exception:
            pass  # Prometheus nunca interrumpe el pipeline
