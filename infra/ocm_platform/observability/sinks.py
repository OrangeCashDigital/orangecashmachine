from __future__ import annotations

"""
ocm_platform/observability/sinks.py
=====================================

Sinks remotos para el sistema de logging de OrangeCashMachine.

Sinks implementados
-------------------
LokiSink
    Envía logs estructurados (JSON) al endpoint push de Loki via HTTP.
    Estrategia: best-effort con reintentos no-bloqueantes en hilo daemon.
    Fail-soft: nunca propaga excepciones al caller.

    Diseño de reintentos
    ~~~~~~~~~~~~~~~~~~~~~
    ``time.sleep()`` en un sink síncrono bloquea el hilo que emite el log.
    En contexto asyncio ese hilo es el event loop — bloqueo catastrófico.
    Solución: el primer intento es síncrono (ruta feliz, <5ms típico).
    Los reintentos se delegan a un hilo daemon via ``threading.Timer``,
    que usa ``threading.Event.wait()`` como backoff interrumpible.
    El pipeline nunca espera a Loki.

PrometheusLogSink
    Incrementa contadores Prometheus por nivel de log.
    Permite alertas en Grafana sobre tasas de ERROR/CRITICAL.
    Fail-soft: la excepción se descarta silenciosamente.
"""

import json
import sys
import threading
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

    Loki acepta ``/loki/api/v1/push`` con ``Content-Type: application/json``.
    Este sink usa ese formato para evitar dependencias de protobuf o snappy.

    Reintentos no-bloqueantes
    -------------------------
    El primer intento HTTP es síncrono. Si falla, los reintentos restantes
    se ejecutan en un ``threading.Timer`` daemon con backoff exponencial
    usando ``threading.Event.wait()`` (interrumpible, no bloquea el caller).
    El pipeline nunca espera a Loki.

    Args:
        url:         Endpoint push de Loki.
        labels:      Labels estáticos (env, run_id, service, etc.).
        timeout:     Timeout HTTP en segundos (default 5.0).
        max_retries: Intentos totales incluyendo el primero (default 3).
    """

    # Backoff base en segundos. Intento N usa: _BACKOFF_BASE ** (N-1).
    # Con max_retries=3: 1s, 2s — total máximo 3s en hilos daemon.
    _BACKOFF_BASE: float = 2.0

    def __init__(
        self,
        url: str,
        labels: Optional[dict[str, str]] = None,
        *,
        timeout: float = 5.0,
        max_retries: int = 3,
    ) -> None:
        self._url         = url
        self._labels      = labels or {}
        self._timeout     = timeout
        self._max_retries = max(1, max_retries)
        self._client      = httpx.Client(timeout=timeout)
        # Evento de shutdown: permite interrumpir waits pendientes en close()
        self._shutdown    = threading.Event()

    def __call__(self, message: Any) -> None:
        """Recibe un mensaje de Loguru y lo envía a Loki (no-bloqueante)."""
        record: dict[str, Any] = message.record
        extra:  dict[str, Any] = record.get("extra", {})

        level  = record["level"].name.lower()
        ts_ns  = str(int(record["time"].timestamp() * 1e9))

        labels = {
            "level":   level,
            "service": extra.get("service", "orangecashmachine"),
            "env":     extra.get("env", "unknown"),
            "run_id":  extra.get("run_id", "-"),
            **self._labels,
        }
        payload = {
            "streams": [{
                "stream": labels,
                "values": [[ts_ns, record["message"]]],
            }]
        }
        # Primer intento síncrono — ruta feliz sin overhead de hilos
        success = self._attempt(payload)
        if not success and self._max_retries > 1:
            self._schedule_retry(payload, attempt=1)

    def _attempt(self, payload: dict[str, Any]) -> bool:
        """Ejecuta un único intento HTTP. Retorna True si exitoso."""
        try:
            resp = self._client.post(
                self._url,
                content=json.dumps(payload),
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code < 300:
                return True
            print(
                f"[LokiSink] HTTP {resp.status_code}: {resp.text[:200]}",
                file=sys.stderr,
            )
        except Exception as exc:
            print(f"[LokiSink] error: {exc}", file=sys.stderr)
        return False

    def _schedule_retry(self, payload: dict[str, Any], attempt: int) -> None:
        """Programa el siguiente reintento en un hilo daemon no-bloqueante."""
        if self._shutdown.is_set() or attempt >= self._max_retries:
            return

        delay = self._BACKOFF_BASE ** (attempt - 1)  # 1s, 2s, 4s…

        def _retry() -> None:
            # wait() con timeout es interrumpible por shutdown.set()
            self._shutdown.wait(timeout=delay)
            if self._shutdown.is_set():
                return
            success = self._attempt(payload)
            if not success:
                self._schedule_retry(payload, attempt + 1)

        t = threading.Timer(0, _retry)
        t.daemon = True
        t.start()

    def close(self) -> None:
        """Señaliza shutdown e interrumpe reintentos pendientes. Cierra el cliente HTTP."""
        self._shutdown.set()
        self._client.close()


# ── PrometheusLogSink ─────────────────────────────────────────────────────────

class PrometheusLogSink:
    """Sink Loguru → Prometheus counter por nivel de log.

    Incrementa ``ocm_log_events_total{level, service}`` en cada log.
    Permite alertas en Grafana: e.g. tasa de ERROR > umbral → alerta.
    Fail-soft: excepciones se descartan silenciosamente.

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
