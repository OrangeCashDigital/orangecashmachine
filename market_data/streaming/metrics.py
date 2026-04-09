from __future__ import annotations

"""
market_data/streaming/metrics.py
==================================

Métricas Prometheus para el pipeline event-driven streaming.

Patrón: mismo que cursor_store.py — import opcional con fallback
a no-ops si prometheus_client no está instalado. El pipeline
funciona igual sin Prometheus; las métricas son observabilidad
adicional, no lógica de negocio.

Métricas expuestas
------------------
  ocm_stream_events_published_total   — eventos publicados (Counter)
  ocm_stream_events_processed_total   — eventos procesados con ACK (Counter)
  ocm_stream_events_failed_total      — eventos fallados por categoría (Counter)
  ocm_stream_processing_latency_ms    — latencia de procesamiento (Histogram)
  ocm_stream_pending_messages         — mensajes en PEL sin ACK (Gauge)
  ocm_stream_backlog_depth            — profundidad del stream (Gauge)

Labels
------
  stream   — nombre del stream (e.g. "ohlcv")
  exchange — exchange del evento (e.g. "bybit")
  reason   — motivo de fallo (schema_mismatch | deserialize_error | router_rejected)

Uso
---
  from market_data.streaming.metrics import StreamMetrics
  m = StreamMetrics(stream_name="ohlcv")
  m.event_published(exchange="bybit")
  m.event_processed(exchange="bybit", latency_ms=12.3)
  m.event_failed(exchange="bybit", reason="router_rejected")

Principios: SRP · optional dependency · zero overhead sin Prometheus
"""

import time
from typing import Optional

try:
    from prometheus_client import Counter, Gauge, Histogram
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False


# --------------------------------------------------
# No-op fallbacks — misma interfaz, cero overhead
# --------------------------------------------------

class _NoOpCounter:
    def labels(self, **_): return self
    def inc(self, _=1):    return


class _NoOpGauge:
    def labels(self, **_): return self
    def set(self, _):      return
    def inc(self, _=1):    return
    def dec(self, _=1):    return


class _NoOpHistogram:
    def labels(self, **_):      return self
    def observe(self, _):       return


# --------------------------------------------------
# Registro global — una sola instancia por proceso
# --------------------------------------------------

def _make_counter(name: str, doc: str, labels: list) -> object:
    if _PROMETHEUS_AVAILABLE:
        return Counter(name, doc, labels)
    return _NoOpCounter()


def _make_gauge(name: str, doc: str, labels: list) -> object:
    if _PROMETHEUS_AVAILABLE:
        return Gauge(name, doc, labels)
    return _NoOpGauge()


def _make_histogram(name: str, doc: str, labels: list, buckets: list) -> object:
    if _PROMETHEUS_AVAILABLE:
        return Histogram(name, doc, labels, buckets=buckets)
    return _NoOpHistogram()


_LATENCY_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]

_events_published = _make_counter(
    "ocm_stream_events_published_total",
    "Total de eventos publicados al stream",
    ["stream", "exchange"],
)

_events_processed = _make_counter(
    "ocm_stream_events_processed_total",
    "Total de eventos procesados con ACK exitoso",
    ["stream", "exchange"],
)

_events_failed = _make_counter(
    "ocm_stream_events_failed_total",
    "Total de eventos fallados por categoría",
    ["stream", "exchange", "reason"],
)

_processing_latency = _make_histogram(
    "ocm_stream_processing_latency_ms",
    "Latencia de procesamiento end-to-end en ms",
    ["stream", "exchange"],
    _LATENCY_BUCKETS,
)

_pending_messages = _make_gauge(
    "ocm_stream_pending_messages",
    "Mensajes en PEL (Pending Entry List) sin ACK",
    ["stream"],
)

_backlog_depth = _make_gauge(
    "ocm_stream_backlog_depth",
    "Número total de entradas en el stream",
    ["stream"],
)


# --------------------------------------------------
# StreamMetrics — interfaz de alto nivel
# --------------------------------------------------

class StreamMetrics:
    """
    Interfaz de métricas para el pipeline streaming.

    Instanciar una vez por stream_name. Thread-safe (Prometheus lo es).

    Parámetros
    ----------
    stream_name : nombre lógico del stream (e.g. "ohlcv").
    """

    def __init__(self, stream_name: str = "ohlcv") -> None:
        self._stream = stream_name

    # -- Producer --

    def event_published(self, exchange: str = "unknown") -> None:
        """Incrementa el contador de eventos publicados."""
        _events_published.labels(
            stream   = self._stream,
            exchange = exchange,
        ).inc()

    # -- Consumer --

    def event_processed(
        self,
        exchange:   str   = "unknown",
        latency_ms: float = 0.0,
    ) -> None:
        """Registra un evento procesado con ACK y su latencia."""
        _events_processed.labels(
            stream   = self._stream,
            exchange = exchange,
        ).inc()
        _processing_latency.labels(
            stream   = self._stream,
            exchange = exchange,
        ).observe(latency_ms)

    def event_failed(
        self,
        exchange: str = "unknown",
        reason:   str = "unknown",
    ) -> None:
        """
        Registra un evento fallado.

        reason: schema_mismatch | deserialize_error | router_rejected
        """
        _events_failed.labels(
            stream   = self._stream,
            exchange = exchange,
            reason   = reason,
        ).inc()

    # -- Backpressure --

    def set_pending_messages(self, count: int) -> None:
        """Actualiza el gauge de mensajes pendientes en PEL."""
        _pending_messages.labels(stream=self._stream).set(count)

    def set_backlog_depth(self, count: int) -> None:
        """Actualiza la profundidad total del stream."""
        _backlog_depth.labels(stream=self._stream).set(count)


# --------------------------------------------------
# Timer helper — context manager para latencia
# --------------------------------------------------

class timer:
    """
    Context manager para medir latencia en ms.

        with timer() as t:
            process(event)
        latency_ms = t.elapsed_ms
    """

    def __enter__(self) -> "timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000

    @property
    def elapsed_ms(self) -> float:
        return getattr(self, "_elapsed_ms", 0.0)

    @elapsed_ms.setter
    def elapsed_ms(self, value: float) -> None:
        self._elapsed_ms = value
