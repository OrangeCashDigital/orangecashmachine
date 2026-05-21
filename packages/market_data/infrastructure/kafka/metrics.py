# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/metrics.py
============================================

Métricas Prometheus para el pipeline Kafka OHLCV.

Patrón
------
Import opcional con fallback a no-ops si prometheus_client no está
instalado. El pipeline funciona igual sin Prometheus; las métricas
son observabilidad adicional, no lógica de negocio.

Métricas expuestas
------------------
  ocm_kafka_events_published_total   — mensajes enviados al producer (Counter)
  ocm_kafka_events_processed_total   — mensajes procesados con commit (Counter)
  ocm_kafka_events_failed_total      — mensajes fallados por categoría (Counter)
  ocm_kafka_processing_latency_ms    — latencia de procesamiento (Histogram)

Labels
------
  topic    — nombre del tópico Kafka (e.g. "ohlcv.raw")
  exchange — exchange de origen (e.g. "bybit")
  reason   — motivo de fallo (deserialize_error | schema_mismatch | write_error | dlq_sent)

Uso
---
  from market_data.infrastructure.kafka.metrics import KafkaMetrics
  m = KafkaMetrics(topic="ohlcv.raw")
  m.event_published(exchange="bybit")
  m.event_processed(exchange="bybit", latency_ms=12.3)
  m.event_failed(exchange="bybit", reason="deserialize_error")

Principios: SRP · optional dependency · zero overhead sin Prometheus
"""
from __future__ import annotations

import time
from typing import Protocol, runtime_checkable

try:
    from prometheus_client import Counter, Histogram
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False


# ---------------------------------------------------------------------------
# No-op fallbacks — misma interfaz, cero overhead
# ---------------------------------------------------------------------------

@runtime_checkable
class _CounterProtocol(Protocol):
    """Interfaz minima compartida entre prometheus_client.Counter y _NoOpCounter."""
    def labels(self, **kwargs: str) -> "_CounterProtocol": ...
    def inc(self, amount: float = 1) -> None: ...


@runtime_checkable
class _HistogramProtocol(Protocol):
    """Interfaz minima compartida entre prometheus_client.Histogram y _NoOpHistogram."""
    def labels(self, **kwargs: str) -> "_HistogramProtocol": ...
    def observe(self, amount: float) -> None: ...


class _NoOpCounter:
    def labels(self, **_): return self
    def inc(self, _=1):    return


class _NoOpHistogram:
    def labels(self, **_): return self
    def observe(self, _):  return


# ---------------------------------------------------------------------------
# Registro global — una sola instancia por proceso
# ---------------------------------------------------------------------------

def _make_counter(name: str, doc: str, labels: list) -> "_CounterProtocol":
    return Counter(name, doc, labels) if _PROMETHEUS_AVAILABLE else _NoOpCounter()


def _make_histogram(name: str, doc: str, labels: list, buckets: list) -> "_HistogramProtocol":
    return (
        Histogram(name, doc, labels, buckets=buckets)
        if _PROMETHEUS_AVAILABLE
        else _NoOpHistogram()
    )


_LATENCY_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000]

_events_published = _make_counter(
    "ocm_kafka_events_published_total",
    "Total de mensajes publicados al producer Kafka",
    ["topic", "exchange"],
)

_events_processed = _make_counter(
    "ocm_kafka_events_processed_total",
    "Total de mensajes procesados con commit exitoso",
    ["topic", "exchange"],
)

_events_failed = _make_counter(
    "ocm_kafka_events_failed_total",
    "Total de mensajes fallados por categoría",
    ["topic", "exchange", "reason"],
)

_processing_latency = _make_histogram(
    "ocm_kafka_processing_latency_ms",
    "Latencia de procesamiento end-to-end en ms",
    ["topic", "exchange"],
    _LATENCY_BUCKETS,
)


# ---------------------------------------------------------------------------
# KafkaMetrics — interfaz de alto nivel
# ---------------------------------------------------------------------------

class KafkaMetrics:
    """
    Interfaz de métricas para el pipeline Kafka.

    Instanciar una vez por tópico. Thread-safe (Prometheus lo es).

    Parámetros
    ----------
    topic : nombre canónico del tópico (e.g. "ohlcv.raw").
    """

    def __init__(self, topic: str = "ohlcv.raw") -> None:
        self._topic = topic

    def event_published(self, exchange: str = "unknown") -> None:
        """Incrementa el contador de mensajes publicados."""
        _events_published.labels(topic=self._topic, exchange=exchange).inc()

    def event_processed(
        self,
        exchange:   str   = "unknown",
        latency_ms: float = 0.0,
    ) -> None:
        """Registra un mensaje procesado con commit y su latencia."""
        _events_processed.labels(topic=self._topic, exchange=exchange).inc()
        _processing_latency.labels(topic=self._topic, exchange=exchange).observe(latency_ms)

    def event_failed(
        self,
        exchange: str = "unknown",
        reason:   str = "unknown",
    ) -> None:
        """
        Registra un mensaje fallado.

        reason: deserialize_error | schema_mismatch | write_error | dlq_sent
        """
        _events_failed.labels(
            topic    = self._topic,
            exchange = exchange,
            reason   = reason,
        ).inc()


# ---------------------------------------------------------------------------
# timer — context manager para latencia
# ---------------------------------------------------------------------------

class timer:
    """
    Context manager para medir latencia en ms.

        with timer() as t:
            process(msg)
        latency_ms = t.elapsed_ms
    """

    def __enter__(self) -> "timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self._elapsed_ms = (time.perf_counter() - self._start) * 1_000

    @property
    def elapsed_ms(self) -> float:
        return getattr(self, "_elapsed_ms", 0.0)


__all__ = ["KafkaMetrics", "timer"]
