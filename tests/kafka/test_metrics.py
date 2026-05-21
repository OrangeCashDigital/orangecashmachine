# -*- coding: utf-8 -*-
"""
tests/kafka/test_metrics.py
=============================

Tests de kafka/metrics.py — KafkaMetrics + timer.

Sin Prometheus real — verifica la interfaz de alto nivel
y el comportamiento del context manager timer.

Principios verificados: SRP · optional dependency · zero overhead
"""

from __future__ import annotations

import time

from market_data.infrastructure.kafka.metrics import KafkaMetrics, timer

# ---------------------------------------------------------------------------
# KafkaMetrics — interfaz de alto nivel
# ---------------------------------------------------------------------------


class TestKafkaMetrics:
    def test_instantiates_with_default_topic(self):
        m = KafkaMetrics()
        assert m._topic == "ohlcv.raw"

    def test_instantiates_with_custom_topic(self):
        m = KafkaMetrics(topic="ohlcv.validated")
        assert m._topic == "ohlcv.validated"

    def test_event_published_does_not_raise(self):
        m = KafkaMetrics(topic="ohlcv.raw")
        m.event_published(exchange="bybit")  # no debe lanzar

    def test_event_published_default_exchange(self):
        m = KafkaMetrics()
        m.event_published()  # exchange="unknown" por defecto

    def test_event_processed_does_not_raise(self):
        m = KafkaMetrics()
        m.event_processed(exchange="bybit", latency_ms=12.3)

    def test_event_processed_zero_latency(self):
        m = KafkaMetrics()
        m.event_processed(exchange="bybit", latency_ms=0.0)

    def test_event_failed_does_not_raise(self):
        m = KafkaMetrics()
        m.event_failed(exchange="bybit", reason="deserialize_error")

    def test_event_failed_all_reason_categories(self):
        """Las cuatro categorías de reason documentadas no lanzan."""
        m = KafkaMetrics()
        reasons = [
            "deserialize_error",
            "schema_mismatch",
            "write_error",
            "dlq_sent",
        ]
        for reason in reasons:
            m.event_failed(exchange="bybit", reason=reason)

    def test_event_failed_default_args(self):
        m = KafkaMetrics()
        m.event_failed()  # exchange="unknown", reason="unknown"

    def test_multiple_topics_independent(self):
        """Instancias con distinto topic no comparten estado visible."""
        m1 = KafkaMetrics(topic="ohlcv.raw")
        m2 = KafkaMetrics(topic="ohlcv.validated")
        assert m1._topic != m2._topic
        # Ambas pueden operar sin interferirse
        m1.event_published(exchange="bybit")
        m2.event_published(exchange="kucoin")


# ---------------------------------------------------------------------------
# timer — context manager para latencia
# ---------------------------------------------------------------------------


class TestTimer:
    def test_elapsed_ms_available_after_exit(self):
        with timer() as t:
            pass
        assert isinstance(t.elapsed_ms, float)

    def test_elapsed_ms_non_negative(self):
        with timer() as t:
            pass
        assert t.elapsed_ms >= 0.0

    def test_elapsed_ms_reflects_real_time(self):
        """Sleep de 10ms → elapsed entre 8ms y 100ms (margen CI)."""
        with timer() as t:
            time.sleep(0.010)
        assert 8.0 <= t.elapsed_ms <= 100.0

    def test_elapsed_ms_zero_before_exit(self):
        """Antes de salir del context manager, elapsed_ms es 0.0."""
        t = timer()
        assert t.elapsed_ms == 0.0  # propiedad default antes de __exit__

    def test_usable_multiple_times(self):
        """Cada uso del context manager produce una medición independiente."""
        with timer() as t1:
            time.sleep(0.005)
        with timer() as t2:
            pass
        assert t1.elapsed_ms >= t2.elapsed_ms or t1.elapsed_ms > 0

    def test_elapsed_ms_in_ms_not_seconds(self):
        """Verificar que la unidad es milisegundos, no segundos."""
        with timer() as t:
            time.sleep(0.020)
        # 20ms sleep → elapsed debe ser > 1.0 (no 0.02)
        assert t.elapsed_ms > 1.0


__all__ = []
