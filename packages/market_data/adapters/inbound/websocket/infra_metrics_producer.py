# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/infra_metrics_producer.py
================================================================
InfraMetricsKafkaProducer — stub estructural.

Source  : SystemMetrics (latencias, errores, circuit breakers)
Topic   : infra.metrics
Payload : InfraMetricEvent (futuro)

Estado actual
-------------
NOT IMPLEMENTED — start()/close() son no-ops. SafeOps.

Wire-up futuro
--------------
Prometheus scrape → serializar → produce() periódico.
O push directo desde MetricsEmitter en cada producer/consumer.
Consumer: ObservabilityConsumer → Prometheus Pushgateway.

Principios: DIP · SafeOps · KISS · SRP
"""

from __future__ import annotations

from loguru import logger

from shared.kafka.topics import GROUP_INFRA_METRICS_EMITTER, TOPIC_INFRA_METRICS


class InfraMetricsKafkaProducer:
    """Stub del productor Kafka para métricas de infraestructura."""

    topic: str = TOPIC_INFRA_METRICS
    group: str = GROUP_INFRA_METRICS_EMITTER

    def __init__(self) -> None:
        self._log = logger.bind(
            component="InfraMetricsKafkaProducer",
            topic=self.topic,
        )
        self._log.warning(
            "InfraMetricsKafkaProducer stub instanciado — NOT IMPLEMENTED. infra.metrics no está siendo publicado."
        )

    async def start(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("InfraMetricsKafkaProducer.start() — stub no-op")

    async def close(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("InfraMetricsKafkaProducer.close() — stub no-op")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """SafeOps: no-op en stub."""
        pass

    def __repr__(self) -> str:
        return f"InfraMetricsKafkaProducer(topic={self.topic!r}, [NOT IMPLEMENTED])"


__all__ = ["InfraMetricsKafkaProducer"]
