# infra_metrics_producer.py
# =================================================================
# Source  : SystemMetrics
# Topic   : infra.metrics
# Payload : InfraMetricEvent
# Note    : Infrastructure metrics emitter
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_INFRA_METRICS_EMITTER
from infra.kafka.topics import TOPIC_INFRA_METRICS

from market_data.domain.ports.kafka_producers import InfraMetricsKafkaProducerProtocol


class InfraMetricsKafkaProducer(InfraMetricsKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/infra_metrics_producer.py
    """

    topic = TOPIC_INFRA_METRICS
    group = GROUP_INFRA_METRICS_EMITTER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_INFRA_METRICS!r}, [NOT IMPLEMENTED])"


__all__ = [
    "InfraMetricsKafkaProducerProtocol",
    "InfraMetricsKafkaProducer",
]
