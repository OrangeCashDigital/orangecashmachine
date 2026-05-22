# oi_producer.py
# ======================================================
# Source  : OIProducer
# Topic   : oi.raw
# Payload : OpenInterestSnapshot
# Note    : Open interest WebSocket feed
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_OI_PRODUCER
from infra.kafka.topics import TOPIC_OI_RAW

from market_data.domain.ports.kafka_producers import OIKafkaProducerProtocol


class OIKafkaProducer(OIKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/oi_producer.py
    """

    topic = TOPIC_OI_RAW
    group = GROUP_WS_OI_PRODUCER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_OI_RAW!r}, [NOT IMPLEMENTED])"


__all__ = [
    "OIKafkaProducerProtocol",
    "OIKafkaProducer",
]
