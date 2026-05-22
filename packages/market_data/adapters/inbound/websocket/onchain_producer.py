# onchain_producer.py
# ===========================================================
# Source  : BlockchainEvents
# Topic   : onchain.raw
# Payload : OnchainEvent
# Note    : On-chain event producer
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_ONCHAIN_PRODUCER
from infra.kafka.topics import TOPIC_ONCHAIN_RAW

from market_data.domain.ports.kafka_producers import OnchainKafkaProducerProtocol


class OnchainKafkaProducer(OnchainKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/onchain_producer.py
    """

    topic = TOPIC_ONCHAIN_RAW
    group = GROUP_WS_ONCHAIN_PRODUCER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_ONCHAIN_RAW!r}, [NOT IMPLEMENTED])"


__all__ = [
    "OnchainKafkaProducerProtocol",
    "OnchainKafkaProducer",
]
