# funding_producer.py
# ===========================================================
# Source  : FundingRateProducer
# Topic   : funding.raw
# Payload : FundingRateSnapshot
# Note    : Funding rate WebSocket feed
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_FUNDING_PRODUCER
from infra.kafka.topics import TOPIC_FUNDING_RAW

from market_data.domain.ports.kafka_producers import FundingKafkaProducerProtocol


class FundingKafkaProducer(FundingKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/funding_producer.py
    """

    topic = TOPIC_FUNDING_RAW
    group = GROUP_WS_FUNDING_PRODUCER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_FUNDING_RAW!r}, [NOT IMPLEMENTED])"


__all__ = [
    "FundingKafkaProducerProtocol",
    "FundingKafkaProducer",
]
