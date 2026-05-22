# liquidations_producer.py
# ================================================================
# Source  : WsLiquidationsStream
# Topic   : liquidations.raw
# Payload : LiquidationEvent
# Note    : Liquidation WebSocket feed
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_LIQUIDATIONS_PRODUCER
from infra.kafka.topics import TOPIC_LIQUIDATIONS_RAW

from market_data.domain.ports.kafka_producers import LiquidationsKafkaProducerProtocol


class LiquidationsKafkaProducer(LiquidationsKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/liquidations_producer.py
    """

    topic = TOPIC_LIQUIDATIONS_RAW
    group = GROUP_WS_LIQUIDATIONS_PRODUCER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_LIQUIDATIONS_RAW!r}, [NOT IMPLEMENTED])"


__all__ = [
    "LiquidationsKafkaProducerProtocol",
    "LiquidationsKafkaProducer",
]
