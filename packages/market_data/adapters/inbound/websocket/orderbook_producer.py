# orderbook_producer.py
# =============================================================
# Source  : WsOrderBookStream
# Topic   : orderbook.raw
# Payload : OrderBookSnapshot
# Note    : L2 order book WebSocket feed
from __future__ import annotations

from infra.kafka.base_producer import BaseKafkaProducer
from infra.kafka.groups import GROUP_WS_ORDERBOOK_PRODUCER
from infra.kafka.topics import TOPIC_ORDERBOOK_RAW

from market_data.domain.ports.kafka_producers import OrderBookKafkaProducerProtocol


class OrderBookKafkaProducer(OrderBookKafkaProducerProtocol, BaseKafkaProducer):
    """Stub — NOT IMPLEMENTED.

    Wire up in: market_data/adapters/inbound/websocket/orderbook_producer.py
    """

    topic = TOPIC_ORDERBOOK_RAW
    group = GROUP_WS_ORDERBOOK_PRODUCER

    async def start(self) -> None:
        pass  # SafeOps: no-op en stub

    async def close(self) -> None:
        pass  # SafeOps: no-op en stub

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(topic={TOPIC_ORDERBOOK_RAW!r}, [NOT IMPLEMENTED])"


__all__ = [
    "OrderBookKafkaProducerProtocol",
    "OrderBookKafkaProducer",
]
