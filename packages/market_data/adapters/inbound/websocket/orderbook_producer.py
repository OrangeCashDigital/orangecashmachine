# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/orderbook_producer.py
=============================================================
OrderBookKafkaProducer — stub estructural.

Source  : WsOrderBookStream (cryptofeed L2 order book)
Topic   : orderbook.raw
Payload : OrderBookSnapshot (futuro)

Estado actual
-------------
NOT IMPLEMENTED — start()/close() son no-ops. SafeOps.
El sistema arranca sin errores de importación.
Implementación real: cryptofeed BOOK channel → serializar → produce().

Principios: DIP · SafeOps · KISS · SRP
"""

from __future__ import annotations

from loguru import logger

from shared.kafka.topics import GROUP_WS_ORDERBOOK_PRODUCER, TOPIC_ORDERBOOK_RAW


class OrderBookKafkaProducer:
    """
    Stub del productor Kafka para L2 order book WebSocket.

    Lifecycle
    ---------
    start()  → no-op (stub)
    close()  → no-op (stub)
    produce() → no-op (stub)

    Wire-up futuro
    --------------
    1. Instanciar KafkaProducerAdapter.from_env()
    2. Suscribirse a cryptofeed BOOK channel
    3. Serializar OrderBookSnapshot → bytes
    4. self._producer.produce(TOPIC_ORDERBOOK_RAW, value=payload)
    """

    topic: str = TOPIC_ORDERBOOK_RAW
    group: str = GROUP_WS_ORDERBOOK_PRODUCER

    def __init__(self) -> None:
        self._log = logger.bind(
            component="OrderBookKafkaProducer",
            topic=self.topic,
        )
        self._log.warning(
            "OrderBookKafkaProducer stub instanciado — NOT IMPLEMENTED. orderbook.raw no está siendo publicado."
        )

    async def start(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("OrderBookKafkaProducer.start() — stub no-op")

    async def close(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("OrderBookKafkaProducer.close() — stub no-op")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """SafeOps: no-op en stub. Payload descartado silenciosamente."""
        pass

    def __repr__(self) -> str:
        return f"OrderBookKafkaProducer(topic={self.topic!r}, [NOT IMPLEMENTED])"


__all__ = ["OrderBookKafkaProducer"]
