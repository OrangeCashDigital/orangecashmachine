# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/oi_producer.py
======================================================
OIKafkaProducer — stub estructural.

Source  : OIProducer (WS o polling REST según exchange)
Topic   : oi.raw
Payload : OpenInterestSnapshot (futuro)

Estado actual
-------------
NOT IMPLEMENTED — start()/close() son no-ops. SafeOps.

Wire-up futuro
--------------
cryptofeed OPEN_INTEREST channel (donde disponible) o REST polling
periódico (Bybit/KuCoin Futures exponen OI por REST).

Principios: DIP · SafeOps · KISS · SRP
"""

from __future__ import annotations

from loguru import logger

from shared.kafka.topics import GROUP_WS_OI_PRODUCER, TOPIC_OI_RAW


class OIKafkaProducer:
    """Stub del productor Kafka para open interest."""

    topic: str = TOPIC_OI_RAW
    group: str = GROUP_WS_OI_PRODUCER

    def __init__(self) -> None:
        self._log = logger.bind(
            component="OIKafkaProducer",
            topic=self.topic,
        )
        self._log.warning("OIKafkaProducer stub instanciado — NOT IMPLEMENTED. oi.raw no está siendo publicado.")

    async def start(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("OIKafkaProducer.start() — stub no-op")

    async def close(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("OIKafkaProducer.close() — stub no-op")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """SafeOps: no-op en stub."""
        pass

    def __repr__(self) -> str:
        return f"OIKafkaProducer(topic={self.topic!r}, [NOT IMPLEMENTED])"


__all__ = ["OIKafkaProducer"]
