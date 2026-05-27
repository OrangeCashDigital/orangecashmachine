# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/liquidations_producer.py
===============================================================
LiquidationsKafkaProducer — stub estructural.

Source  : WsLiquidationsStream (cryptofeed LIQUIDATIONS channel)
Topic   : liquidations.raw
Payload : LiquidationEvent (futuro)

Estado actual
-------------
NOT IMPLEMENTED — start()/close() son no-ops. SafeOps.

Wire-up futuro
--------------
cryptofeed LIQUIDATIONS channel (Bybit perpetuals).
KuCoin Futures no expone liquidaciones públicas por WS.

Principios: DIP · SafeOps · KISS · SRP
"""

from __future__ import annotations

from loguru import logger

from shared.kafka.topics import GROUP_WS_LIQUIDATIONS_PRODUCER, TOPIC_LIQUIDATIONS_RAW


class LiquidationsKafkaProducer:
    """Stub del productor Kafka para liquidaciones forzadas."""

    topic: str = TOPIC_LIQUIDATIONS_RAW
    group: str = GROUP_WS_LIQUIDATIONS_PRODUCER

    def __init__(self) -> None:
        self._log = logger.bind(
            component="LiquidationsKafkaProducer",
            topic=self.topic,
        )
        self._log.warning(
            "LiquidationsKafkaProducer stub instanciado — NOT IMPLEMENTED. liquidations.raw no está siendo publicado."
        )

    async def start(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("LiquidationsKafkaProducer.start() — stub no-op")

    async def close(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("LiquidationsKafkaProducer.close() — stub no-op")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """SafeOps: no-op en stub."""
        pass

    def __repr__(self) -> str:
        return f"LiquidationsKafkaProducer(topic={self.topic!r}, [NOT IMPLEMENTED])"


__all__ = ["LiquidationsKafkaProducer"]
