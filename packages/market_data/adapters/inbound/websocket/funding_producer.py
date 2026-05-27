# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/funding_producer.py
==========================================================
FundingKafkaProducer — stub estructural.

Source  : FundingRateProducer (WS o polling REST según exchange)
Topic   : funding.raw
Payload : FundingRateSnapshot (futuro)

Estado actual
-------------
NOT IMPLEMENTED — start()/close() son no-ops. SafeOps.

Wire-up futuro
--------------
cryptofeed FUNDING channel (Bybit perpetuals) o REST polling
(KuCoin Futures no tiene WS funding — usar polling periódico).

Principios: DIP · SafeOps · KISS · SRP
"""

from __future__ import annotations

from loguru import logger

from shared.kafka.topics import GROUP_WS_FUNDING_PRODUCER, TOPIC_FUNDING_RAW


class FundingKafkaProducer:
    """Stub del productor Kafka para funding rates."""

    topic: str = TOPIC_FUNDING_RAW
    group: str = GROUP_WS_FUNDING_PRODUCER

    def __init__(self) -> None:
        self._log = logger.bind(
            component="FundingKafkaProducer",
            topic=self.topic,
        )
        self._log.warning(
            "FundingKafkaProducer stub instanciado — NOT IMPLEMENTED. funding.raw no está siendo publicado."
        )

    async def start(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("FundingKafkaProducer.start() — stub no-op")

    async def close(self) -> None:
        """SafeOps: no-op en stub."""
        self._log.debug("FundingKafkaProducer.close() — stub no-op")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """SafeOps: no-op en stub."""
        pass

    def __repr__(self) -> str:
        return f"FundingKafkaProducer(topic={self.topic!r}, [NOT IMPLEMENTED])"


__all__ = ["FundingKafkaProducer"]
