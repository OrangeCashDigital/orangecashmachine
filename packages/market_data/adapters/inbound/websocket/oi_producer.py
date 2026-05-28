# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/oi_producer.py
======================================================

OIKafkaProducer — productor real WS/REST → Kafka.

Cadena
------
  OISource (WS Bybit / REST polling)
      → on_open_interest(exchange, symbol, ...) → OpenInterestPayload → oi.raw

DIP: recibe KafkaProducerPort por constructor.

Principios: DIP · SRP · SafeOps · Kappa · SSOT
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from loguru import logger

from shared.kafka.schemas.oi import OpenInterestPayload
from shared.kafka.serializer import make_symbol_key, serialize
from shared.kafka.topics import (
    GROUP_WS_OI_PRODUCER,
    HEADER_DOMAIN,
    HEADER_SOURCE,
    TOPIC_OI_RAW,
)

if TYPE_CHECKING:
    from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class OIKafkaProducer:
    """Productor Kafka para open interest snapshots."""

    topic: str = TOPIC_OI_RAW
    group: str = GROUP_WS_OI_PRODUCER

    _KAPPA_HEADERS: dict = {
        HEADER_SOURCE: "live",
        HEADER_DOMAIN: "oi",
    }

    def __init__(self, producer: "KafkaProducerPort") -> None:
        self._producer = producer
        self._log = logger.bind(
            component="OIKafkaProducer",
            topic=self.topic,
        )

    async def start(self) -> None:
        await self._producer.start()
        self._log.info("oi_producer_started")

    async def close(self) -> None:
        try:
            await self._producer.stop()
            self._log.info("oi_producer_closed")
        except Exception as exc:
            self._log.warning("oi_producer_close_error", error=str(exc))

    async def on_open_interest(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timestamp_ms: int,
        open_interest_contracts: str,
        open_interest_value: Optional[str] = None,
        mark_price: Optional[str] = None,
    ) -> None:
        """
        Serializa y publica un OI snapshot.

        SafeOps: captura excepciones.
        """
        try:
            payload = OpenInterestPayload(
                exchange=exchange,
                symbol=symbol,
                market_type=market_type,
                timestamp_ms=timestamp_ms,
                open_interest_contracts=open_interest_contracts,
                open_interest_value=open_interest_value,
                mark_price=mark_price,
            )
            key = make_symbol_key(exchange, symbol)
            await self._producer.produce(
                topic=self.topic,
                value=serialize(payload),
                key=key,
                headers=self._KAPPA_HEADERS,
            )
            self._log.bind(exchange=exchange, symbol=symbol).debug(
                "oi_published | contracts={}", open_interest_contracts
            )
        except Exception as exc:
            self._log.bind(exchange=exchange, symbol=symbol, error=str(exc)).warning("oi_publish_failed")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """API de bajo nivel — compatibilidad legacy."""
        await self._producer.produce(topic=self.topic, value=payload, key=key, headers=self._KAPPA_HEADERS)

    def __repr__(self) -> str:
        return f"OIKafkaProducer(topic={self.topic!r})"


__all__ = ["OIKafkaProducer"]
