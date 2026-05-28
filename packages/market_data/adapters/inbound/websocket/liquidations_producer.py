# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/liquidations_producer.py
===============================================================

LiquidationsKafkaProducer — productor real WS → Kafka.

Cadena
------
  WsLiquidationsStream (cryptofeed LIQUIDATIONS channel)
      → on_liquidation(exchange, symbol, ...) → LiquidationPayload → liquidations.raw

DIP: recibe KafkaProducerPort por constructor.

Principios: DIP · SRP · SafeOps · Kappa · SSOT
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from loguru import logger

from shared.kafka.schemas.liquidations import LiquidationPayload
from shared.kafka.serializer import make_symbol_key, serialize
from shared.kafka.topics import (
    GROUP_WS_LIQUIDATIONS_PRODUCER,
    HEADER_DOMAIN,
    HEADER_SOURCE,
    TOPIC_LIQUIDATIONS_RAW,
)

if TYPE_CHECKING:
    from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class LiquidationsKafkaProducer:
    """Productor Kafka para liquidaciones forzadas."""

    topic: str = TOPIC_LIQUIDATIONS_RAW
    group: str = GROUP_WS_LIQUIDATIONS_PRODUCER

    _KAPPA_HEADERS: dict = {
        HEADER_SOURCE: "live",
        HEADER_DOMAIN: "liquidations",
    }

    def __init__(self, producer: "KafkaProducerPort") -> None:
        self._producer = producer
        self._log = logger.bind(
            component="LiquidationsKafkaProducer",
            topic=self.topic,
        )

    async def start(self) -> None:
        await self._producer.start()
        self._log.info("liquidations_producer_started")

    async def close(self) -> None:
        try:
            await self._producer.stop()
            self._log.info("liquidations_producer_closed")
        except Exception as exc:
            self._log.warning("liquidations_producer_close_error", error=str(exc))

    async def on_liquidation(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timestamp_ms: int,
        price: str,
        quantity: str,
        side: str,
        quantity_usd: Optional[str] = None,
        order_type: str = "market",
    ) -> None:
        """
        Serializa y publica un evento de liquidación.

        SafeOps: captura excepciones.
        """
        try:
            payload = LiquidationPayload(
                exchange=exchange,
                symbol=symbol,
                market_type=market_type,
                timestamp_ms=timestamp_ms,
                price=price,
                quantity=quantity,
                quantity_usd=quantity_usd,
                side=side,  # type: ignore[arg-type]
                order_type=order_type,
            )
            key = make_symbol_key(exchange, symbol)
            await self._producer.produce(
                topic=self.topic,
                value=serialize(payload),
                key=key,
                headers=self._KAPPA_HEADERS,
            )
            self._log.bind(exchange=exchange, symbol=symbol).debug(
                "liquidation_published | side={} price={} qty={}", side, price, quantity
            )
        except Exception as exc:
            self._log.bind(exchange=exchange, symbol=symbol, error=str(exc)).warning("liquidation_publish_failed")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """API de bajo nivel — compatibilidad legacy."""
        await self._producer.produce(topic=self.topic, value=payload, key=key, headers=self._KAPPA_HEADERS)

    def __repr__(self) -> str:
        return f"LiquidationsKafkaProducer(topic={self.topic!r})"


__all__ = ["LiquidationsKafkaProducer"]
