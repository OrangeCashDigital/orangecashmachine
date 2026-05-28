# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/funding_producer.py
==========================================================

FundingKafkaProducer — productor real WS/REST → Kafka.

Cadena
------
  FundingRateSource (WS Bybit / REST polling KuCoin Futures)
      → on_funding_rate(exchange, symbol, ...) → FundingRatePayload → funding.raw

DIP: recibe KafkaProducerPort por constructor.

Kappa headers
-------------
  x-ocm-source  : "live"
  x-ocm-domain  : "funding"

Principios: DIP · SRP · SafeOps · Kappa · SSOT
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from loguru import logger

from shared.kafka.schemas.funding import FundingRatePayload
from shared.kafka.serializer import make_symbol_key, serialize
from shared.kafka.topics import (
    GROUP_WS_FUNDING_PRODUCER,
    HEADER_DOMAIN,
    HEADER_SOURCE,
    TOPIC_FUNDING_RAW,
)

if TYPE_CHECKING:
    from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class FundingKafkaProducer:
    """Productor Kafka para funding rates de perpetuos."""

    topic: str = TOPIC_FUNDING_RAW
    group: str = GROUP_WS_FUNDING_PRODUCER

    _KAPPA_HEADERS: dict = {
        HEADER_SOURCE: "live",
        HEADER_DOMAIN: "funding",
    }

    def __init__(self, producer: "KafkaProducerPort") -> None:
        self._producer = producer
        self._log = logger.bind(
            component="FundingKafkaProducer",
            topic=self.topic,
        )

    async def start(self) -> None:
        await self._producer.start()
        self._log.info("funding_producer_started")

    async def close(self) -> None:
        try:
            await self._producer.stop()
            self._log.info("funding_producer_closed")
        except Exception as exc:
            self._log.warning("funding_producer_close_error", error=str(exc))

    async def on_funding_rate(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timestamp_ms: int,
        funding_rate: str,
        next_funding_ms: Optional[int] = None,
        interval_h: Optional[int] = None,
        predicted_rate: Optional[str] = None,
    ) -> None:
        """
        Serializa y publica un funding rate snapshot.

        SafeOps: captura excepciones — la fuente no debe morir por el producer.
        """
        try:
            payload = FundingRatePayload(
                exchange=exchange,
                symbol=symbol,
                market_type=market_type,
                timestamp_ms=timestamp_ms,
                funding_rate=funding_rate,
                next_funding_ms=next_funding_ms,
                interval_h=interval_h,
                predicted_rate=predicted_rate,
            )
            key = make_symbol_key(exchange, symbol)
            await self._producer.produce(
                topic=self.topic,
                value=serialize(payload),
                key=key,
                headers=self._KAPPA_HEADERS,
            )
            self._log.bind(exchange=exchange, symbol=symbol).debug("funding_rate_published | rate={}", funding_rate)
        except Exception as exc:
            self._log.bind(exchange=exchange, symbol=symbol, error=str(exc)).warning("funding_rate_publish_failed")

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """API de bajo nivel — compatibilidad legacy."""
        await self._producer.produce(topic=self.topic, value=payload, key=key, headers=self._KAPPA_HEADERS)

    def __repr__(self) -> str:
        return f"FundingKafkaProducer(topic={self.topic!r})"


__all__ = ["FundingKafkaProducer"]
