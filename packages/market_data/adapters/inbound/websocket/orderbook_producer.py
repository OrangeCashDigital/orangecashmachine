# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/orderbook_producer.py
=============================================================

OrderBookKafkaProducer — productor real WS → Kafka.

Cadena
------
  CryptofeedOrderBookStream
      → on_snapshot(OrderBookSnapshot) → OrderBookSnapshotPayload → orderbook.raw
      → on_delta(OrderBookDelta)       → OrderBookDeltaPayload    → orderbook.raw

DIP
---
Recibe KafkaProducerPort por constructor.
No instancia KafkaProducerAdapter internamente — el Composition Root inyecta.

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"
  Mismo símbolo → misma partición → snapshot antes que deltas (FIFO).

Kappa headers
-------------
  x-ocm-source  : "live"
  x-ocm-domain  : "orderbook"

Principios: DIP · SRP · SafeOps · Kappa · SSOT
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from loguru import logger

from shared.kafka.schemas.orderbook import (
    OrderBookDeltaPayload,
    OrderBookSnapshotPayload,
)
from shared.kafka.serializer import make_symbol_key, serialize
from shared.kafka.topics import (
    GROUP_WS_ORDERBOOK_PRODUCER,
    HEADER_DOMAIN,
    HEADER_SOURCE,
    TOPIC_ORDERBOOK_RAW,
)

if TYPE_CHECKING:
    from market_data.infrastructure.kafka.metrics import KafkaMetrics
    from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class OrderBookKafkaProducer:
    """
    Productor Kafka para L2 order book WebSocket.

    Recibe eventos de OrderBookStream y los publica a orderbook.raw.
    Diseñado para ser pasado como callback a CryptofeedOrderBookStream.

    Uso
    ---
        producer = OrderBookKafkaProducer(kafka_port)
        stream = CryptofeedOrderBookStream(
            exchange="bybit",
            symbols=["BTC/USDT"],
            on_snapshot=producer.on_snapshot,
            on_delta=producer.on_delta,
        )
    """

    topic: str = TOPIC_ORDERBOOK_RAW
    group: str = GROUP_WS_ORDERBOOK_PRODUCER

    _KAPPA_HEADERS: dict = {
        HEADER_SOURCE: "live",
        HEADER_DOMAIN: "orderbook",
    }

    def __init__(
        self,
        producer: "KafkaProducerPort",
        metrics: Optional["KafkaMetrics"] = None,
    ) -> None:
        self._producer = producer
        self._metrics = metrics or self._make_metrics()
        self._log = logger.bind(
            component="OrderBookKafkaProducer",
            topic=self.topic,
        )

    @staticmethod
    def _make_metrics() -> "KafkaMetrics":
        # Import lazy — evita arista adapters → infrastructure en import-linter
        # (BC-08). Mismo patrón que CCXTAdapter en pipeline_orchestrator.
        from market_data.infrastructure.kafka.metrics import KafkaMetrics

        return KafkaMetrics(topic=TOPIC_ORDERBOOK_RAW)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Inicia el KafkaProducerPort subyacente. Idempotente."""
        await self._producer.start()
        self._log.info("orderbook_producer_started")

    async def close(self) -> None:
        """Cierra el KafkaProducerPort. SafeOps."""
        try:
            await self._producer.stop()
            self._log.info("orderbook_producer_closed")
        except Exception as exc:
            self._log.warning("orderbook_producer_close_error", error=str(exc))

    # ------------------------------------------------------------------ #
    # Callbacks — API para OrderBookStream                                 #
    # ------------------------------------------------------------------ #

    async def on_snapshot(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: list,
        asks: list,
        depth: int = 0,
        checksum: int | None = None,
    ) -> None:
        """
        Serializa y publica un snapshot L2 completo.

        SafeOps: captura cualquier excepción — el stream no debe morir
        porque el producer falle en un mensaje.

        Métricas (F2.6c): publica ocm_kafka_events_published_total y
        ocm_kafka_processing_latency_ms (KafkaMetrics existente) al éxito;
        ocm_kafka_events_failed_total al fallo.
        """
        from market_data.infrastructure.kafka.metrics import timer

        try:
            payload = OrderBookSnapshotPayload(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=timestamp_ms,
                bids=bids,
                asks=asks,
                depth=depth,
                checksum=checksum,
            )
            key = make_symbol_key(exchange, symbol)
            with timer() as t:
                await self._producer.produce(
                    topic=self.topic,
                    value=serialize(payload),
                    key=key,
                    headers=self._KAPPA_HEADERS,
                )
            self._metrics.event_published(exchange=exchange)
            self._metrics.event_processed(exchange=exchange, latency_ms=t.elapsed_ms)
            self._log.bind(exchange=exchange, symbol=symbol).debug(
                "orderbook_snapshot_published | bids={} asks={}", len(bids), len(asks)
            )
        except Exception as exc:
            self._metrics.event_failed(exchange=exchange, reason="write_error")
            self._log.bind(exchange=exchange, symbol=symbol, error=str(exc)).warning(
                "orderbook_snapshot_publish_failed"
            )

    async def on_delta(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        side: str,
        price: str,
        size: str,
    ) -> None:
        """
        Serializa y publica un delta incremental.

        SafeOps: captura cualquier excepción.

        Métricas (F2.6c): ocm_kafka_events_published_total +
        ocm_kafka_processing_latency_ms al éxito; ocm_kafka_events_failed_total
        (reason=write_error) al fallo.
        """
        from market_data.infrastructure.kafka.metrics import timer

        try:
            payload = OrderBookDeltaPayload(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=timestamp_ms,
                side=side,  # type: ignore[arg-type]
                price=price,
                size=size,
            )
            key = make_symbol_key(exchange, symbol)
            with timer() as t:
                await self._producer.produce(
                    topic=self.topic,
                    value=serialize(payload),
                    key=key,
                    headers=self._KAPPA_HEADERS,
                )
            self._metrics.event_published(exchange=exchange)
            self._metrics.event_processed(exchange=exchange, latency_ms=t.elapsed_ms)
            self._log.bind(exchange=exchange, symbol=symbol).debug(
                "orderbook_delta_published | side={} price={}", side, price
            )
        except Exception as exc:
            self._metrics.event_failed(exchange=exchange, reason="write_error")
            self._log.bind(exchange=exchange, symbol=symbol, error=str(exc)).warning("orderbook_delta_publish_failed")

    # ------------------------------------------------------------------ #
    # Compatibilidad con la firma de stub anterior                         #
    # ------------------------------------------------------------------ #

    async def produce(self, payload: bytes, key: bytes | None = None) -> None:
        """
        API de bajo nivel — publica bytes pre-serializados.

        Usar on_snapshot() / on_delta() para el flujo normal.
        Este método existe para compatibilidad con callers legacy.
        """
        await self._producer.produce(
            topic=self.topic,
            value=payload,
            key=key,
            headers=self._KAPPA_HEADERS,
        )

    def __repr__(self) -> str:
        return f"OrderBookKafkaProducer(topic={self.topic!r})"


__all__ = ["OrderBookKafkaProducer"]
