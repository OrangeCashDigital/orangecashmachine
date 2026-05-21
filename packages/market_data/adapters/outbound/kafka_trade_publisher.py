"""
market_data/adapters/outbound/kafka_trade_publisher.py
───────────────────────────────────────────────────────
KafkaTradePublisher — async Kafka sink implementing TradeCallback.

Publishes each NormalizedTrade as JSON to the configured topic.
Key   = symbol bytes (enables per-symbol partition ordering).
Value = JSON-encoded trade dict (all Decimal fields as strings).

Lifecycle (managed by FeedOrchestrator):
    publisher = KafkaTradePublisher(bootstrap_servers="localhost:9092")
    await publisher.start()
    # ... adapter calls publisher(trade) for each trade ...
    await publisher.stop()
"""
from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer
from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade

_DEFAULT_TOPIC = "market.trades.raw"


class KafkaTradePublisher:
    """Async Kafka publisher — implements TradeCallback interface."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = _DEFAULT_TOPIC,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Create and start the Kafka producer."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=None,
            key_serializer=None,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        logger.info(
            "[kafka-publisher] started | brokers={} topic={}",
            self._bootstrap_servers,
            self._topic,
        )

    async def stop(self) -> None:
        """Flush pending messages and stop the producer gracefully."""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("[kafka-publisher] stopped.")

    # ── TradeCallback interface ───────────────────────────────────────────────

    async def __call__(self, trade: NormalizedTrade) -> None:
        """Publish a normalized trade.  Fail-fast if producer not started."""
        if self._producer is None:
            raise RuntimeError(
                "KafkaTradePublisher: __call__ invoked before start(). "
                "Call await publisher.start() first."
            )
        payload: bytes = json.dumps(trade.to_dict()).encode("utf-8")
        key: bytes = trade.symbol.encode("utf-8")

        await self._producer.send(
            self._topic,
            value=payload,
            key=key,
        )
        logger.debug(
            "[kafka-publisher] published | topic={} symbol={} side={}",
            self._topic,
            trade.symbol,
            trade.side,
        )
