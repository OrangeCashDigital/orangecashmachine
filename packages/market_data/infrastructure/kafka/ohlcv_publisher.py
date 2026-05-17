"""
market_data/infrastructure/kafka/ohlcv_publisher.py
====================================================

Implementación de OHLCVPublisherPort sobre Kafka.

Responsabilidad única: traducir un OHLCVChunk del dominio al wire format
(EventPayload + KafkaOHLCVBar[]) y publicarlo al topic ohlcv.raw.

Clean Architecture
------------------
Este módulo depende de:
  - domain (OHLCVChunk, Candle)          ← el dominio NO depende de esto
  - shared/kafka (contratos wire)        ← SSOT de serialización
  - ports/kafka_producer (abstracción)   ← DIP

Este módulo NO depende de pandas.

Principios: SRP · DIP · SafeOps · Kappa · SSOT · Clean Architecture
"""

from __future__ import annotations

import uuid as _uuid

from shared.kafka.schemas.ohlcv import (
    EventPayload,
    KafkaOHLCVBar,
    OHLCV_SCHEMA_VERSION as PAYLOAD_SCHEMA_VERSION,
)
from shared.kafka.serializer import serialize, make_ohlcv_key as make_routing_key
from shared.kafka.topics import (
    TOPIC_OHLCV_RAW,
    HEADER_SOURCE,
    HEADER_VERSION,
    HEADER_RUN_ID,
)
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk
from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class KafkaOHLCVPublisher:
    """
    Publica chunks OHLCV a Kafka en formato EventPayload.

    Kappa architecture
    ------------------
    Tanto backfill como incremental pasan por ohlcv.raw.
    El source (backfill | live) se propaga en el header x-ocm-source
    para que el consumer downstream distinga el origen sin cambiar el schema.

    SafeOps
    -------
    publish_chunk() captura cualquier excepción y retorna False.
    Nunca lanza — el caller activa el fallback (escritura directa a Iceberg).
    """

    def __init__(self, producer: KafkaProducerPort) -> None:
        if producer is None:
            raise ValueError("KafkaOHLCVPublisher: producer no puede ser None")
        self._producer = producer

    async def publish_chunk(
        self,
        chunk: OHLCVChunk,
    ) -> bool:
        """
        Serializa el OHLCVChunk a EventPayload y lo publica a ohlcv.raw.

        Fail-Fast interno: lanza ValueError si chunk está vacío (bug del caller).
        SafeOps externo: cualquier fallo de Kafka retorna False.
        """
        if chunk.is_empty:
            raise ValueError(
                f"KafkaOHLCVPublisher.publish_chunk: chunk vacío "
                f"(exchange={chunk.exchange} symbol={chunk.symbol} "
                f"timeframe={chunk.timeframe})"
            )

        try:
            # Lazy start — idempotente
            await self._producer.start()

            # Mapping directo Candle → KafkaOHLCVBar (sin pandas)
            bars = [
                KafkaOHLCVBar(
                    ts=c.timestamp_ms,
                    open=c.open,
                    high=c.high,
                    low=c.low,
                    close=c.close,
                    volume=c.volume,
                )
                for c in chunk.candles
            ]

            event = EventPayload(
                event_id=str(_uuid.uuid4()),
                exchange=chunk.exchange,
                symbol=chunk.symbol,
                timeframe=chunk.timeframe,
                batch_start_ts=chunk.start_ms or 0,
                bars=bars,
                source=chunk.source,  # type: ignore[arg-type]
                run_id=chunk.run_id,
            )

            payload_bytes = serialize(event)
            routing_key = make_routing_key(chunk.exchange, chunk.symbol, chunk.timeframe)
            headers = {
                HEADER_SOURCE: chunk.source,
                HEADER_VERSION: str(PAYLOAD_SCHEMA_VERSION),
                HEADER_RUN_ID: chunk.run_id,
            }

            await self._producer.produce(
                topic=TOPIC_OHLCV_RAW,
                value=payload_bytes,
                key=routing_key,
                headers=headers,
            )
            return True

        except Exception:
            return False  # SafeOps — caller activa fallback a Iceberg directo
