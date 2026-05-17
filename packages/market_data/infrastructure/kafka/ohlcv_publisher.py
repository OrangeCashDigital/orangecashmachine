# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/ohlcv_publisher.py
====================================================

Implementación de OHLCVPublisherPort sobre Kafka.

Responsabilidad única: traducir un DataFrame OHLCV al wire format
(EventPayload + serializer) y publicarlo al topic ohlcv.raw.

Fix B-NEW-01: corregido self._producer.send_async() → self._producer.produce().
  send_async() no existe en KafkaProducerPort — causaba AttributeError en runtime.

Fix B-NEW-05: topics/headers importados desde topics.py (SSOT), no desde el port.

Principios: SRP · DIP · SafeOps · Kappa architecture
"""
from __future__ import annotations

import uuid as _uuid

import pandas as pd

# Migrado a shared.kafka — SSOT de contratos wire (Fix C-NUEVO-3).
# market_data.infrastructure.kafka.payloads es el legacy — deprecado.
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
from market_data.ports.outbound.kafka_producer import KafkaProducerPort


class KafkaOHLCVPublisher:
    """
    Publica chunks OHLCV a Kafka en formato EventPayload.

    Implementa OHLCVPublisherPort (duck typing — sin herencia explícita).
    Satisface el Protocol estructuralmente.

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
        exchange_id: str,
        symbol:      str,
        timeframe:   str,
        df:          pd.DataFrame,
        source:      str,
        run_id:      str = "",
    ) -> bool:
        """
        Serializa el DataFrame a EventPayload y lo publica a ohlcv.raw.

        Fail-Fast interno: lanza ValueError si df está vacío (bug del caller).
        SafeOps externo: cualquier fallo de Kafka retorna False.
        """
        if df is None or df.empty:
            raise ValueError(
                f"KafkaOHLCVPublisher.publish_chunk: df vacío "
                f"(exchange={exchange_id} symbol={symbol} timeframe={timeframe})"
            )

        try:
            # Lazy start — idempotente: KafkaProducerAdapter guarda _started flag.
            await self._producer.start()

            bars = [
                KafkaOHLCVBar(
                    ts     = int(row["timestamp"].timestamp() * 1000),
                    open   = float(row["open"]),
                    high   = float(row["high"]),
                    low    = float(row["low"]),
                    close  = float(row["close"]),
                    volume = float(row["volume"]),
                )
                for _, row in df.iterrows()
            ]

            event = EventPayload(
                event_id       = str(_uuid.uuid4()),
                exchange       = exchange_id,
                symbol         = symbol,
                timeframe      = timeframe,
                batch_start_ts = int(df["timestamp"].min().timestamp() * 1000),
                bars           = bars,
                source         = source,  # type: ignore[arg-type]
                run_id         = run_id,
            )

            payload_bytes = serialize(event)
            routing_key   = make_routing_key(exchange_id, symbol, timeframe)
            headers       = {
                HEADER_SOURCE:  source,
                HEADER_VERSION: str(PAYLOAD_SCHEMA_VERSION),
                HEADER_RUN_ID:  run_id,
            }

            # FIX B-NEW-01: produce() — método canónico del port.
            # send_async() no existe en KafkaProducerPort.
            await self._producer.produce(
                topic   = TOPIC_OHLCV_RAW,
                value   = payload_bytes,
                key     = routing_key,
                headers = headers,
            )
            return True

        except Exception:
            return False  # SafeOps — caller activa fallback a Iceberg directo
