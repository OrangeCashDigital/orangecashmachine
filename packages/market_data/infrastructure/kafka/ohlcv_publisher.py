# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/ohlcv_publisher.py
====================================================

Implementación de OHLCVPublisherPort sobre Kafka.

Responsabilidad única: traducir un DataFrame OHLCV al wire format
(EventPayload + serializer) y publicarlo al topic ohlcv.raw.

Por qué está en infrastructure/
--------------------------------
Conoce tres detalles concretos de infraestructura:
  1. EventPayload / KafkaOHLCVBar   — schema del mensaje Kafka
  2. serialize / make_routing_key   — serialización a bytes
  3. TOPIC_OHLCV_RAW, HEADER_*      — constantes del protocolo (ports)

Application/domain solo ven OHLCVPublisherPort — ignorancia total del
formato de wire. Cumple DIP: la abstracción está en ports/, la
implementación aquí.

Principios: SRP · DIP · SafeOps · Kappa architecture
"""
from __future__ import annotations

import uuid as _uuid

import pandas as pd

from market_data.infrastructure.kafka.payloads import (
    EventPayload,
    KafkaOHLCVBar,
    PAYLOAD_SCHEMA_VERSION,
)
from market_data.infrastructure.kafka.serializer import serialize, make_routing_key
from market_data.ports.outbound.kafka_producer import (
    KafkaProducerPort,
    TOPIC_OHLCV_RAW,
    HEADER_SOURCE,
    HEADER_VERSION,
    HEADER_RUN_ID,
)


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
            # No puede iniciarse en _build_kafka_publisher_safe() porque start() es async.
            # SafeOps: si start() falla aquí, la excepción es capturada por el bloque
            # exterior y publish_chunk() retorna False → fallback a Iceberg directo.
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

            return await self._producer.send_async(
                topic   = TOPIC_OHLCV_RAW,
                value   = payload_bytes,
                key     = routing_key,
                headers = headers,
            )

        except Exception:
            return False  # SafeOps — caller activa fallback a Iceberg directo
