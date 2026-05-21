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

ACL de source (Anti-Corruption Layer)
--------------------------------------
OHLCVChunk.source usa el vocabulario del dominio: "rest", "backfill",
"live", "replay". EventPayload.source usa DataSource (Literal wire):
"backfill", "live", "replay". Los vocabularios son distintos por diseño —
el dominio no conoce el wire format.

_map_source() es el ACL que traduce en la frontera, sin contaminar ninguno
de los dos lados. SSOT: _CHUNK_SOURCE_TO_DATASOURCE en este módulo.

Kappa semantics
---------------
Solo source="live" (WebSocket en tiempo real) debe generar señales de
trading. REST polling = backfill semánticamente, aunque sea incremental.
EventPayload.should_generate_signal() es el árbitro en consumers.

Principios: SRP · DIP · SafeOps · Kappa · SSOT · ACL · Clean Architecture
"""

from __future__ import annotations

import uuid as _uuid

from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk
from market_data.ports.outbound.kafka_producer import KafkaProducerPort
from shared.kafka.schemas.ohlcv import (
    DATASOURCE_BACKFILL,
    DATASOURCE_LIVE,
    DATASOURCE_REPLAY,
    DataSource,
    EventPayload,
    KafkaOHLCVBar,
)
from shared.kafka.schemas.ohlcv import (
    OHLCV_SCHEMA_VERSION as PAYLOAD_SCHEMA_VERSION,
)
from shared.kafka.serializer import make_ohlcv_key as make_routing_key
from shared.kafka.serializer import serialize
from shared.kafka.topics import (
    HEADER_RUN_ID,
    HEADER_SOURCE,
    HEADER_VERSION,
    TOPIC_OHLCV_RAW,
)

# ---------------------------------------------------------------------------
# ACL: domain source → wire DataSource
# ---------------------------------------------------------------------------

# SSOT de traducción entre vocabularios.
#
# Dominio (OHLCVChunk.source)  →  Wire (DataSource)
# ─────────────────────────────────────────────────────────────────────────────
# "rest"      REST polling incremental → BACKFILL (no genera señal de trading)
# "backfill"  backfill histórico       → BACKFILL
# "live"      WebSocket tiempo real    → LIVE     (genera señal de trading)
# "replay"    Kafka seek_to_beginning  → REPLAY   (no genera señal de trading)
#
# Fail-Soft: source desconocido → BACKFILL (seguro — no genera señales).
_CHUNK_SOURCE_TO_DATASOURCE: dict[str, DataSource] = {
    "rest": DATASOURCE_BACKFILL,
    "backfill": DATASOURCE_BACKFILL,
    "live": DATASOURCE_LIVE,
    "replay": DATASOURCE_REPLAY,
}


def _map_source(source: str) -> DataSource:
    """
    ACL: traduce OHLCVChunk.source (vocabulario dominio) → DataSource (wire).

    Fail-Soft: source desconocido → DATASOURCE_BACKFILL.
    Semántica de seguridad: si no conocemos el origen, no generamos señales.
    """
    return _CHUNK_SOURCE_TO_DATASOURCE.get(source, DATASOURCE_BACKFILL)


# ---------------------------------------------------------------------------
# KafkaOHLCVPublisher
# ---------------------------------------------------------------------------


class KafkaOHLCVPublisher:
    """
    Publica OHLCVChunks del dominio a Kafka en formato EventPayload.

    Kappa architecture
    ------------------
    Tanto backfill como incremental pasan por ohlcv.raw.
    EventPayload.source discrimina el origen para consumers downstream
    sin cambiar el schema del tópico.

    SafeOps
    -------
    publish_chunk() captura cualquier excepción de infraestructura y
    retorna False. Nunca lanza — el caller activa el fallback a Iceberg.
    """

    def __init__(self, producer: KafkaProducerPort) -> None:
        if producer is None:
            raise ValueError("KafkaOHLCVPublisher: producer no puede ser None")
        self._producer = producer

    async def publish_chunk(self, chunk: OHLCVChunk) -> bool:
        """
        Serializa un OHLCVChunk a EventPayload y lo publica a ohlcv.raw.

        Fail-Fast: lanza ValueError si chunk está vacío (contrato violado por caller).
        SafeOps  : cualquier fallo de Kafka/red retorna False sin propagar.

        Parameters
        ----------
        chunk : OHLCVChunk del dominio con velas validadas.

        Returns
        -------
        True si el mensaje fue encolado al broker.
        False si ocurrió cualquier error de infraestructura.
        """
        if chunk.is_empty:
            raise ValueError(
                f"KafkaOHLCVPublisher.publish_chunk: chunk vacío — "
                f"exchange={chunk.exchange} symbol={chunk.symbol} "
                f"timeframe={chunk.timeframe}"
            )

        try:
            await self._producer.start()  # idempotente — guarda _started flag

            # Mapping dominio → wire sin pandas (SRP: publisher no transforma)
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

            # ACL: traducción de vocabulario en la frontera wire
            wire_source: DataSource = _map_source(chunk.source)

            event = EventPayload(
                event_id=str(_uuid.uuid4()),
                exchange=chunk.exchange,
                symbol=chunk.symbol,
                timeframe=chunk.timeframe,
                batch_start_ts=chunk.start_ms or 0,
                bars=bars,
                source=wire_source,
                run_id=chunk.run_id,
            )

            payload_bytes = serialize(event)
            routing_key = make_routing_key(chunk.exchange, chunk.symbol, chunk.timeframe)

            # HEADER_SOURCE lleva el source del dominio (legible para observabilidad).
            # EventPayload.source lleva el wire DataSource (legible para consumers).
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


__all__ = ["KafkaOHLCVPublisher"]
