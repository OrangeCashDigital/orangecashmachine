# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/kafka_producer.py
=============================================

Puerto OUTBOUND: contrato que usan los inbound adapters para publicar
mensajes a Kafka.

Responsabilidad
---------------
Definir la abstracción que cualquier producer Kafka debe cumplir.
Los adapters de ingestión dependen de este protocolo — nunca de
aiokafka directamente (DIP).

Tópicos canónicos OCM
---------------------
  TOPIC_OHLCV_RAW       = "ohlcv.raw"       — velas crudas CCXT → Bronze
  TOPIC_OHLCV_VALIDATED = "ohlcv.validated"  — post quality-gate → Silver
  TOPIC_OHLCV_FEATURES  = "ohlcv.features"   — features Gold computados
  TOPIC_DLQ             = "dlq.ohlcv"        — Dead Letter Queue

Wire format
-----------
Los mensajes viajan como bytes JSON. La clave (key) es el routing key
canónico: "{exchange}:{symbol}:{timeframe}" — garantiza orden dentro
de una partición para el mismo par/timeframe (SSOT de particionado).

Principios
----------
DIP  — adapters dependen de este Protocol, no de aiokafka
ISP  — solo send_async; flush vive en KafkaProducerPort.flush()
OCP  — KafkaProducerAdapter implementa este contrato sin modificarlo
SafeOps — send_async es fail-soft: no propaga errores al caller
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Constantes de tópicos — SSOT
# ---------------------------------------------------------------------------

TOPIC_OHLCV_RAW:       str = "ohlcv.raw"
TOPIC_OHLCV_VALIDATED: str = "ohlcv.validated"
TOPIC_OHLCV_FEATURES:  str = "ohlcv.features"
TOPIC_DLQ:             str = "dlq.ohlcv"


# ---------------------------------------------------------------------------
# KafkaProducerPort — Protocol (DIP)
# ---------------------------------------------------------------------------

@runtime_checkable
class KafkaProducerPort(Protocol):
    """
    Contrato async de publicación a Kafka.

    Implementado por: KafkaProducerAdapter (infrastructure/kafka/producer.py)
    Usado por      : adapters/inbound/rest/ohlcv_fetcher.py
                     adapters/inbound/websocket/trades_stream.py

    Routing key
    -----------
    key = "{exchange}:{symbol}:{timeframe}"
    Kafka usa la clave para asignar partición — mismo par/timeframe
    siempre va a la misma partición → orden garantizado (SSOT).

    SafeOps
    -------
    send_async() DEBE ser fail-soft:
      - errores de red → loguear + retornar False
      - el adapter inbound no debe saber si Kafka está caído
    """

    async def send_async(
        self,
        topic:   str,
        value:   bytes,
        key:     Optional[bytes] = None,
        headers: Optional[dict]  = None,
    ) -> bool:
        """
        Publica un mensaje a un tópico Kafka.

        Parámetros
        ----------
        topic   : nombre canónico del tópico (usar constantes TOPIC_*)
        value   : payload serializado como bytes (JSON UTF-8)
        key     : routing key como bytes — determina partición
        headers : metadatos opcionales (tracing, schema version)

        Retorna
        -------
        True  — mensaje encolado con éxito
        False — fallo (SafeOps: no lanza)
        """
        ...

    async def flush(self) -> None:
        """
        Fuerza el envío de todos los mensajes pendientes en el buffer.

        Llamar en shutdown para garantizar entrega antes de cerrar.
        SafeOps: no lanza si Kafka no está disponible.
        """
        ...

    async def close(self) -> None:
        """
        Cierra la conexión al broker limpiamente.

        Llamar en shutdown del servicio (lifespan FastAPI o Dagster).
        Idempotente: seguro llamar más de una vez.
        """
        ...


__all__ = [
    "KafkaProducerPort",
    "TOPIC_OHLCV_RAW",
    "TOPIC_OHLCV_VALIDATED",
    "TOPIC_OHLCV_FEATURES",
    "TOPIC_DLQ",
]
