# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/kafka_producer.py
=============================================

Puerto OUTBOUND del productor Kafka — DIP.

Responsabilidad única
---------------------
Definir el contrato async mínimo que el productor Kafka debe cumplir.
Los topics, headers y grupos viven en infrastructure/kafka/topics.py (SSOT).
Este port NO contiene constantes de infraestructura — SRP.

Nivel de abstracción
--------------------
Este puerto opera sobre bytes — nivel bajo.
Application/ usa OHLCVPublisherPort (alto nivel) — nunca este puerto.
KafkaOHLCVPublisher es el único caller legítimo de KafkaProducerPort.

Por qué async
-------------
El sistema es event-loop first. Un puerto sync en un sistema async rompe
el contrato estructural en tests (isinstance() pasa pero await falla)
y obliga a asyncio.to_thread() ocultando latencia real.

Principios: DIP · ISP · SRP · async-first
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class KafkaProducerPort(Protocol):
    """
    Contrato async mínimo de publicación en Kafka.

    Implementado por: KafkaProducerAdapter (infrastructure/kafka/producer.py)
    Usado por       : KafkaOHLCVPublisher  (infrastructure/kafka/ohlcv_publisher.py)

    Método canónico
    ---------------
    produce() — recibe bytes serializado y routing key bytes.
    El nombre 'produce' es consistente con el vocabulario de Kafka.
    No existe send_async() — cualquier implementación que use ese nombre
    viola el contrato y fallará en runtime.

    SafeOps
    -------
    produce() no lanza si el mensaje fue encolado (best-effort en el producer).
    flush() lanza si el broker no confirma en timeout — llamar en shutdown.
    """

    async def produce(
        self,
        topic:   str,
        value:   bytes,
        key:     Optional[bytes] = None,
        headers: Optional[dict]  = None,
    ) -> None:
        """
        Encola un mensaje para publicación.

        Parameters
        ----------
        topic   : nombre canónico del tópico (desde topics.py — SSOT).
        value   : payload serializado como bytes (JSON UTF-8 via serialize()).
        key     : routing key bytes — determina la partición.
                  None = round-robin. Usar make_routing_key() para OHLCV.
        headers : metadatos del mensaje. Claves desde topics.HEADER_*.

        SafeOps: encola sin esperar confirmación del broker.
        Raises KafkaProducerError solo si el buffer interno está lleno.
        """
        ...

    async def flush(self, timeout: float = 10.0) -> None:
        """
        Espera confirmación del broker para todos los mensajes en vuelo.

        Llamar en shutdown para garantizar at-least-once delivery.

        Raises
        ------
        TimeoutError : si timeout se alcanza antes de confirmar.
                       No silenciar — significa pérdida potencial de mensajes.
        """
        ...

    async def start(self) -> None:
        """Inicializa la conexión al broker. Idempotente."""
        ...

    async def stop(self) -> None:
        """Cierra la conexión limpiamente. flush() implícito. Idempotente."""
        ...


__all__ = ["KafkaProducerPort"]
