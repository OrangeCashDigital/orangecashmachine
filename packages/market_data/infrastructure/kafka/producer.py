# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/producer.py
=============================================

KafkaProducerAdapter — implementación concreta de KafkaProducerPort.

from_env() usa env_vars.py como SSOT de nombres de variables.
Nadie escribe "KAFKA_BOOTSTRAP_SERVERS" directamente aquí.

Principios: DIP · SRP · SafeOps · Resiliencia · SSOT
"""
from __future__ import annotations

import os
from typing import Optional

from loguru import logger

from market_data.ports.outbound.kafka_producer import KafkaProducerPort  # noqa: F401
from ocm.config.env_vars import (
    KAFKA_ACKS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID_PRODUCER,
    KAFKA_COMPRESSION_TYPE,
    KAFKA_LINGER_MS,
    KAFKA_MAX_BATCH_SIZE,
)


class KafkaProducerAdapter:
    """
    Adaptador concreto de KafkaProducerPort usando aiokafka.

    Kappa source header
    -------------------
    send_async() acepta headers dict. Para Kappa, el caller debe incluir:
        headers={"x-ocm-source": payload.source}
    Esto permite que los consumers filtren sin deserializar el body.
    """

    def __init__(
        self,
        bootstrap_servers: str    = "kafka:9092",
        client_id:         str    = "ocm-producer",
        compression_type:  str    = "lz4",
        acks:              object  = "all",
        max_batch_size:    int    = 16_384,
        linger_ms:         int    = 5,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id         = client_id
        self._compression_type  = compression_type
        self._acks              = acks
        self._max_batch_size    = max_batch_size
        self._linger_ms         = linger_ms
        self._producer          = None
        self._started           = False
        self._log               = logger.bind(
            component = "KafkaProducerAdapter",
            broker    = bootstrap_servers,
            client_id = client_id,
        )

    # ------------------------------------------------------------------
    # Factory — SSOT via env_vars.py
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "KafkaProducerAdapter":
        """
        Construye el adapter desde variables de entorno.

        Nombres leídos desde ocm.config.env_vars (SSOT).
        Nunca strings literales aquí.
        """
        return cls(
            bootstrap_servers = os.environ.get(KAFKA_BOOTSTRAP_SERVERS, "kafka:9092"),
            client_id         = os.environ.get(KAFKA_CLIENT_ID_PRODUCER, "ocm-producer"),
            compression_type  = os.environ.get(KAFKA_COMPRESSION_TYPE,   "lz4"),
            acks              = os.environ.get(KAFKA_ACKS,                "all"),
            linger_ms         = int(os.environ.get(KAFKA_LINGER_MS,      "5")),
            max_batch_size    = int(os.environ.get(KAFKA_MAX_BATCH_SIZE,  "16384")),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Conecta al broker. Fail-Fast. Idempotente."""
        if self._started:
            return
        from aiokafka import AIOKafkaProducer
        self._producer = AIOKafkaProducer(
            bootstrap_servers  = self._bootstrap_servers,
            client_id          = self._client_id,
            compression_type   = self._compression_type,
            acks               = self._acks,
            max_batch_size     = self._max_batch_size,
            linger_ms          = self._linger_ms,
            request_timeout_ms = 30_000,
            retry_backoff_ms   = 500,
        )
        await self._producer.start()
        self._started = True
        self._log.info("kafka_producer_started")

    async def close(self) -> None:
        """Cierra la conexión limpiamente. Idempotente. SafeOps."""
        if not self._started or self._producer is None:
            return
        try:
            await self._producer.stop()
            self._started = False
            self._log.info("kafka_producer_closed")
        except Exception as exc:
            self._log.warning("kafka_producer_close_error", error=str(exc))

    # ------------------------------------------------------------------
    # KafkaProducerPort
    # ------------------------------------------------------------------

    async def send_async(
        self,
        topic:   str,
        value:   bytes,
        key:     Optional[bytes] = None,
        headers: Optional[dict]  = None,
    ) -> bool:
        """
        Publica un mensaje a Kafka.

        Kappa convention: incluir {"x-ocm-source": source} en headers
        para que los consumers puedan filtrar sin deserializar el body.

        SafeOps: captura cualquier excepción y retorna False.
        """
        if not self._started or self._producer is None:
            self._log.warning("send_async_skipped — producer not started", topic=topic)
            return False
        try:
            kafka_headers = (
                [(k, v.encode() if isinstance(v, str) else v)
                 for k, v in headers.items()]
                if headers else []
            )
            await self._producer.send_and_wait(
                topic,
                value   = value,
                key     = key,
                headers = kafka_headers,
            )
            self._log.bind(topic=topic).debug("kafka_message_sent")
            return True
        except Exception as exc:
            self._log.bind(topic=topic, error=str(exc)).warning("kafka_send_failed")
            return False

    async def flush(self) -> None:
        """Vacía el buffer interno. SafeOps."""
        if not self._started or self._producer is None:
            return
        try:
            await self._producer.flush()
        except Exception as exc:
            self._log.warning("kafka_flush_error", error=str(exc))


__all__ = ["KafkaProducerAdapter"]
