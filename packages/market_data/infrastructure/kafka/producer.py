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

from market_data.domain.exceptions import KafkaProducerError
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
        bootstrap_servers: str = "kafka:9092",
        client_id: str = "ocm-producer",
        compression_type: str = "gzip",
        acks: object = "all",
        max_batch_size: int = 16_384,
        linger_ms: int = 5,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._compression_type = compression_type
        self._acks = acks
        self._max_batch_size = max_batch_size
        self._linger_ms = linger_ms
        self._producer = None
        self._started = False
        self._close_flush_timeout = 10.0
        self._log = logger.bind(
            component="KafkaProducerAdapter",
            broker=bootstrap_servers,
            client_id=client_id,
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
            bootstrap_servers=os.environ.get(KAFKA_BOOTSTRAP_SERVERS, "kafka:9092"),
            client_id=os.environ.get(KAFKA_CLIENT_ID_PRODUCER, "ocm-producer"),
            compression_type=os.environ.get(KAFKA_COMPRESSION_TYPE, "gzip"),
            acks=os.environ.get(KAFKA_ACKS, "all"),
            linger_ms=int(os.environ.get(KAFKA_LINGER_MS, "5")),
            max_batch_size=int(os.environ.get(KAFKA_MAX_BATCH_SIZE, "16384")),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Conecta al broker. Fail-Fast. Idempotente."""
        if self._started:
            return
        from aiokafka import AIOKafkaProducer

        # F-015: enable_idempotence=True — elimina duplicados bajo retry.
        # Verificado compatible con acks="all" (→ -1) por aiokafka
        # producer.py:265-281. El retry del sender es interno (no hay
        # parámetro retries expuesto); retry_backoff_ms=500 ya configurado.
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            compression_type=self._compression_type,
            acks=self._acks,
            max_batch_size=self._max_batch_size,
            linger_ms=self._linger_ms,
            request_timeout_ms=30_000,
            retry_backoff_ms=500,
            enable_idempotence=True,
        )
        if self._producer is None:
            raise RuntimeError("producer_not_initialized")
        await self._producer.start()
        self._started = True
        self._log.info("kafka_producer_started")

    async def close(self) -> None:
        """
        Cierra la conexión limpiamente. Idempotente. SafeOps.

        F-014: antes de stop() se fuerza un flush() explícito para no
        perder mensajes en buffer (linger_ms=5, batching activo). El
        flush lanza TimeoutError si el broker no confirma — se DEBE
        registrar (no es SafeOps de silencio) porque implica pérdida.
        """
        if not self._started or self._producer is None:
            return
        try:
            await self.flush(timeout=self._close_flush_timeout)
        except TimeoutError as exc:
            self._log.error(
                "kafka_close_flush_timeout",
                timeout=self._close_flush_timeout,
                error=str(exc),
            )
        except Exception as exc:
            self._log.warning("kafka_close_flush_error", error=str(exc))
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
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        headers: Optional[dict] = None,
    ) -> bool:
        """
        Publica un mensaje a Kafka.

        Kappa convention: incluir {"x-ocm-source": source} en headers
        para que los consumers puedan filtrar sin deserializar el body.

        SafeOps: captura cualquier excepción y retorna False.

        F-019: la razón de fallo se CLASIFICA en el log (reason=category)
        en vez de colapsar todas las causas en "write_error". El espacio de
        problema (DLQ/gap detection) sigue cubierto por el ADR pendiente de
        B-25 — aquí solo se gana visibilidad, no se decide estrategia.
        """
        from aiokafka.errors import (
            BrokerResponseError,
            KafkaConnectionError,
            KafkaTimeoutError,
        )

        if not self._started or self._producer is None:
            self._log.warning("send_async_skipped — producer not started", topic=topic)
            return False
        try:
            kafka_headers = (
                [(k, v.encode() if isinstance(v, str) else v) for k, v in headers.items()] if headers else []
            )
            await self._producer.send_and_wait(
                topic,
                value=value,
                key=key,
                headers=kafka_headers,
            )
            self._log.bind(topic=topic).debug("kafka_message_sent")
            return True
        except KafkaTimeoutError:
            self._log.bind(topic=topic, reason="broker_timeout").warning("kafka_send_failed")
            return False
        except KafkaConnectionError:
            self._log.bind(topic=topic, reason="connection_error").warning("kafka_send_failed")
            return False
        except BrokerResponseError as exc:
            self._log.bind(
                topic=topic,
                reason="broker_response",
                code=exc.errno,
            ).warning("kafka_send_failed", error=str(exc))
            return False
        except Exception as exc:
            self._log.bind(topic=topic, reason="unknown_error", error=str(exc)).warning("kafka_send_failed")
            return False

    async def flush(self, timeout: float = 10.0) -> None:
        """
        Espera confirmación del broker para todos los mensajes en vuelo.

        Cumple KafkaProducerPort.flush(): NO usa el patrón SafeOps de silenciar
        excepciones — a diferencia de close()/send_async(), un fallo aquí implica
        pérdida potencial de mensajes y debe propagarse.

        Raises
        ------
        TimeoutError : si el broker no confirma dentro de `timeout` segundos.
        """
        if not self._started or self._producer is None:
            return
        import asyncio

        try:
            await asyncio.wait_for(self._producer.flush(), timeout=timeout)
        except asyncio.TimeoutError:
            self._log.error("kafka_flush_timeout", timeout=timeout)
            raise TimeoutError(f"kafka_flush no confirmó en {timeout}s — mensajes en riesgo") from None

    async def produce(
        self,
        topic: str,
        value: bytes,
        key=None,
        headers=None,
    ) -> None:
        """
        Método canónico de KafkaProducerPort.

        Delega a send_async() — adaptación de nombre para cumplir el contrato.

        F-013: antes de volver, verifica la señal real de send_async().
        Si el mensaje no fue confirmado, eleva KafkaProducerError en vez de
        tragarse el fallo. El port lo documenta (Raises KafkaProducerError);
        los callers ya lo capturan con SafeOps — señal visible, sin silencio.
        """
        ok = await self.send_async(topic=topic, value=value, key=key, headers=headers)
        if not ok:
            self._log.bind(topic=topic).critical("kafka_produce_not_confirmed")
            raise KafkaProducerError(f"kafka produce no confirmado: topic={topic} (send_async retornó False)") from None

    async def stop(self) -> None:
        """
        Método canónico de KafkaProducerPort.

        Alias de close() — flush implícito + cierre de conexión.
        Idempotente. SafeOps.
        """
        await self.close()


__all__ = ["KafkaProducerAdapter"]
