# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/consumer.py
=============================================

KafkaConsumerAdapter — implementación concreta de KafkaConsumerPort.

Responsabilidad
---------------
Consumir mensajes de Kafka usando aiokafka.AIOKafkaConsumer.
Implementa KafkaConsumerPort — los stream processors solo conocen
el port, nunca esta clase concreta (DIP).

Semántica at-least-once
-----------------------
poll() obtiene mensajes → processor los procesa → commit() confirma.
Si el proceso muere entre poll y commit, los mensajes se reprocesarán.
El processor garantiza idempotencia via event_id dedup.

Consumer groups
---------------
Cada grupo de consumers comparte la carga de particiones.
Un grupo por rol funcional:
  "ocm-bronze-writer"   — escribe a Iceberg Bronze
  "ocm-quality-gate"    — valida y publica a ohlcv.validated

Principios: DIP · SRP · SafeOps · Resiliencia · KISS
"""
from __future__ import annotations

import os
from typing import List, Optional

from loguru import logger

from market_data.ports.outbound.kafka_consumer import KafkaConsumerPort, KafkaMessage  # noqa: F401


class KafkaConsumerAdapter:
    """
    Adaptador concreto de KafkaConsumerPort usando aiokafka.

    Parámetros
    ----------
    topics            : lista de tópicos a suscribirse
    group_id          : consumer group — determina offset tracking
    bootstrap_servers : host:port del broker
    auto_offset_reset : "earliest" (replay desde inicio) | "latest" (solo nuevos)
    enable_auto_commit: False — commit manual para at-least-once
    max_poll_records  : máximo de mensajes por poll
    """

    def __init__(
        self,
        topics:             List[str],
        group_id:           str,
        bootstrap_servers:  str  = "kafka:9092",
        auto_offset_reset:  str  = "earliest",
        enable_auto_commit: bool = False,
        max_poll_records:   int  = 500,
        session_timeout_ms: int  = 30_000,
        heartbeat_interval_ms: int = 10_000,
    ) -> None:
        self._topics              = topics
        self._group_id            = group_id
        self._bootstrap_servers   = bootstrap_servers
        self._auto_offset_reset   = auto_offset_reset
        self._enable_auto_commit  = enable_auto_commit
        self._max_poll_records    = max_poll_records
        self._session_timeout_ms  = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._consumer            = None
        self._started             = False
        self._log                 = logger.bind(
            component  = "KafkaConsumerAdapter",
            group_id   = group_id,
            topics     = topics,
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def for_bronze_writer(cls) -> "KafkaConsumerAdapter":
        """Consumer para el BronzeWriter — consume ohlcv.raw."""
        from market_data.ports.outbound.kafka_producer import TOPIC_OHLCV_RAW
        return cls(
            topics            = [TOPIC_OHLCV_RAW],
            group_id          = "ocm-bronze-writer",
            bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            auto_offset_reset = "earliest",
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Conecta al broker y arranca el consumer. Idempotente."""
        if self._started:
            return
        from aiokafka import AIOKafkaConsumer
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers     = self._bootstrap_servers,
            group_id              = self._group_id,
            auto_offset_reset     = self._auto_offset_reset,
            enable_auto_commit    = self._enable_auto_commit,
            max_poll_records      = self._max_poll_records,
            session_timeout_ms    = self._session_timeout_ms,
            heartbeat_interval_ms = self._heartbeat_interval_ms,
        )
        await self._consumer.start()
        self._started = True
        self._log.info("kafka_consumer_started")

    async def close(self) -> None:
        """Cierra la conexión limpiamente. Idempotente. SafeOps."""
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.stop()
            self._started = False
            self._log.info("kafka_consumer_closed")
        except Exception as exc:
            self._log.warning("kafka_consumer_close_error", error=str(exc))

    # ------------------------------------------------------------------
    # KafkaConsumerPort — implementación del contrato
    # ------------------------------------------------------------------

    async def poll(
        self,
        timeout_ms:  int = 1_000,
        max_records: int = 500,
    ) -> List[KafkaMessage]:
        """
        Obtiene mensajes del broker. SafeOps: retorna [] si falla.
        """
        if not self._started or self._consumer is None:
            return []
        try:
            raw = await self._consumer.getmany(
                timeout_ms  = timeout_ms,
                max_records = max_records,
            )
            messages = []
            for tp, records in raw.items():
                for r in records:
                    messages.append(KafkaMessage(
                        topic     = r.topic,
                        partition = r.partition,
                        offset    = r.offset,
                        key       = r.key,
                        value     = r.value,
                        timestamp = r.timestamp,
                        headers   = tuple(r.headers) if r.headers else (),
                    ))
            return messages
        except Exception as exc:
            self._log.warning("kafka_poll_error", error=str(exc))
            return []

    async def commit(self) -> None:
        """Confirma offset del último mensaje procesado. SafeOps."""
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.commit()
        except Exception as exc:
            self._log.warning("kafka_commit_error", error=str(exc))

    async def seek_to_beginning(self) -> None:
        """Reposiciona al inicio para replay Kappa. SafeOps."""
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.seek_to_beginning()
            self._log.info("kafka_consumer_seeked_to_beginning")
        except Exception as exc:
            self._log.warning("kafka_seek_error", error=str(exc))


__all__ = ["KafkaConsumerAdapter"]
