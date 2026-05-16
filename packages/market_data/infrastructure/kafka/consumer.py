# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/consumer.py
=============================================

KafkaConsumerAdapter — implementación concreta de KafkaConsumerPort.

Factories por consumer group
-----------------------------
Cada método de clase `for_*` construye el consumer correcto para su rol.
Los nombres de grupo vienen de kafka_producer.py (SSOT).
Los nombres de env var vienen de env_vars.py (SSOT).

Principios: DIP · SRP · SafeOps · SSOT · Kappa
"""
from __future__ import annotations

import os
from typing import List

from loguru import logger

from market_data.ports.outbound.kafka_consumer import KafkaConsumerPort, KafkaMessage  # noqa
from market_data.ports.outbound.kafka_producer import (
    TOPIC_OHLCV_RAW,
    TOPIC_OHLCV_VALIDATED,
    TOPIC_OHLCV_FEATURES,
    TOPIC_SIGNALS_RAW,
    TOPIC_SIGNALS_APPROVED,
    GROUP_BRONZE_WRITER,
    GROUP_FEATURES,
    GROUP_STRATEGY,
    GROUP_RISK_GATE,
    GROUP_EXECUTION,
    GROUP_PORTFOLIO,
)
from ocm.config.env_vars import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_CONSUMER_SESSION_TIMEOUT_MS,
    KAFKA_CONSUMER_HEARTBEAT_MS,
)


def _broker() -> str:
    """Bootstrap servers desde SSOT. No strings literales aquí."""
    return os.environ.get(KAFKA_BOOTSTRAP_SERVERS, "kafka:9092")

def _offset_reset() -> str:
    return os.environ.get(KAFKA_AUTO_OFFSET_RESET, "earliest")

def _session_timeout() -> int:
    return int(os.environ.get(KAFKA_CONSUMER_SESSION_TIMEOUT_MS, "30000"))

def _heartbeat() -> int:
    return int(os.environ.get(KAFKA_CONSUMER_HEARTBEAT_MS, "10000"))


class KafkaConsumerAdapter:
    """
    Adaptador concreto de KafkaConsumerPort usando aiokafka.

    Semántica at-least-once
    -----------------------
    poll() → procesar → commit(). Sin auto-commit.
    El processor garantiza idempotencia via event_id + SeenFilter.

    Kappa replay
    ------------
    seek_to_beginning() reposiciona al inicio de todas las particiones.
    Llamar después de start() y antes del primer poll().
    """

    def __init__(
        self,
        topics:                List[str],
        group_id:              str,
        bootstrap_servers:     str  = "kafka:9092",
        auto_offset_reset:     str  = "earliest",
        enable_auto_commit:    bool = False,
        max_poll_records:      int  = 500,
        session_timeout_ms:    int  = 30_000,
        heartbeat_interval_ms: int  = 10_000,
    ) -> None:
        self._topics               = topics
        self._group_id             = group_id
        self._bootstrap_servers    = bootstrap_servers
        self._auto_offset_reset    = auto_offset_reset
        self._enable_auto_commit   = enable_auto_commit
        self._max_poll_records     = max_poll_records
        self._session_timeout_ms   = session_timeout_ms
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._consumer             = None
        self._started              = False
        self._log                  = logger.bind(
            component = "KafkaConsumerAdapter",
            group_id  = group_id,
            topics    = topics,
        )

    # ------------------------------------------------------------------
    # Factories — un método por consumer group (SSOT de configuración)
    # ------------------------------------------------------------------

    @classmethod
    def for_bronze_writer(cls) -> "KafkaConsumerAdapter":
        """
        ohlcv.raw → KafkaBronzeWriter.
        Procesa TODOS los sources (live, backfill, replay).
        """
        return cls(
            topics            = [TOPIC_OHLCV_RAW],
            group_id          = GROUP_BRONZE_WRITER,
            bootstrap_servers = _broker(),
            auto_offset_reset = _offset_reset(),
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    @classmethod
    def for_feature_consumer(cls) -> "KafkaConsumerAdapter":
        """
        ohlcv.validated → FeatureConsumer → Gold Iceberg.
        Procesa TODOS los sources.
        """
        return cls(
            topics            = [TOPIC_OHLCV_VALIDATED],
            group_id          = GROUP_FEATURES,
            bootstrap_servers = _broker(),
            auto_offset_reset = _offset_reset(),
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    @classmethod
    def for_strategy_consumer(cls) -> "KafkaConsumerAdapter":
        """
        ohlcv.features → StrategyConsumer.
        SOLO procesa source="live" — filtro en StrategyConsumer,
        no aquí (el header x-ocm-source evita deserializar el body).
        """
        return cls(
            topics            = [TOPIC_OHLCV_FEATURES],
            group_id          = GROUP_STRATEGY,
            bootstrap_servers = _broker(),
            auto_offset_reset = "latest",   # estrategia: solo datos nuevos en vivo
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    @classmethod
    def for_risk_gate(cls) -> "KafkaConsumerAdapter":
        """signals.raw → RiskGateConsumer → signals.approved | rejected."""
        return cls(
            topics            = [TOPIC_SIGNALS_RAW],
            group_id          = GROUP_RISK_GATE,
            bootstrap_servers = _broker(),
            auto_offset_reset = "latest",
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    @classmethod
    def for_execution(cls) -> "KafkaConsumerAdapter":
        """signals.approved → ExecutionConsumer → OMS → exchange."""
        return cls(
            topics            = [TOPIC_SIGNALS_APPROVED],
            group_id          = GROUP_EXECUTION,
            bootstrap_servers = _broker(),
            auto_offset_reset = "latest",
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    @classmethod
    def for_portfolio(cls) -> "KafkaConsumerAdapter":
        """orders.filled → PortfolioConsumer → TradeTracker."""
        from market_data.ports.outbound.kafka_producer import TOPIC_ORDERS_FILLED
        return cls(
            topics            = [TOPIC_ORDERS_FILLED],
            group_id          = GROUP_PORTFOLIO,
            bootstrap_servers = _broker(),
            auto_offset_reset = "earliest",
            session_timeout_ms    = _session_timeout(),
            heartbeat_interval_ms = _heartbeat(),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Conecta al broker. Idempotente."""
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
        if self._consumer is None:
            raise RuntimeError("consumer_not_initialized")
        await self._consumer.start()
        self._started = True
        self._log.info("kafka_consumer_started")

    async def close(self) -> None:
        """Cierra limpiamente. Idempotente. SafeOps."""
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.stop()
            self._started = False
            self._log.info("kafka_consumer_closed")
        except Exception as exc:
            self._log.warning("kafka_consumer_close_error", error=str(exc))

    # ------------------------------------------------------------------
    # KafkaConsumerPort
    # ------------------------------------------------------------------

    async def poll(
        self,
        timeout_ms:  int = 1_000,
        max_records: int = 500,
    ) -> List[KafkaMessage]:
        """Obtiene mensajes. SafeOps: retorna [] si falla."""
        if not self._started or self._consumer is None:
            return []
        try:
            raw = await self._consumer.getmany(
                timeout_ms  = timeout_ms,
                max_records = max_records,
            )
            messages = []
            for _tp, records in raw.items():
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
        """Confirma offset. SafeOps."""
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.commit()
        except Exception as exc:
            self._log.warning("kafka_commit_error", error=str(exc))

    async def seek_to_beginning(self) -> None:
        """
        Kappa replay — reposiciona al inicio de todas las particiones.

        Llamar después de start() y antes del primer poll().
        SafeOps: no lanza si el consumer no está listo.
        """
        if not self._started or self._consumer is None:
            return
        try:
            await self._consumer.seek_to_beginning()
            self._log.info("kafka_consumer_seeked_to_beginning — Kappa replay activo")
        except Exception as exc:
            self._log.warning("kafka_seek_error", error=str(exc))


__all__ = ["KafkaConsumerAdapter"]
