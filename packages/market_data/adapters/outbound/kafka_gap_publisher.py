# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/kafka_gap_publisher.py
=====================================================

Adapter Kafka para el control plane de gaps OHLCV.

Responsabilidad
---------------
Serializar y publicar GapDetectedEvent / GapHealedEvent / GapFailedEvent
en el topic `market.gaps` usando aiokafka.

Arquitectura
------------
Implementa GapEventPublisherPort (DIP):
  RepairStrategy ──▶ GapEventPublisherPort ◀── KafkaGapPublisher

Paralelismo con KafkaTradePublisher
------------------------------------
Mismo patrón de lifecycle (start/stop), mismo serializer JSON,
misma configuración de producer (idempotente, acks=all).
Diferencia: la key es "{exchange_id}:{symbol}:{timeframe}" para
asegurar ordering por par en la misma partición.

Topic: market.gaps
------------------
Particionado por key → ordering garantizado por par de trading.
Consumidores previstos: dashboards de observabilidad, alertas,
gap backlog tracker, replay de auditoría.

SafeOps
-------
publish_gap_event es fail-soft: cualquier excepción de aiokafka
se logea y se descarta — RepairStrategy no debe fallar por Kafka.

Serialización
-------------
JSON puro — sin Avro/Schema Registry en esta iteración.
Los campos de los dataclasses son todos primitivos (str, int, float)
→ json.dumps nativo, sin encoders custom.
Extensión futura: reemplazar el body de _serialize por Avro/protobuf
sin cambiar la interfaz del puerto.

Principios
----------
DIP     — implementa GapEventPublisherPort, sin acoplar al dominio
SRP     — solo serializa y publica, no valida ni transforma
SafeOps — fail-soft en publish, fail-fast en start (sin broker = error claro)
KISS    — JSON plano, sin abstracción prematura
"""

from __future__ import annotations

import dataclasses
import json
import time

from aiokafka import AIOKafkaProducer
from loguru import logger

from market_data.domain.events.gap_events import (
    GapDetectedEvent,
    GapFailedEvent,
    GapHealedEvent,
)
from market_data.ports.outbound.gap_event_publisher import GapEvent

_DEFAULT_TOPIC = "market.gaps"

# Tipo → nombre corto para el campo "event_type" del payload JSON.
# SSOT: un solo lugar donde se define el mapping.
_EVENT_TYPE_NAMES: dict[type, str] = {
    GapDetectedEvent: "gap.detected",
    GapHealedEvent: "gap.healed",
    GapFailedEvent: "gap.failed",
}


class KafkaGapPublisher:
    """
    Sink Kafka para eventos del control plane de gaps.

    Lifecycle
    ---------
    Gestionado por el composition root (OCMContainer / factory):
        publisher = KafkaGapPublisher(bootstrap_servers="localhost:9092")
        await publisher.start()
        # inyectar en RepairStrategy via constructor
        await publisher.stop()  # al shutdown del pipeline
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = _DEFAULT_TOPIC,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Inicializa el producer Kafka.

        Fail-fast: si el broker no está disponible, lanza inmediatamente.
        El composition root es responsable de capturar este error.
        """
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=None,
            key_serializer=None,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        logger.info(
            "[kafka-gap-publisher] started | brokers={} topic={}",
            self._bootstrap_servers,
            self._topic,
        )

    async def stop(self) -> None:
        """Flush y cierra el producer limpiamente."""
        if self._producer is not None:
            await self._producer.stop()
            logger.info("[kafka-gap-publisher] stopped.")

    # ── GapEventPublisherPort ─────────────────────────────────────────────────

    async def publish_gap_event(self, event: GapEvent) -> None:
        """
        Publica un evento de gap en market.gaps.

        SafeOps: fail-soft — loguea y descarta cualquier excepción.
        RepairStrategy no puede fallar por un problema de broker.
        """
        if self._producer is None:
            logger.warning(
                "[kafka-gap-publisher] publish_gap_event llamado antes de start() — descartando evento",
                event_type=type(event).__name__,
            )
            return

        try:
            payload = self._serialize(event)
            key = self._routing_key(event)
            await self._producer.send(
                self._topic,
                value=payload,
                key=key,
            )
            logger.debug(
                "[kafka-gap-publisher] published | topic={} type={} key={}",
                self._topic,
                _EVENT_TYPE_NAMES.get(type(event), "unknown"),
                key.decode(),
            )
        except Exception as exc:
            # SafeOps: nunca propagar al caller (RepairStrategy)
            logger.error(
                "[kafka-gap-publisher] publish failed — descartando | type={} error={}",
                type(event).__name__,
                str(exc),
            )

    # ── serialización ─────────────────────────────────────────────────────────

    def _serialize(self, event: GapEvent) -> bytes:
        """
        JSON plano desde el dataclass.

        Añade `event_type` y `published_at_ms` como campos de envelope
        sin modificar el dataclass de dominio (OCP).
        """
        data = dataclasses.asdict(event)
        data["event_type"] = _EVENT_TYPE_NAMES.get(type(event), "unknown")
        data["published_at_ms"] = int(time.time() * 1000)
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _routing_key(event: GapEvent) -> bytes:
        """
        Clave de particionado Kafka.

        Formato: "{exchange_id}:{symbol}:{timeframe}"
        Garantiza ordering por par de trading en la misma partición.
        GapHealedEvent / GapFailedEvent no tienen estos campos directamente
        — se usa gap_event_id como fallback para mantener ordering de ciclo.
        """
        if isinstance(event, GapDetectedEvent):
            key = f"{event.exchange_id}:{event.symbol}:{event.timeframe}"
        else:
            # GapHealedEvent / GapFailedEvent: ordering por gap_event_id
            key = event.gap_event_id
        return key.encode("utf-8")


# ── NoopGapPublisher — para tests y dry_run ───────────────────────────────────


class NoopGapPublisher:
    """
    Publisher nulo: descarta silenciosamente todos los eventos.

    Uso: tests unitarios, dry_run=True, entornos sin Kafka.
    También útil como default en composition root antes de conectar Kafka.
    """

    async def publish_gap_event(self, event: GapEvent) -> None:  # noqa: D102
        pass

    async def start(self) -> None:  # noqa: D102
        pass

    async def stop(self) -> None:  # noqa: D102
        pass


__all__ = [
    "KafkaGapPublisher",
    "NoopGapPublisher",
]
