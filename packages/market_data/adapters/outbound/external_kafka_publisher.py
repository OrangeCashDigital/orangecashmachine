# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/external_kafka_publisher.py
=========================================================

ExternalKafkaEventPublisher — publica ExternalMetricEvent en Kafka.

Convierte el evento canónico de dominio en wire payload
(ExternalMetricPayload) y lo serializa vía KafkaProducerPort (DIP).

Kappa headers
-------------
  x-ocm-source  : "replay" (los datos externos periódicos son histórico/backfill)
  x-ocm-domain  : "external"

DIP: recibe KafkaProducerPort por constructor — no instancia el adapter.

Principios: DIP · SRP · SafeOps · Kappa · SSOT
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from shared.kafka.schemas.external import ExternalMetricPayload
from shared.kafka.serializer import make_external_key, serialize
from shared.kafka.topics import (
    HEADER_DOMAIN,
    HEADER_SOURCE,
)

if TYPE_CHECKING:
    from market_data.domain.events.external_events import ExternalMetricEvent
    from market_data.ports.outbound.kafka_producer import KafkaProducerPort

__all__ = ["ExternalKafkaEventPublisher"]


class ExternalKafkaEventPublisher:
    """Productor Kafka de eventos canónicos de fuentes externas."""

    _KAPPA_HEADERS: dict = {
        HEADER_SOURCE: "replay",
        HEADER_DOMAIN: "external",
    }

    def __init__(self, producer: "KafkaProducerPort") -> None:
        self._producer = producer
        self._log = logger.bind(component="ExternalKafkaEventPublisher")

    async def publish(self, topic: str, event: "ExternalMetricEvent") -> None:
        """Serializa y publica un evento canónico al topic indicado.

        SafeOps: captura excepciones — el orquestador no debe morir por
        el publisher.
        """
        try:
            payload = ExternalMetricPayload(
                source_id=event.source_id,
                metric=event.metric,
                symbol=event.symbol,
                timestamp_ms=event.timestamp_ms,
                value=event.value,
                quality_flags=event.quality_flags,
            )
            key = make_external_key(event.source_id, event.metric, event.symbol)
            await self._producer.produce(
                topic=topic,
                value=serialize(payload),
                key=key,
                headers=self._KAPPA_HEADERS,
            )
            self._log.bind(
                source_id=event.source_id,
                metric=event.metric,
                symbol=event.symbol,
            ).debug("external_metric_published | value={}", event.value)
        except Exception as exc:  # noqa: BLE001 — SafeOps, no matar al orquestador
            self._log.bind(error=str(exc)).warning("external_metric_publish_failed")
