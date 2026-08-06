"""
market_data/ports/outbound/external_publisher.py
================================================

Puerto outbound de publicación de eventos canónicos externos (DIP).

Alto nivel: application ExternalIngestionOrchestrator depende de este
puerto (no de KafkaProducerPort, que opera sobre bytes, ni de la
implementación Kafka). El adapter concreto (adapters/outbound) mapea
ExternalMetricEvent → wire payload y lo serializa.

El topic se pasa por invocación porque cada fuente tiene su topic de
destino en config (ExternalSourceConfig.topic).

Principios: DIP · ISP · SRP · async-first
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.domain.events.external_events import ExternalMetricEvent

__all__ = ["ExternalEventPublisherPort"]


@runtime_checkable
class ExternalEventPublisherPort(Protocol):
    """Contrato de publicación de un ExternalMetricEvent al log operacional."""

    async def publish(self, topic: str, event: ExternalMetricEvent) -> None:
        """Publica un evento canónico al topic indicado (at-least-once)."""
        ...
