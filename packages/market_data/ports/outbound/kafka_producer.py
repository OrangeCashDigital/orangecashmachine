# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/kafka_producer.py
=============================================

Puerto del productor Kafka — DIP.

OHLCVPipeline (application) emite eventos al bus via este protocolo.
La implementación concreta vive en infrastructure.kafka.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class KafkaProducerPort(Protocol):
    """Contrato mínimo de publicación en Kafka."""

    def produce(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
    ) -> None: ...

    def flush(self, timeout: float = 10.0) -> None: ...
