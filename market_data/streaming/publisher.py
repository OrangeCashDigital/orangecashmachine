from __future__ import annotations

"""
market_data/streaming/publisher.py
====================================

StreamPublisher — publica EventPayload al Redis Stream.

Responsabilidad
---------------
Serializar un EventPayload a wire format y delegarlo a
RedisStreamPublisher (infra). Punto de entrada del producer-side
del pipeline event-driven.

Wire format
-----------
Todos los valores son strings. bars y meta se serializan a JSON
antes de llamar a publish(), manteniendo el contrato wire format
que from_dict() sabe deserializar.

Principios: SRP · DI · SafeOps · wire/domain separation · observabilidad
"""

import json
from typing import Dict, Optional

from loguru import logger

from market_data.streaming.metrics  import StreamMetrics
from market_data.streaming.payloads import EventPayload


class StreamPublisher:
    """
    Publica EventPayload al Redis Stream.

    Parámetros
    ----------
    publisher   : RedisStreamPublisher — infra ya construida (inyectada).
    stream_name : str                  — nombre lógico para métricas.
    """

    def __init__(
        self,
        publisher,
        stream_name: str = "ohlcv",
    ) -> None:
        self._publisher = publisher
        self._metrics   = StreamMetrics(stream_name=stream_name)
        self._log       = logger.bind(component="StreamPublisher")

    def publish(self, event: EventPayload) -> Optional[str]:
        """
        Serializa el EventPayload a wire format y lo publica.

        Retorna el Redis entry_id si tuvo éxito, None si falla (SafeOps).
        """
        try:
            wire     = self._to_wire(event)
            entry_id = self._publisher.publish(wire)
            if entry_id:
                self._metrics.event_published(exchange=event.exchange)
                self._log.bind(
                    entry_id = entry_id,
                    event_id = event.event_id,
                    exchange = event.exchange,
                    symbol   = event.symbol,
                ).debug("event_published")
            else:
                self._log.bind(
                    event_id = event.event_id,
                ).warning("publish_failed — entry_id is None")
            return entry_id
        except Exception as exc:
            self._log.bind(
                event_id = event.event_id,
                error    = str(exc),
            ).warning("publish_failed — unexpected error")
            return None

    # --------------------------------------------------
    # Internal
    # --------------------------------------------------

    @staticmethod
    def _to_wire(event: EventPayload) -> Dict[str, str]:
        """
        Convierte EventPayload a wire format compatible con Redis Streams.

        Todos los valores son strings. bars y meta se serializan a JSON
        para que from_dict() pueda reconstruirlos correctamente.
        """
        return {
            "event_version":  str(event.event_version),
            "event_id":       str(event.event_id),
            "exchange":       str(event.exchange),
            "symbol":         str(event.symbol),
            "timeframe":      str(event.timeframe),
            "batch_start_ts": str(event.batch_start_ts),
            "bars":           json.dumps([b.to_dict() for b in event.bars]),
            "meta":           json.dumps(event.meta),
        }
