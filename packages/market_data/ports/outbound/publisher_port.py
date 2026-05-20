"""
market_data.ports.outbound.publisher_port
==========================================
Puerto de salida (secondary/driven port): publicación de chunks OHLCV
hacia sistemas downstream (Kafka, Redis Streams, WebSocket, etc.).

Principios aplicados
--------------------
DIP   — application/ depende de esta abstracción, nunca de KafkaOHLCVPublisher.
OCP   — añadir un nuevo canal = nueva clase que implementa el Protocol.
         Sin modificar application/ ni el pipeline.
SSOT  — OHLCVPublisherPort es la única fuente de verdad del contrato de publicación.
NullObject — NullPublisher es el modo degradado declarado.
         Elimina `if publisher is not None:` en toda la cadena de llamadas.
         Ref: Fowler, «Refactoring» §Replace Special Case with Object.

Contratos enforced: BC-04 (ports dependen solo de domain), BC-05, BC-29.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Solo para type checkers — evita import circular en runtime.
    # Los puertos dependen del dominio (BC-04), pero OHLCVChunk
    # vive en domain/models: import seguro.
    from market_data.domain.models.ohlcv import OHLCVChunk

__all__ = ["OHLCVPublisherPort", "NullPublisher"]


@runtime_checkable
class OHLCVPublisherPort(Protocol):
    """
    Contrato de publicación de chunks OHLCV hacia downstream.

    Semántica: fire-and-forget con confirmación asíncrona.
    El pipeline entrega el dato — no gestiona el transporte.

    Implementaciones concretas viven en infrastructure/kafka/,
    infrastructure/redis/, etc. Nunca en application/.
    """

    async def publish_chunk(self, chunk: "OHLCVChunk") -> None:
        """
        Publica un chunk OHLCV procesado.

        El implementador decide el transporte.
        El caller nunca sabe si hay Kafka, Redis o /dev/null.

        Args:
            chunk: Chunk validado listo para downstream.
        Raises:
            PublishError: si el transporte falla sin posibilidad de recuperación.
        """
        ...

    async def close(self) -> None:
        """
        Libera recursos del transporte. Idempotente.
        Llamado por el pipeline al finalizar o en shutdown graceful.
        """
        ...


class NullPublisher:
    """
    NullObject de OHLCVPublisherPort — modo degradado sin Kafka.

    Inyectado por CompositionRoot cuando:
      - KAFKA_ENABLED != true  (flag apagado)
      - El broker no está disponible (fail-soft)

    Por qué NullObject > Optional[publisher]
    -----------------------------------------
    - Elimina ramas condicionales `if self._publisher` en pipeline.
    - La lógica de negocio no sabe si hay Kafka — SRP respetado.
    - Tests de pipeline sin broker: no requieren mocks ni patches.
    - Modo degradado declarado en el tipo, no en comentarios sueltos.

    Por qué aquí (ports/) y no en adapters/ o shared/
    ---------------------------------------------------
    NullPublisher no tiene dependencias de infraestructura.
    Es la implementación «vacía» del contrato del puerto.
    Co-ubicarla con el Protocol documenta el modo degradado
    como parte del contrato, no como detalle de implementación.
    """

    async def publish_chunk(self, chunk: "OHLCVChunk") -> None:
        """No-op intencional. Modo degradado sin Kafka."""

    async def close(self) -> None:
        """No-op intencional. Sin recursos que liberar."""

    def __repr__(self) -> str:
        return "NullPublisher(mode=degraded/no_kafka)"
