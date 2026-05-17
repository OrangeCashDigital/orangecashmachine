"""
market_data/ports/outbound/publisher.py
========================================

Puerto OUTBOUND: publicación de chunks OHLCV a un bus de eventos.

Desacopla application/strategies del formato de serialización concreto
(Kafka + EventPayload + serialize). Las strategies no saben nada de Kafka.

Constantes de source — SSOT de las etiquetas de origen de datos.

Implementaciones:
  infrastructure.kafka.ohlcv_publisher.KafkaOHLCVPublisher
  ports.outbound.publisher.NullOHLCVPublisher  (tests / modo degradado)

Principios: DIP · ISP · SafeOps · SSOT · KISS · Clean Architecture
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk, OHLCVSource


# ── Constantes de source — re-exports desde OHLCVSource (SSOT en dominio) ────
# No redefinir aquí — OHLCVSource es la única fuente de verdad.
# Estos aliases se mantienen por compatibilidad con callers existentes.

SOURCE_BACKFILL: str = OHLCVSource.BACKFILL
SOURCE_LIVE:     str = OHLCVSource.LIVE
SOURCE_REPLAY:   str = OHLCVSource.REPLAY


# ── Puerto ───────────────────────────────────────────────────────────────────

@runtime_checkable
class OHLCVPublisherPort(Protocol):
    """
    Contrato de publicación de chunks OHLCV a un bus de eventos.

    Abstrae el formato de wire (Kafka, NATS, etc.) de la lógica de negocio.

    SafeOps
    -------
    publish_chunk() retorna False en lugar de lanzar — nunca propaga
    excepciones al caller. El caller decide si abortar o reintentar.

    En Kappa Architecture el publisher es obligatorio — publisher=None
    es un error de configuración, no un modo degradado válido.
    """

    async def publish_chunk(
        self,
        chunk: OHLCVChunk,
    ) -> bool:
        """
        Publica un chunk OHLCV al bus de eventos.

        Parameters
        ----------
        chunk : OHLCVChunk con candles y metadatos (exchange, symbol,
                timeframe, source, run_id).

        Returns
        -------
        True  — chunk publicado y confirmado por el broker.
        False — fallo transitorio (red, broker no disponible).
               El caller debe loguear y decidir el fallback.
        """


# ── Implementación nula (solo para tests unitarios) ──────────────────────────

class NullOHLCVPublisher:
    """
    Implementación nula de OHLCVPublisherPort — SOLO para tests unitarios.

    Descarta silenciosamente todos los chunks y retorna True (éxito simulado).

    Por qué True y no False
    -----------------------
    False simularía un fallo de Kafka, lo que activaría el código de error
    en las strategies bajo test. Para testear el happy path el publisher
    nulo debe comportarse como éxito. Para testear fallos de publicación,
    usar un mock explícito que retorne False.

    Uso fuera de tests
    ------------------
    No usar en producción ni en entornos con Kafka disponible.
    En Kappa Architecture publisher=None es error de configuración.
    """

    async def publish_chunk(
        self,
        chunk: OHLCVChunk,
    ) -> bool:
        return True  # éxito simulado — solo válido en tests unitarios


__all__ = [
    "OHLCVPublisherPort",
    "NullOHLCVPublisher",
    "OHLCVSource",
    # Aliases de compatibilidad — preferir OHLCVSource.* en código nuevo
    "SOURCE_BACKFILL",
    "SOURCE_LIVE",
    "SOURCE_REPLAY",
]
