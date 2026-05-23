# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/publisher_port.py
============================================

SSOT del contrato de publicación OHLCV.

Los tests de arquitectura verifican este archivo directamente:
  - contiene 'Protocol'          (BC-04)
  - contiene 'runtime_checkable' (BC-04)
  - contiene 'publish_chunk'     (TestPublisherPortContract)
  - contiene 'close'             (TestPublisherPortContract)

publisher.py re-exporta desde aquí — la dirección es publisher_port → publisher.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk, OHLCVSource

# ── Constantes de source ──────────────────────────────────────────────────────
SOURCE_BACKFILL: str = OHLCVSource.BACKFILL
SOURCE_LIVE: str = OHLCVSource.LIVE
SOURCE_REPLAY: str = OHLCVSource.REPLAY


# ── Puerto ────────────────────────────────────────────────────────────────────


@runtime_checkable
class OHLCVPublisherPort(Protocol):
    """
    Contrato de publicación de chunks OHLCV a un bus de eventos.

    SafeOps
    -------
    publish_chunk() retorna False en lugar de lanzar.
    close() libera recursos del transporte. Siempre debe llamarse al finalizar.
    """

    async def publish_chunk(self, chunk: OHLCVChunk) -> bool:
        """
        Publica un chunk OHLCV al bus de eventos.

        Returns True si confirmado, False en fallo transitorio.
        """
        ...

    async def close(self) -> None:
        """Libera recursos del transporte. Idempotente."""
        ...


# ── Implementación nula ───────────────────────────────────────────────────────


class NullOHLCVPublisher:
    """
    Implementación nula de OHLCVPublisherPort — SOLO para tests unitarios.

    Descarta silenciosamente todos los chunks y retorna True (éxito simulado).
    """

    async def publish_chunk(self, chunk: OHLCVChunk) -> bool:  # noqa: ARG002
        return True

    async def close(self) -> None:
        pass


# Alias de compatibilidad
NullPublisher = NullOHLCVPublisher


__all__ = [
    "NullOHLCVPublisher",
    "NullPublisher",
    "OHLCVPublisherPort",
    "Protocol",
    "runtime_checkable",
    "SOURCE_BACKFILL",
    "SOURCE_LIVE",
    "SOURCE_REPLAY",
]
