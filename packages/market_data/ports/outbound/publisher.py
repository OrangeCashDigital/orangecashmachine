# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/publisher.py
========================================

Puerto OUTBOUND: publicación de chunks OHLCV a un bus de eventos.

Desacopla application/strategies del formato de serialización concreto
(Kafka + EventPayload + serialize). Las strategies no saben nada de Kafka.

Constantes de source — SSOT de las etiquetas de origen de datos:
  SOURCE_BACKFILL → datos históricos paginados hacia atrás
  SOURCE_LIVE     → datos en tiempo real (streaming / incremental)
  SOURCE_REPLAY   → replay de datos históricos para testing/análisis

Implementaciones:
  infrastructure.kafka.ohlcv_publisher.KafkaOHLCVPublisher
  ports.outbound.publisher.NullOHLCVPublisher  (tests / modo degradado)

Principios: DIP · ISP · SafeOps · SSOT (constantes de source)
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pandas as pd

# ── Constantes de source — SSOT ───────────────────────────────────────────────
SOURCE_BACKFILL: str = "backfill"
SOURCE_LIVE:     str = "live"
SOURCE_REPLAY:   str = "replay"


@runtime_checkable
class OHLCVPublisherPort(Protocol):
    """
    Contrato de publicación de chunks OHLCV a un bus de eventos.

    Abstrae el formato de wire (Kafka, NATS, etc.) de la lógica de negocio.

    SafeOps: publish_chunk() DEBE retornar False en lugar de lanzar.
    El caller decide el fallback (escritura directa a Iceberg en modo degradado).
    """

    async def publish_chunk(
        self,
        exchange_id: str,
        symbol:      str,
        timeframe:   str,
        df:          "pd.DataFrame",
        source:      str,
        run_id:      str = "",
    ) -> bool:
        """
        Publica un chunk OHLCV al bus de eventos.

        Parameters
        ----------
        exchange_id : Identificador canónico del exchange.
        symbol      : Par de trading normalizado, e.g. "BTC/USDT".
        timeframe   : Intervalo temporal canónico, e.g. "1h".
        df          : DataFrame con columnas [timestamp, open, high, low, close, volume].
        source      : Origen del dato: SOURCE_BACKFILL | SOURCE_LIVE | SOURCE_REPLAY.
        run_id      : Correlación externa para lineage. Vacío si no aplica.

        Returns
        -------
        True si el chunk fue publicado con éxito, False si falló (no lanza).
        """
        ...


class NullOHLCVPublisher:
    """
    Implementación vacía de OHLCVPublisherPort.

    Uso: tests, entornos sin Kafka, modo degradado.
    Siempre retorna False — el caller activa el fallback (escritura directa).
    """

    async def publish_chunk(
        self,
        exchange_id: str,
        symbol:      str,
        timeframe:   str,
        df:          "pd.DataFrame",
        source:      str,
        run_id:      str = "",
    ) -> bool:
        return False


__all__ = [
    "OHLCVPublisherPort",
    "NullOHLCVPublisher",
    "SOURCE_BACKFILL",
    "SOURCE_LIVE",
    "SOURCE_REPLAY",
]
