# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/chunk_converter.py
==============================================

Puerto OUTBOUND: conversión de DataFrame OHLCV a OHLCVChunk del dominio.

Responsabilidad
---------------
Definir el contrato de conversión pandas → dominio.
Desacopla las strategies (application layer) del ACL concreto
(adapters/pandas_to_domain.py) — DIP.

Implementación canónica
-----------------------
market_data.adapters.chunk_converter.PassthroughChunkConverter

Principios: DIP · ISP · SRP · runtime_checkable · Clean Architecture
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

import pandas as pd

from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk


@runtime_checkable
class OHLCVChunkConverterPort(Protocol):
    """
    Contrato de conversión pd.DataFrame → OHLCVChunk.

    El source debe ser un valor canónico de OHLCVSource (SSOT).
    Las strategies pasan SOURCE_BACKFILL o SOURCE_LIVE — nunca
    strings literales.

    Fail-Fast
    ---------
    Las implementaciones deben lanzar DataFrameMappingError si el
    DataFrame tiene columnas faltantes o candles inválidas.
    No silenciar datos corruptos — el caller decide el fallback.
    """

    def to_chunk(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        source: str = "rest",
        run_id: str = "",
        chunk_index: int = 0,
        total_chunks: Optional[int] = None,
    ) -> OHLCVChunk:
        """
        Convierte un DataFrame OHLCV a OHLCVChunk del dominio.

        Parameters
        ----------
        df           : DataFrame con columnas [timestamp, open, high,
                       low, close, volume]. timestamp puede ser
                       pd.Timestamp o int epoch ms.
        exchange     : Identificador del exchange ("bybit", "kucoin", …).
        symbol       : Par en formato canónico ("BTC/USDT").
        timeframe    : Resolución canónica ("1m", "1h", …).
        source       : Origen del chunk — usar OHLCVSource.* (SSOT).
        run_id       : Correlación con LineageTracker.
        chunk_index  : Índice 0-based en un backfill multi-chunk.
        total_chunks : Total de chunks esperados (None = stream continuo).

        Returns
        -------
        OHLCVChunk inmutable con candles validadas.

        Raises
        ------
        DataFrameMappingError : columnas faltantes o candle inválida.
        """
        ...


__all__ = ["OHLCVChunkConverterPort"]
