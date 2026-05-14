# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/ohlcv_batch.py
=================================================

OHLCVBatch — Value Object de un lote de velas OHLCV.

Responsabilidad
---------------
Representar un conjunto ordenado e inmutable de Candle con metadatos
de contexto (exchange, symbol, timeframe). Es la unidad semántica que
viaja desde los adapters inbound hasta los consumers del pipeline.

Principios
----------
DDD       — VO puro: inmutable, definido por valor, sin identidad de negocio
SSOT      — OHLCVBatchReceived (event) usa este tipo; no tuplas crudas
KISS      — solo los campos que el dominio necesita
No pandas — el dominio no depende de infraestructura de análisis
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Import diferido para evitar dependencia circular en import-time
# Candle se importa aquí como forward reference resuelta en runtime
from market_data.domain.value_objects.candle import Candle


# ===========================================================================
# OHLCVBatch — Value Object
# ===========================================================================

@dataclass(frozen=True)
class OHLCVBatch:
    """
    Lote inmutable de velas OHLCV con metadatos de contexto.

    Es la unidad semántica de transferencia entre adapters inbound
    y consumers del pipeline — reemplaza Tuple[CandleTuple, ...].

    Campos
    ------
    exchange    : identificador del exchange ("binance", "okx", …)
    symbol      : par de trading en formato canónico ("BTC/USDT")
    timeframe   : resolución temporal canónica ("1m", "1h", "4h", …)
    candles     : secuencia inmutable de Candle, ordenada por timestamp_ms ASC
    source      : "rest" | "websocket" | "replay"
    run_id      : correlación con LineageTracker (vacío si no aplica)
    chunk_index : índice del chunk en un backfill (0-based)
    total_chunks: total de chunks esperados (None si stream continuo)

    Properties
    ----------
    count        : número de velas en el lote
    is_empty     : True si no hay velas
    is_last_chunk: True si es el último chunk de un backfill
    start_ms     : timestamp_ms de la primera vela (None si vacío)
    end_ms       : timestamp_ms de la última vela (None si vacío)
    valid_count  : número de velas que pasan is_valid()
    """
    exchange:     str                  = ""
    symbol:       str                  = ""
    timeframe:    str                  = ""
    candles:      tuple[Candle, ...]   = field(default_factory=tuple)
    source:       str                  = "rest"
    run_id:       str                  = ""
    chunk_index:  int                  = 0
    total_chunks: Optional[int]        = None

    # ----------------------------------------------------------
    # Properties
    # ----------------------------------------------------------

    @property
    def count(self) -> int:
        """Número de velas en el lote."""
        return len(self.candles)

    @property
    def is_empty(self) -> bool:
        """True si el lote no tiene velas."""
        return len(self.candles) == 0

    @property
    def is_last_chunk(self) -> bool:
        """True si este es el último chunk de un backfill."""
        if self.total_chunks is None:
            return False
        return self.chunk_index >= self.total_chunks - 1

    @property
    def start_ms(self) -> Optional[int]:
        """timestamp_ms de la primera vela. None si el lote está vacío."""
        if self.is_empty:
            return None
        return self.candles[0].timestamp_ms

    @property
    def end_ms(self) -> Optional[int]:
        """timestamp_ms de la última vela. None si el lote está vacío."""
        if self.is_empty:
            return None
        return self.candles[-1].timestamp_ms

    @property
    def valid_count(self) -> int:
        """Número de velas que pasan las invariantes de dominio (is_valid())."""
        return sum(1 for c in self.candles if c.is_valid())

    # ----------------------------------------------------------
    # Factory
    # ----------------------------------------------------------

    @classmethod
    def from_tuples(
        cls,
        tuples:      list[tuple[int, float, float, float, float, float]],
        exchange:    str,
        symbol:      str,
        timeframe:   str,
        source:      str           = "rest",
        run_id:      str           = "",
        chunk_index: int           = 0,
        total_chunks: Optional[int] = None,
    ) -> "OHLCVBatch":
        """
        Construye un OHLCVBatch desde tuplas crudas CCXT.

        Usado en el ACL de adapters inbound (normalizer).
        Fail-Fast: Candle.from_tuple lanza si la tupla es inválida.

        Parameters
        ----------
        tuples : lista de [timestamp_ms, open, high, low, close, volume]
        """
        candles = tuple(Candle.from_tuple(t) for t in tuples)
        return cls(
            exchange     = exchange,
            symbol       = symbol,
            timeframe    = timeframe,
            candles      = candles,
            source       = source,
            run_id       = run_id,
            chunk_index  = chunk_index,
            total_chunks = total_chunks,
        )


# ===========================================================================
# __all__
# ===========================================================================

__all__ = ["OHLCVBatch"]
