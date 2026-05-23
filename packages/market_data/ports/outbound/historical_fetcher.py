# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/historical_fetcher.py
=================================================

Puerto OUTBOUND: fetching histórico paginado de candles OHLCV.

Responsabilidad
---------------
Declarar el contrato mínimo que cualquier fetcher histórico debe cumplir
para ser inyectado en PipelineContext via composition root.

Implementación canónica
-----------------------
market_data.adapters.inbound.rest.ohlcv_fetcher.HistoricalFetcherAsync

Por qué existe este puerto
--------------------------
PipelineContext.fetcher era Any — sin contrato formal, las strategies
dependían implícitamente de la clase concreta HistoricalFetcherAsync.
Con este puerto, application/ depende de la abstracción (DIP) y
HistoricalFetcherAsync satisface estructuralmente el contrato (duck typing
via runtime_checkable, sin herencia explícita).

Contrato semántico
------------------
ensure_exchange  — garantiza que la sesión HTTP/WebSocket está abierta.
                   Idempotente — safe llamar múltiples veces.
fetch_ohlcv      — descarga candles paginando desde since_ms hasta now.
                   Retorna AsyncIterator para que el caller procese chunk
                   a chunk sin acumular en memoria (Kappa — streaming).

Principios
----------
DIP   — application/ depende de este port, no de HistoricalFetcherAsync
ISP   — solo los métodos que BackfillStrategy e IncrementalStrategy usan
OCP   — nuevos adapters (WebSocket, mock) no modifican este contrato
SSOT  — única fuente de verdad del contrato de fetching histórico
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class HistoricalFetcherPort(Protocol):
    """
    Contrato de fetching histórico paginado de candles OHLCV.

    Implementación canónica
    -----------------------
    market_data.adapters.inbound.rest.ohlcv_fetcher.HistoricalFetcherAsync

    Implementación nula (tests)
    ---------------------------
    Cualquier objeto con ensure_exchange() y fetch_ohlcv() — duck typing.

    SafeOps
    -------
    fetch_ohlcv es un AsyncIterator — el caller controla la paginación.
    Errores de red se propagan como excepciones dentro del iterador;
    el caller (BackfillStrategy) los captura y decide el fallback.
    """

    async def ensure_exchange(self) -> None:
        """
        Garantiza que la sesión de exchange está activa.

        Idempotente — safe llamar antes de cada run().
        Lanza ExchangeConnectionError si no puede conectar.
        """
        ...

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since_ms: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Descarga candles OHLCV en chunks paginados.

        Parameters
        ----------
        symbol    : Par canónico ("BTC/USDT").
        timeframe : Resolución canónica ("1m", "1h", …).
        since_ms  : Timestamp de inicio en epoch ms. None = cursor Redis.
        limit     : Máximo de candles por chunk. None = default del exchange.

        Yields
        ------
        pd.DataFrame con columnas [timestamp, open, high, low, close, volume].
        Cada yield es un chunk de la paginación — procesable sin buffer total.

        Raises
        ------
        ChunkFetchError          : error de red en un chunk específico.
        ExchangeConnectionError  : sesión caída — no recuperable sin retry.
        NoDataAvailableError     : el par no tiene datos desde since_ms.
        """
        ...  # type: ignore[misc]  # AsyncIterator en Protocol requiere esta supresión

    async def download_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        """
        Descarga todos los datos OHLCV disponibles desde start_date.

        Returns
        -------
        pd.DataFrame con columnas OHLCV canónicas. DataFrame vacío si no hay datos.
        """
        ...

    async def fetch_chunk(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int],
        limit: int = 500,
        end_ms: Optional[int] = None,
    ) -> List[list]:
        """
        Fetcha un chunk crudo de candles del exchange.

        Parameters
        ----------
        symbol    : Par canónico ("BTC/USDT").
        timeframe : Resolución canónica ("1m", "1h", …).
        since     : Timestamp epoch ms de inicio. None = desde el principio.
        limit     : Máximo de candles por chunk.
        end_ms    : Timestamp epoch ms de fin (backward pagination).

        Returns
        -------
        List[list] — filas crudas [[ts, open, high, low, close, vol], …].
        Lista vacía si no hay datos en el rango.
        """
        ...


__all__ = ["HistoricalFetcherPort"]
