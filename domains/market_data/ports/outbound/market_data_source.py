# -*- coding: utf-8 -*-
"""
market_data/ports/market_data_source.py
=========================================

Puerto INBOUND (driving side): contrato de fuente de datos de mercado.

Responsabilidad
---------------
Declarar la interfaz abstracta de cualquier fuente de datos OHLCV,
sea REST (CCXT), WebSocket, archivo histórico o replay.

Los use cases de backfill y fetching dependen de este protocolo.
Ningún use case importa CCXT directamente.

Principios
----------
DIP  — application depende de abstracción, no de CCXT
OCP  — binance, okx, kraken, CSV replay implementan sin tocar callers
ISP  — solo fetch_ohlcv; watch_ohlcv en un puerto separado si se requiere
"""
from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable


# (timestamp_ms, open, high, low, close, volume)
RawOHLCV = Sequence[Sequence]


@runtime_checkable
class MarketDataSourcePort(Protocol):
    """
    Contrato de fuente de datos OHLCV (REST/histórico).

    Implementaciones de referencia
    ------------------------------
    market_data.adapters.inbound.rest.ccxt_adapter.CcxtRestAdapter
    market_data.adapters.inbound.stream.replay_adapter.ReplayAdapter (futuro)
    """

    async def fetch_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        since:     int | None = None,
        limit:     int        = 500,
    ) -> RawOHLCV:
        """
        Obtiene velas OHLCV en formato crudo del exchange.

        Returns
        -------
        Lista de [timestamp_ms, open, high, low, close, volume]

        Raises
        ------
        FetcherError        : error de red o exchange
        NoDataAvailableError: el exchange no tiene datos para el rango
        SymbolNotFoundError : el par no existe en este exchange
        """
        ...


__all__ = [
    "RawOHLCV",
    "MarketDataSourcePort",
]
