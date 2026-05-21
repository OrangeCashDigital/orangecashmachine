# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/fetcher.py
======================================

Puertos de fetching de datos secundarios (trades, derivados).

DIP — Los pipelines de application dependen de estos protocolos;
los adapters inbound (TradesFetcher, FundingRateFetcher, etc.) los
satisfacen estructuralmente via Protocol (sin herencia explícita).

SSOT — Contratos definidos aquí son la única fuente de verdad para
las firmas de fetching. Implementaciones concretas deben coincidir
exactamente con estos contratos.

Principios: DIP · ISP · SSOT · runtime_checkable
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class TradesFetcherPort(Protocol):
    """
    Contrato de fetching incremental de trades tick-by-tick.

    Implementación canónica
    -----------------------
    market_data.adapters.inbound.rest.trades_fetcher.TradesFetcher

    Contrato semántico
    ------------------
    fetch_symbol descarga todos los trades nuevos para ``symbol``
    desde el cursor persistido (Redis → storage → None), persiste
    los resultados y actualiza el cursor.

    Returns int — filas escritas en esta ejecución (0 si no hay nuevos).
    El caller (TradesPipeline) no necesita conocer detalles de paginación
    ni de cursor: son responsabilidad del fetcher (SRP).
    """

    async def fetch_symbol(
        self,
        symbol: str,
        since_ms: Optional[int] = None,
    ) -> int:
        """
        Descarga y persiste todos los trades nuevos para ``symbol``.

        Parameters
        ----------
        symbol   : Par de trading normalizado, e.g. "BTC/USDT".
        since_ms : Override del cursor (backfill manual). None = cursor interno.

        Returns
        -------
        int : filas escritas en esta ejecución. 0 si ya está al día.
        """
        ...


@runtime_checkable
class DerivativesFetcherPort(Protocol):
    """
    Contrato de fetching de métricas de derivados (funding_rate, open_interest).

    Implementaciones canónicas
    --------------------------
    market_data.adapters.inbound.rest.derivatives_fetcher.FundingRateFetcher
    market_data.adapters.inbound.rest.derivatives_fetcher.OpenInterestFetcher

    Contrato semántico
    ------------------
    fetch_symbol obtiene el snapshot más reciente del dataset para ``symbol``,
    aplica dedup por cursor y persiste si es nuevo.

    Returns int — filas escritas (0 si el snapshot ya estaba almacenado).
    """

    async def fetch_symbol(self, symbol: str) -> int:
        """
        Fetcha snapshot del dataset para ``symbol`` y lo persiste si es nuevo.

        Returns
        -------
        int : filas escritas. 0 si snapshot ya almacenado o endpoint no soportado.
        """
        ...
