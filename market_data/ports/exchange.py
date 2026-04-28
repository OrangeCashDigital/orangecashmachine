# -*- coding: utf-8 -*-
"""
market_data/ports/exchange.py
==============================

Puerto abstracto para adapters de exchange.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de exchange debe cumplir.
Los pipelines y tasks dependen de esta abstracción — nunca de CCXTAdapter
directamente (DIP — SOLID).

Implementación de referencia
-----------------------------
market_data.adapters.outbound.exchange.ccxt_adapter.CCXTAdapter

Principios aplicados
--------------------
DIP   — consumidores importan desde ports/, nunca desde adapters/
OCP   — agregar un nuevo exchange no modifica este contrato
KISS  — interfaz mínima; solo lo que los pipelines realmente necesitan
SafeOps — close() NUNCA debe lanzar excepción en ninguna implementación

Context manager
---------------
__aenter__ / __aexit__ implementados en la base en términos de connect/close.
Las subclases no necesitan sobreescribirlos.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExchangeAdapter(ABC):
    """
    Interfaz abstracta para cualquier exchange (ccxt, mock, sandbox).

    Todas las subclases deben implementar los métodos @abstractmethod.
    Python lanzará TypeError en instanciación si alguno falta — Fail-Fast
    en construcción, no en runtime al llamar el método (SOLID — LSP).

    SafeOps
    -------
    close() NUNCA debe lanzar excepción en ninguna implementación.
    Las subclases deben capturar todos los errores internamente en close().
    """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """
        Inicializa la conexión al exchange.

        Idempotente: llamar dos veces no debe causar error ni doble conexión.
        """

    @abstractmethod
    async def close(self) -> None:
        """
        Cierra la conexión y libera recursos.

        Idempotente. NUNCA lanza excepción — capturar todos los errores
        internamente. El caller no debe necesitar try/except alrededor de close().
        """

    @abstractmethod
    async def is_healthy(self) -> bool:
        """Retorna True si la conexión está activa y puede recibir requests."""

    @abstractmethod
    async def reconnect(self) -> None:
        """
        Fuerza reconexión cerrando el cliente actual y creando uno nuevo.

        Llamado por el fetcher cuando detecta sesión muerta (aiohttp
        "Session is closed" / "Event loop is closed").
        """

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    @abstractmethod
    async def load_markets(self) -> Dict[str, Any]:
        """
        Carga y retorna el mapa de mercados del exchange.

        El resultado se cachea internamente por el adapter.
        La primera llamada establece el universo de símbolos disponibles.
        """

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Retorna el ticker actual para el símbolo dado.

        Usado por validate_exchange_connection para medir latencia y spread.
        """

    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol:      str,
        timeframe:   str,
        since:       Optional[int] = None,
        limit:       int           = 100,
        market_type: Optional[str] = None,
    ) -> List[List[Any]]:
        """
        Retorna datos OHLCV crudos para el símbolo y timeframe dados.

        Parameters
        ----------
        symbol      : Par de trading, e.g. "BTC/USDT".
        timeframe   : Intervalo, e.g. "1m", "1h".
        since       : Timestamp Unix en ms (inicio del rango). None = más reciente.
        limit       : Máximo de velas a retornar.
        market_type : Tipo de mercado override ("spot", "swap"). None = default.

        Returns
        -------
        List[List[Any]]
            Cada elemento: [timestamp_ms, open, high, low, close, volume]
        """

    @abstractmethod
    async def fetch_trades(
        self,
        symbol: str,
        limit:  int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retorna trades recientes para el símbolo dado.

        Usado por TradesPipeline — dominio distinto a OHLCV.
        """

    # ------------------------------------------------------------------
    # Context manager — implementado en base, no sobreescribir
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "ExchangeAdapter":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
