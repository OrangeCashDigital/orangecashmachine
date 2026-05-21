# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/exchange.py
=======================================

Puerto abstracto para adapters de exchange.

Responsabilidad
---------------
Declarar el contrato mínimo que cualquier implementación de exchange debe
cumplir. Los pipelines y tasks dependen de esta abstracción — nunca de
CCXTAdapter directamente (DIP · SOLID).

ISP — Interface Segregation Principle
--------------------------------------
fetch_order_book NO es abstracto: solo el pipeline de order book lo
necesita. Declararlo abstracto forzaría a CCXTAdapter (y a todos los mocks)
a implementar un método que la mayoría de consumidores nunca invoca.
Adapters que necesiten order book lo sobreescriben; el resto hereda la
implementación base que lanza NotImplementedError con mensaje claro.

Implementación de referencia
-----------------------------
market_data.adapters.outbound.exchange.ccxt_adapter.CCXTAdapter

Principios: DIP · OCP · ISP · KISS · SafeOps
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExchangeAdapter(ABC):
    """
    Interfaz abstracta para cualquier exchange (ccxt, mock, sandbox).

    Subclases deben implementar los métodos @abstractmethod.
    Python lanza TypeError en instanciación si alguno falta — Fail-Fast
    en construcción, no en runtime (SOLID · LSP).

    SafeOps
    -------
    close() NUNCA debe lanzar excepción en ninguna implementación.
    Las subclases capturan todos los errores internamente en close().
    """

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def connect(self) -> None:
        """
        Inicializa la conexión al exchange.

        Idempotente: llamar dos veces no causa error ni doble conexión.
        """

    @abstractmethod
    async def close(self) -> None:
        """
        Cierra la conexión y libera recursos.

        Idempotente. NUNCA lanza excepción — capturar todos los errores
        internamente. El caller no necesita try/except alrededor de close().
        """

    @abstractmethod
    async def is_healthy(self) -> bool:
        """True si la conexión está activa y puede recibir requests."""

    @abstractmethod
    async def reconnect(self) -> None:
        """
        Fuerza reconexión cerrando el cliente actual y creando uno nuevo.

        Llamado por el fetcher cuando detecta sesión muerta.
        """

    # ------------------------------------------------------------------ #
    # Market data — contrato obligatorio                                   #
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def load_markets(self) -> Dict[str, Any]:
        """
        Carga y retorna el mapa de mercados del exchange.

        El resultado se cachea internamente por el adapter.
        """

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Retorna el ticker actual para el símbolo dado."""

    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[int] = None,
        limit: int = 100,
        market_type: Optional[str] = None,
    ) -> List[List[Any]]:
        """
        Retorna datos OHLCV crudos para el símbolo y timeframe dados.

        Returns
        -------
        List[List[Any]]
            Cada elemento: [timestamp_ms, open, high, low, close, volume]
        """

    @abstractmethod
    async def fetch_trades(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retorna trades recientes para el símbolo dado.

        Usado por TradesPipeline.
        """

    # ------------------------------------------------------------------ #
    # Market data — contrato opcional (ISP)                               #
    # ------------------------------------------------------------------ #

    async def fetch_order_book(
        self,
        symbol: str,
        depth: int = 20,
        market_type: Optional[str] = None,
    ) -> dict:
        """
        Retorna snapshot L2 del order book para el símbolo dado.

        Implementación base: lanza NotImplementedError con mensaje claro.
        Solo los adapters que soporten order book (CCXTAdapter, mocks L2)
        deben sobreescribir este método.

        ISP — No abstracto: la mayoría de pipelines no necesitan order book.
        Forzar implementación en todos los adapters violaría ISP y
        contaminaría los mocks de tests con lógica irrelevante.

        Parameters
        ----------
        symbol      : Par de trading, e.g. "BTC/USDT".
        depth       : Niveles L2 a solicitar (0 = máximo del exchange).
        market_type : Override de tipo de mercado. None = default del adapter.

        Returns
        -------
        dict CCXT crudo: {"bids": [...], "asks": [...], "timestamp": int|None, ...}

        Nota para implementadores
        -------------------------
        Si el exchange no reporta timestamp, usar int(time.time() * 1000)
        como fallback (SafeOps). NUNCA retornar None.
        """
        raise NotImplementedError(
            f"{type(self).__name__} no implementa fetch_order_book. "
            "Solo los adapters con soporte L2 deben implementar este método. "
            "Si tu pipeline necesita order book, usa un adapter que lo soporte."
        )

    # ------------------------------------------------------------------ #
    # Context manager — implementado en base, no sobreescribir            #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> "ExchangeAdapter":
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
