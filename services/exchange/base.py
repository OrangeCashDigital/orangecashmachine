from __future__ import annotations

"""
services/exchange/base.py
=========================

Interfaz base para adapters de exchange.

Principios
----------
- DIP: los pipelines y tasks dependen de esta abstracción, no de CCXTAdapter
- Testeable: permite mocks sin ccxt ni red
- ABC + @abstractmethod: Python detecta subclases incompletas en instanciación,
  no en runtime al llamar el método

CCXTAdapter hereda de ExchangeAdapter — cualquier mock también debe hacerlo.

SafeOps
-------
- close() está marcado abstracto pero NUNCA debe lanzar excepción en implementaciones.
  Las subclases deben capturar todos los errores internamente.
- __aenter__ / __aexit__ están implementados en la base en términos de connect/close.
  Las subclases no necesitan sobreescribirlos.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExchangeAdapter(ABC):
    """
    Interfaz abstracta para cualquier exchange (ccxt, mock, sandbox, etc).

    Subclases deben implementar todos los métodos @abstractmethod.
    Python lanzará TypeError en instanciación si alguno falta — no en runtime.

    SafeOps: close() nunca debe lanzar excepción en ninguna implementación.
    """

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Inicializa la conexión al exchange. Idempotente."""

    @abstractmethod
    async def close(self) -> None:
        """
        Cierra la conexión. Idempotente.

        NUNCA debe lanzar excepción — capturar todos los errores internamente.
        """

    @abstractmethod
    async def is_healthy(self) -> bool:
        """Devuelve True si la conexión está activa y usable."""

    @abstractmethod
    async def reconnect(self) -> None:
        """Fuerza reconexión cerrando el cliente actual."""

    # ----------------------------------------------------------
    # Market data
    # ----------------------------------------------------------

    @abstractmethod
    async def load_markets(self) -> Dict[str, Any]:
        """Carga y devuelve el mapa de mercados del exchange."""

    @abstractmethod
    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """Devuelve el ticker actual para el símbolo dado."""

    @abstractmethod
    async def fetch_ohlcv(
        self,
        symbol:      str,
        timeframe:   str,
        since:       Optional[int] = None,
        limit:       int = 100,
        market_type: Optional[str] = None,
    ) -> List[List[Any]]:
        """Devuelve datos OHLCV para el símbolo y timeframe dados."""

    @abstractmethod
    async def fetch_trades(
        self,
        symbol: str,
        limit:  int = 100,
    ) -> List[Dict[str, Any]]:
        """Devuelve trades recientes para el símbolo dado."""

    # ----------------------------------------------------------
    # Context manager — implementado en la base, no sobreescribir
    # ----------------------------------------------------------

    async def __aenter__(self) -> "ExchangeAdapter":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()
