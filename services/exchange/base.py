from __future__ import annotations

"""
services/exchange/base.py
=========================

Interfaz base para adapters de exchange.

Principios
----------
- DIP: los pipelines y tasks dependen de esta abstracción, no de CCXTAdapter
- Testeable: permite mocks sin ccxt ni red
- Completa: cubre todos los métodos que CCXTAdapter expone

CCXTAdapter hereda de ExchangeAdapter — cualquier mock también debe hacerlo.
"""

from typing import Any, Dict, List, Optional


class ExchangeAdapter:
    """
    Interfaz abstracta para cualquier exchange (ccxt, mock, sandbox, etc).

    Todos los métodos lanzan NotImplementedError — las subclases deben
    implementar los que usen. SafeOps: close() nunca debe lanzar excepción.
    """

    # ----------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------

    async def connect(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def is_healthy(self) -> bool:
        raise NotImplementedError

    async def reconnect(self) -> None:
        raise NotImplementedError

    # ----------------------------------------------------------
    # Market data
    # ----------------------------------------------------------

    async def load_markets(self) -> Dict[str, Any]:
        raise NotImplementedError

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    async def fetch_ohlcv(
        self,
        symbol:      str,
        timeframe:   str,
        since:       Optional[int] = None,
        limit:       int = 100,
        market_type: Optional[str] = None,
    ) -> List[List[Any]]:
        raise NotImplementedError

    async def fetch_trades(
        self,
        symbol: str,
        limit:  int = 100,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    # ----------------------------------------------------------
    # Context manager
    # ----------------------------------------------------------

    async def __aenter__(self) -> "ExchangeAdapter":
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()
