from __future__ import annotations
from typing import Any, Dict, List


class ExchangeAdapter:
    """
    Interfaz para cualquier exchange (ccxt, mock, etc)
    """

    async def load_markets(self) -> None:
        raise NotImplementedError

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        raise NotImplementedError

    async def fetch_ohlcv(self, symbol: str, timeframe: str) -> List:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

