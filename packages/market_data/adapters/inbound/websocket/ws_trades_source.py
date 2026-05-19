"""
market_data.adapters.inbound.websocket.ws_trades_source
======================================
Fuente WebSocket — stub estructural.

Rol en la arquitectura
-----------------------
- Fuente PRINCIPAL (live stream, baja latencia)
- El manager la selecciona sobre REST cuando está disponible
- Implementa TradesSourceProtocol → sustituible sin tocar consumers

Estado actual
-------------
  NOT_IMPLEMENTED — emite StopAsyncIteration inmediatamente.
  El manager detecta esto y hace fallback a REST.

Principios
----------
- Fail-fast  : constructor valida invariantes
- SafeOps    : stop() nunca lanza
- DIP        : no importa CCXT ni cryptofeed directamente
"""

from __future__ import annotations

import asyncio
from typing import AsyncIterator

from loguru import logger


class WSTradesSource:
    """
    Fuente WebSocket de trades.

    Parámetros
    ----------
    exchange_id  : slug del exchange  (ej. 'bybit', 'kucoin')
    symbol       : par de trading     (ej. 'BTC/USDT')
    market_type  : 'spot' | 'futures' | 'perpetual'
    """

    def __init__(
        self,
        exchange_id: str,
        symbol: str,
        market_type: str = "spot",
    ) -> None:
        # Fail-fast
        if not exchange_id:
            raise ValueError("exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("symbol no puede ser vacío")

        self._exchange_id = exchange_id
        self._symbol = symbol
        self._market_type = market_type
        self._running = False
        self._stop_event = asyncio.Event()
        self._log = logger.bind(
            component="WSTradesSource",
            exchange=exchange_id,
            symbol=symbol,
        )

    # ------------------------------------------------------------------ #
    # TradesSourceProtocol                                                 #
    # ------------------------------------------------------------------ #

    @property
    def source_id(self) -> str:
        return f"ws:{self._exchange_id}:{self._symbol}"

    @property
    def is_running(self) -> bool:
        return self._running

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self):
        # TODO: implementar conexión WS real (cryptofeed / ccxt-pro)
        self._log.warning(
            "WSTradesSource no implementado — StopAsyncIteration inmediato | "
            "source_id={}",
            self.source_id,
        )
        raise StopAsyncIteration

    async def stop(self) -> None:
        """SafeOps: señaliza el fin del stream. Nunca lanza."""
        try:
            self._stop_event.set()
            self._running = False
            self._log.debug("WSTradesSource stopped | source_id={}", self.source_id)
        except Exception:
            pass  # SafeOps

    def __repr__(self) -> str:
        return (
            f"WSTradesSource("
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"running={self._running})"
        )


__all__ = ["WSTradesSource"]
