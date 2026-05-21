# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/manager.py
==================================================

WebSocketManager — orquestador del ciclo de vida de streams WebSocket.

Responsabilidad única (SRP)
---------------------------
Gestionar el ciclo de vida de TODOS los streams WebSocket activos:
  - Trades stream    (TradesStream)
  - Order book stream (OrderBookStream / CryptofeedOrderBookStream)

No sabe nada de fetching REST, cursores, pipelines ni schedules.

Estado actual
-------------
NOT IMPLEMENTED — interfaz definida, implementación pendiente.

Diseño de la implementación futura
------------------------------------
WebSocketManager será el Composition Root de los streams WebSocket.
Recibirá por inyección de dependencia:
  - event_bus        → publica OrderBookSnapshotReceived, OrderBookDeltaReceived
  - order_book_stream → OrderBookStreamProtocol (impl concreta = cryptofeed)
  - trades_stream     → TradesStreamProtocol    (impl concreta = cryptofeed)

Topología de flujo
------------------
  exchange WS
      ↓ cryptofeed callback
  OrderBookStream.on_snapshot / on_delta
      ↓
  event_bus.publish(OrderBookSnapshotReceived / OrderBookDeltaReceived)
      ↓
  BronzeOrderBookWriter | OrderBookStateManager (consumers)

SafeOps
-------
- stop() NUNCA lanza — captura todos los errores internamente.
- Los streams individuales se paran de forma independiente.
  Si uno falla al parar, los demás continúan (resiliencia parcial).

Principios
----------
SRP    — gestión de ciclo de vida, nada más.
DIP    — depende de OrderBookStreamProtocol, no de la impl concreta.
OCP    — agregar un nuevo tipo de stream no modifica la interfaz.
SafeOps — stop() siempre seguro; degradación parcial en fallos.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from loguru import logger

# ---------------------------------------------------------------------------
# Protocolo del manager
# ---------------------------------------------------------------------------


class WebSocketManagerProtocol(ABC):
    """
    Contrato abstracto del gestor de streams WebSocket.

    Implementaciones concretas previstas
    -------------------------------------
    - CryptofeedWebSocketManager → gestiona cryptofeed.FeedHandler.
    - MockWebSocketManager       → para tests de integración.
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Inicia todos los streams registrados.

        Fail-fast: lanza si algún stream no puede iniciar.
        Los streams parcialmente iniciados se paran antes de relanzar.
        """

    @abstractmethod
    async def stop(self) -> None:
        """
        Para todos los streams activos.

        SafeOps: NUNCA lanza. Para cada stream independientemente.
        """

    @abstractmethod
    async def health_check(self) -> dict[str, bool]:
        """
        Retorna estado de salud de cada stream.

        Returns
        -------
        dict[stream_name, is_running]
        Ejemplo: {"orderbook": True, "trades": False}
        """


# ---------------------------------------------------------------------------
# Stub — NOT IMPLEMENTED
# ---------------------------------------------------------------------------


class WebSocketManager(WebSocketManagerProtocol):
    """
    Stub del gestor de streams WebSocket. NOT IMPLEMENTED.

    Implementación futura: CryptofeedWebSocketManager.
    Ver: https://github.com/bmoscon/cryptofeed

    Este stub hace explícito el contrato del manager.
    health_check() retorna todos los streams como False —
    permite que los callers detecten la indisponibilidad sin crash.
    """

    _log = logger.bind(component="WebSocketManager")

    def __init__(self) -> None:
        self._log.warning(
            "WebSocketManager stub instanciado — streams WebSocket no disponibles. "
            "El sistema opera en modo REST polling. "
            "Implementación futura: CryptofeedWebSocketManager."
        )

    async def start(self) -> None:
        raise NotImplementedError(
            "WebSocketManager no está implementado. "
            "Implementación futura: CryptofeedWebSocketManager. "
            "Usar REST polling (OHLCVFetcher, TradesFetcher) como fallback."
        )

    async def stop(self) -> None:
        # SafeOps — stop() nunca lanza
        self._log.debug("WebSocketManager.stop() llamado en stub — no-op")

    async def health_check(self) -> dict[str, bool]:
        return {
            "orderbook": False,
            "trades": False,
        }

    def __repr__(self) -> str:
        return "WebSocketManager([NOT IMPLEMENTED])"


__all__ = [
    "WebSocketManagerProtocol",
    "WebSocketManager",
]
