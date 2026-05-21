# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/orderbook_stream.py
===========================================================

Interfaz y stub del stream L2 de order book vía WebSocket.

Estado actual
-------------
NOT IMPLEMENTED — interfaz definida, implementación pendiente.
La implementación concreta usará cryptofeed como backend WebSocket.

Diseño de la implementación futura
------------------------------------
cryptofeed implementará ``OrderBookStreamProtocol`` suscribiéndose
al canal ``l2_book`` de cada exchange. El flujo será:

  cryptofeed callback
      → _on_snapshot() → OrderBookSnapshotReceived → event_bus.publish()
      → _on_delta()    → OrderBookDeltaReceived    → event_bus.publish()

La clase concreta NO se llama desde el dominio — solo desde el
WebSocketManager (Composition Root), que inyecta el event_bus.

Referencia cryptofeed
----------------------
https://github.com/bmoscon/cryptofeed
  feed = cryptofeed.FeedHandler()
  feed.add_feed(exchange, channels=[L2Book], symbols=[...], callbacks={...})

Principios
----------
DIP    — consumers importan OrderBookStreamProtocol, no la impl concreta.
OCP    — agregar un exchange nuevo no modifica el protocolo.
SSOT   — la interfaz define qué es "stream de order book" para el sistema.
SRP    — este módulo declara el contrato; manager.py gestiona el ciclo de vida.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Awaitable

from market_data.domain.value_objects.order_book import (
    OrderBookSnapshot,
    OrderBookDelta,
)

# ---------------------------------------------------------------------------
# Tipos de callback
# ---------------------------------------------------------------------------

SnapshotCallback = Callable[[OrderBookSnapshot], Awaitable[None]]
DeltaCallback = Callable[[OrderBookDelta], Awaitable[None]]


# ---------------------------------------------------------------------------
# Protocolo / Port del stream de order book
# ---------------------------------------------------------------------------


class OrderBookStreamProtocol(ABC):
    """
    Contrato abstracto para cualquier implementación de stream L2.

    Implementaciones concretas previstas
    -------------------------------------
    - CryptofeedOrderBookStream  → usa cryptofeed.FeedHandler (real).
    - MockOrderBookStream        → replay de fixtures (tests).

    Ciclo de vida
    -------------
    1. Instanciar con exchange, symbols y callbacks.
    2. Llamar ``await stream.start()`` para conectar y suscribirse.
    3. Los callbacks se invocan automáticamente por cada evento.
    4. Llamar ``await stream.stop()`` para desconectar limpiamente.

    SafeOps
    -------
    stop() NUNCA debe lanzar excepción — capturar todos los errores
    internamente. El caller (WebSocketManager) no debe necesitar
    try/except alrededor de stop().
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Conecta al exchange e inicia la suscripción al canal l2_book.

        Para cada símbolo suscrito:
          1. Solicita snapshot inicial y llama ``on_snapshot``.
          2. Comienza a recibir deltas y llama ``on_delta`` por cada uno.

        Idempotente: llamar dos veces no debe causar doble suscripción.
        """

    @abstractmethod
    async def stop(self) -> None:
        """
        Desconecta del exchange y libera recursos.

        Idempotente. NUNCA lanza excepción.
        """

    @abstractmethod
    async def is_running(self) -> bool:
        """True si el stream está activo y recibiendo datos."""

    @abstractmethod
    async def subscribe(self, symbol: str) -> None:
        """
        Añade un símbolo a la suscripción en caliente.

        Permite expandir el universo de pares sin reiniciar el stream.
        """

    @abstractmethod
    async def unsubscribe(self, symbol: str) -> None:
        """
        Elimina un símbolo de la suscripción en caliente.

        SafeOps: no lanza si el símbolo no estaba suscrito.
        """


# ---------------------------------------------------------------------------
# Stub — NOT IMPLEMENTED
# ---------------------------------------------------------------------------


class OrderBookStream(OrderBookStreamProtocol):
    """
    Stub de stream L2 de order book. NOT IMPLEMENTED.

    Implementación futura: CryptofeedOrderBookStream.
    Ver: https://github.com/bmoscon/cryptofeed

    Este stub hace explícito el contrato de la interfaz.
    Lanza NotImplementedError en todos los métodos — el caller
    (WebSocketManager) detecta esto en startup y degrada a REST polling.
    """

    def __init__(
        self,
        exchange: str,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        on_delta: DeltaCallback,
        depth: int = 20,
    ) -> None:
        self._exchange = exchange
        self._symbols = symbols
        self._on_snapshot = on_snapshot
        self._on_delta = on_delta
        self._depth = depth

    async def start(self) -> None:
        raise NotImplementedError(
            "OrderBookStream no está implementado. "
            "Implementación futura: CryptofeedOrderBookStream. "
            "Fallback disponible: fetch_order_book() vía REST (ExchangeAdapter)."
        )

    async def stop(self) -> None:
        # stop() NUNCA lanza — SafeOps
        pass

    async def is_running(self) -> bool:
        return False

    async def subscribe(self, symbol: str) -> None:
        raise NotImplementedError("OrderBookStream.subscribe not implemented")

    async def unsubscribe(self, symbol: str) -> None:
        pass  # Fail-soft: no hay nada que desuscribir

    def __repr__(self) -> str:
        return f"OrderBookStream(exchange={self._exchange!r}, symbols={self._symbols!r}, [NOT IMPLEMENTED])"


__all__ = [
    "SnapshotCallback",
    "DeltaCallback",
    "OrderBookStreamProtocol",
    "OrderBookStream",
]
