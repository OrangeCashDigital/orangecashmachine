# -*- coding: utf-8 -*-
"""
market_data/ports/inbound/trades_source.py
==========================================

TradesSource — puerto INBOUND del stream de trades.

Responsabilidad
---------------
Declarar el contrato async que cualquier fuente de trades debe cumplir,
independientemente del transporte (REST polling, WebSocket, replay).

Kappa Architecture
------------------
REST polling, WebSocket y Replay son transportes distintos del mismo
stream semántico de RawTrade. El pipeline downstream (OHLCV builder,
OrderBook builder, feature pipeline) depende de este protocolo —
nunca de una implementación concreta.

    GapAwareStream(WSTradesSource)  ─┐
    TradesBackfillFetcher           ├── TradesSourceProtocol → AsyncIterator[RawTrade]
    GapRecoveryFetcher              |
    ReplayTradesSource (futuro)    ─┘

El stream es la semántica. El transporte es un detalle de implementación.

Implementaciones activas
------------------------
  adapters/inbound/websocket/gap_aware_stream.py      — WS + auto-recovery (fuente canónica)
  adapters/inbound/rest/trades_backfill_fetcher.py    — backfill histórico acotado
  adapters/inbound/rest/gap_recovery_fetcher.py       — gap recovery acotado
  adapters/inbound/replay/replay_trades_source.py     — replay (futuro)

Principios
----------
DIP    — pipeline depende de este Protocol, nunca de implementaciones
OCP    — añadir WebSocket no modifica este contrato ni el pipeline
ISP    — interfaz mínima: iterate + stop + is_running
KISS   — AsyncIterator[RawTrade] es el contrato más simple posible
Kappa  — todos los transportes producen el mismo evento semántico
"""

from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from market_data.domain.value_objects.raw_trade import RawTrade, TradeSource

# ---------------------------------------------------------------------------
# TradesSourceProtocol — contrato async del stream de trades
# ---------------------------------------------------------------------------


@runtime_checkable
class TradesSourceProtocol(Protocol):
    """
    Contrato async mínimo para cualquier fuente de trades.

    El consumer (OHLCV builder, feature pipeline) hace:

        async for trade in source:
            process(trade)

    y no sabe ni le importa si el trade vino de REST, WebSocket o replay.

    Ciclo de vida
    -------------
    1. __aiter__() → inicia la iteración (idempotente)
    2. __anext__() → retorna el siguiente RawTrade o lanza StopAsyncIteration
    3. stop()      → señaliza al source que deje de producir (SafeOps)

    Responsabilidad del implementador
    ----------------------------------
    Cada RawTrade producido DEBE declarar ``source`` explícitamente (SSOT):
      GapAwareStream / WSTradesSource → source=TradeSource.WS
      TradesBackfillFetcher           → source=TradeSource.REST_BACKFILL
      GapRecoveryFetcher              → source=TradeSource.REST_RECOVERY
      ReplayTradesSource (futuro)     → source=TradeSource.REPLAY

    Esto garantiza que los consumers downstream puedan deduplicar y
    priorizar eventos de forma determinista sin depender del transporte.

    SafeOps
    -------
    stop() NUNCA lanza. Implementaciones capturan todos los errores internamente.
    __anext__() puede lanzar StopAsyncIteration para finalizar el stream limpiamente.
    Errores de red/transporte se logean internamente y el stream se reinicia
    (resiliencia) o termina con StopAsyncIteration (fail-soft).
    """

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        """Retorna el iterador async. Idempotente."""
        ...

    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del stream.

        Lanza StopAsyncIteration cuando el stream termina limpiamente
        (fuente agotada, stop() llamado, o error irrecuperable).
        """
        ...

    async def stop(self) -> None:
        """
        Señaliza al source que deje de producir trades.

        SafeOps: NUNCA lanza. Idempotente.
        """
        ...

    @property
    def is_running(self) -> bool:
        """True si el source está activo y produciendo trades."""
        ...

    @property
    def source_id(self) -> str:
        """
        Identificador legible del source para logging y observabilidad.

        Ejemplos: "rest:bybit:BTC/USDT", "ws:bybit:BTC/USDT", "replay:bybit"
        """
        ...


# ---------------------------------------------------------------------------
# OrderBookSourceProtocol — contrato del stream L2
# ---------------------------------------------------------------------------


@runtime_checkable
class OrderBookSourceProtocol(Protocol):
    """
    Contrato async del stream L2 de order book.

    Produce tuplas (snapshot | delta) — el consumer construye el estado
    del libro sobre ellas.

    Implementaciones previstas
    --------------------------
    RESTOrderBookPoller   — snapshot periódico via fetch_order_book() (hoy)
    WebSocketBookStream   — snapshot + delta via cryptofeed (futuro)

    Ciclo de vida
    -------------
    Igual que TradesSourceProtocol — start implícito en __aiter__,
    stop() SafeOps, is_running para health checks.
    """

    def __aiter__(self) -> "AsyncIterator[object]":
        """Retorna el iterador. Produce OrderBookSnapshot | OrderBookDelta."""
        ...

    async def __anext__(self) -> object:
        """
        Retorna el siguiente evento del libro.

        Type: OrderBookSnapshot | OrderBookDelta.
        Lanza StopAsyncIteration al finalizar.
        """
        ...

    async def stop(self) -> None:
        """SafeOps: nunca lanza."""
        ...

    @property
    def is_running(self) -> bool: ...

    @property
    def source_id(self) -> str: ...


__all__ = [
    "TradesSourceProtocol",
    "OrderBookSourceProtocol",
    "TradeSource",
]
