# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/feed_runner_protocol.py
==============================================================

FeedRunnerProtocol — contrato de los runners WebSocket vendor-específicos.

Propósito (DIP)
---------------
Los feed adapters (bybit_feed_adapter, kucoin_feed_adapter) dependen de
esta abstracción — nunca de cryptofeed directamente.
La implementación concreta (BybitCryptofeedRunner, etc.) vive en este mismo
paquete websocket/ y es inyectada desde el Composition Root.

Invariante de frontera
----------------------
Este módulo NO importa ningún vendor SDK (cryptofeed, websockets, ccxt).
Es la línea que separa el dominio limpio del mundo vendor.
BC-39 lo garantiza para sus consumers; la ausencia de imports aquí lo
garantiza para el módulo mismo.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

from market_data.ports.inbound.market_data_source import TradeCallback


@runtime_checkable
class FeedRunnerProtocol(Protocol):
    """
    Abstracción del runner WebSocket vendor-específico.

    El runner es el único punto donde viven los imports de cryptofeed.
    Recibe los símbolos y un callback de NormalizedTrade ya traducido,
    y corre hasta que se señala stop_event.

    Implementaciones concretas
    --------------------------
    - BybitCryptofeedRunner   → websocket/bybit_cryptofeed_runner.py
    - KuCoinCryptofeedRunner  → websocket/kucoin_cryptofeed_runner.py
    - MockFeedRunner          → tests/fakes/
    """

    async def run_until_stopped(
        self,
        symbols: list[str],
        on_trade: TradeCallback,
        stop_event: asyncio.Event,
    ) -> None:
        """
        Inicia el stream WebSocket y bloquea hasta que stop_event se active.

        Responsabilidades del runner
        ----------------------------
        1. Construir y arrancar el FeedHandler vendor.
        2. Traducir el tipo vendor → NormalizedTrade (ACL).
        3. Invocar on_trade(normalized) por cada trade recibido.
        4. Parar limpiamente cuando stop_event.is_set().

        Parámetros
        ----------
        symbols    : lista de símbolos en formato del exchange.
        on_trade   : callback que recibe NormalizedTrade (ya traducido).
        stop_event : señal de parada; el runner debe retornar cuando se active.
        """
        ...


__all__ = ["FeedRunnerProtocol"]
