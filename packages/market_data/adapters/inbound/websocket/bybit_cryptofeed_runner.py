# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/bybit_cryptofeed_runner.py
=================================================================

BybitCryptofeedRunner — implementación concreta de FeedRunnerProtocol.

Este módulo ES la Anti-Corruption Layer de cryptofeed para Bybit.
Todos los tipos vendor (FeedHandler, Trade, TRADES, Bybit) están
confinados aquí — nunca cruzan esta frontera hacia el dominio.

Principios
----------
SRP   — única responsabilidad: traducir cryptofeed Bybit → NormalizedTrade.
DIP   — implementa FeedRunnerProtocol; el adapter no sabe que este runner existe.
ACL   — trade.price / trade.amount son floats de cryptofeed;
        se convierten a Decimal antes de salir de este módulo.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import Bybit
from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade
from market_data.ports.inbound.market_data_source import TradeCallback


class BybitCryptofeedRunner:
    """
    Runner WebSocket Bybit sobre cryptofeed.

    Ciclo de vida
    -------------
        runner = BybitCryptofeedRunner()
        await runner.run_until_stopped(symbols, on_trade, stop_event)
        # bloquea hasta stop_event.set()
    """

    _exchange: str = "bybit"

    def __init__(self) -> None:
        self._on_trade_cb: TradeCallback | None = None

    # ── FeedRunnerProtocol ────────────────────────────────────────────────────

    async def run_until_stopped(
        self,
        symbols: list[str],
        on_trade: TradeCallback,
        stop_event: asyncio.Event,
    ) -> None:
        """Arranca el FeedHandler y bloquea hasta stop_event."""
        self._on_trade_cb = on_trade

        handler = FeedHandler()
        handler.add_feed(
            Bybit(
                symbols=symbols,
                channels=[TRADES],
                callbacks={TRADES: self._translate_and_dispatch},
            )
        )

        logger.info("[bybit-runner] feed starting | symbols={}", symbols)
        handler.run(start_loop=False, install_signal_handlers=False)

        await stop_event.wait()
        logger.info("[bybit-runner] feed stopped cleanly.")

    # ── ACL interna ───────────────────────────────────────────────────────────

    async def _translate_and_dispatch(
        self,
        trade: object,  # cryptofeed.types.Trade — confinado aquí
        receipt_timestamp: float,
    ) -> None:
        """
        ACL: cryptofeed Trade → NormalizedTrade.

        Todos los accesos a atributos vendor (trade.symbol, trade.price, …)
        ocurren exclusivamente en este método.
        """
        normalized = NormalizedTrade(
            exchange=self._exchange,
            symbol=trade.symbol,  # type: ignore[attr-defined]
            price=Decimal(str(trade.price)),  # type: ignore[attr-defined]
            amount=Decimal(str(trade.amount)),  # type: ignore[attr-defined]
            side=trade.side,  # type: ignore[attr-defined]
            trade_id=str(trade.id),  # type: ignore[attr-defined]
            timestamp=trade.timestamp,  # type: ignore[attr-defined]
            received_at=receipt_timestamp,
        )
        logger.debug(
            "[bybit-runner] {} | {} {} @ {}",
            normalized.symbol,
            normalized.side,
            normalized.amount,
            normalized.price,
        )
        if self._on_trade_cb is not None:
            await self._on_trade_cb(normalized)


__all__ = ["BybitCryptofeedRunner"]
