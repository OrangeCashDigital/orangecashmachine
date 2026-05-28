# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/kucoin_cryptofeed_runner.py
==================================================================

KuCoinCryptofeedRunner — implementación concreta de FeedRunnerProtocol.

KuCoin soporta spot únicamente en cryptofeed (canal: /market/match).
Formato de símbolo: BTC-USDT (sin sufijo -PERP).

Mismos principios que BybitCryptofeedRunner:
SRP / DIP / ACL — ver bybit_cryptofeed_runner.py para la documentación completa.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import KuCoin
from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade
from market_data.ports.inbound.market_data_source import TradeCallback


class KuCoinCryptofeedRunner:
    """Runner WebSocket KuCoin (spot) sobre cryptofeed."""

    _exchange: str = "kucoin"

    def __init__(self) -> None:
        self._on_trade_cb: TradeCallback | None = None

    # ── FeedRunnerProtocol ────────────────────────────────────────────────────

    async def run_until_stopped(
        self,
        symbols: list[str],
        on_trade: TradeCallback,
        stop_event: asyncio.Event,
    ) -> None:
        self._on_trade_cb = on_trade

        handler = FeedHandler()
        handler.add_feed(
            KuCoin(
                symbols=symbols,
                channels=[TRADES],
                callbacks={TRADES: self._translate_and_dispatch},
            )
        )

        logger.info("[kucoin-runner] feed starting | symbols={}", symbols)
        handler.run(start_loop=False, install_signal_handlers=False)

        await stop_event.wait()
        logger.info("[kucoin-runner] feed stopped cleanly.")

    # ── ACL interna ───────────────────────────────────────────────────────────

    async def _translate_and_dispatch(
        self,
        trade: object,  # cryptofeed.types.Trade — confinado aquí
        receipt_timestamp: float,
    ) -> None:
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
            "[kucoin-runner] {} | {} {} @ {}",
            normalized.symbol,
            normalized.side,
            normalized.amount,
            normalized.price,
        )
        if self._on_trade_cb is not None:
            await self._on_trade_cb(normalized)


__all__ = ["KuCoinCryptofeedRunner"]
