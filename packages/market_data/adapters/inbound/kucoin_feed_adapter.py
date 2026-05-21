"""
market_data/adapters/inbound/kucoin_feed_adapter.py
────────────────────────────────────────────────────
Anti-Corruption Layer: cryptofeed KuCoin WebSocket → NormalizedTrade.

KuCoin supports spot only in cryptofeed (channel: /market/match).
Symbols use format: BTC-USDT  (no -PERP suffix).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES
from cryptofeed.exchanges import KuCoin
from loguru import logger

from market_data.ports.inbound.market_data_source import TradeCallback
from market_data.domain.value_objects.normalized_trade import NormalizedTrade

if TYPE_CHECKING:
    from cryptofeed.types import Trade as CryptoTrade


class KuCoinFeedAdapter:
    """Adapts cryptofeed KuCoin stream to canonical NormalizedTrade callbacks."""

    exchange: str = "kucoin"

    def __init__(self) -> None:
        self._callbacks: list[TradeCallback] = []
        self._symbols: list[str] = []
        self._stop_event: asyncio.Event | None = None
        self._running: bool = False

    def subscribe_trades(
        self,
        symbols: list[str],
        callback: TradeCallback,
    ) -> None:
        if self._running:
            raise RuntimeError("KuCoinFeedAdapter: subscribe_trades() called after start().")
        if not symbols:
            raise ValueError("KuCoinFeedAdapter: symbols list must not be empty.")

        new = [s for s in symbols if s not in self._symbols]
        self._symbols.extend(new)
        self._callbacks.append(callback)
        logger.debug("[kucoin] subscribed {} symbol(s): {}", len(new), new)

    async def start(self) -> None:
        if not self._symbols:
            raise RuntimeError("KuCoinFeedAdapter: no symbols registered. Call subscribe_trades() before start().")
        if self._running:
            logger.warning("[kucoin] start() called while already running — ignored.")
            return

        self._stop_event = asyncio.Event()
        self._running = True

        handler = FeedHandler()
        handler.add_feed(
            KuCoin(
                symbols=self._symbols,
                channels=[TRADES],
                callbacks={TRADES: self._on_trade},
            )
        )
        logger.info(
            "[kucoin] feed starting | mode=websocket symbols={}",
            self._symbols,
        )
        handler.run(start_loop=False, install_signal_handlers=False)
        await self._stop_event.wait()
        logger.info("[kucoin] feed stopped cleanly.")

    async def stop(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
        self._running = False

    async def _on_trade(
        self,
        trade: "CryptoTrade",
        receipt_timestamp: float,
    ) -> None:
        normalized = NormalizedTrade(
            exchange=self.exchange,
            symbol=trade.symbol,
            price=Decimal(str(trade.price)),
            amount=Decimal(str(trade.amount)),
            side=trade.side,
            trade_id=str(trade.id),
            timestamp=trade.timestamp,
            received_at=receipt_timestamp,
        )
        logger.debug(
            "[kucoin] {} | {} {} @ {}",
            normalized.symbol,
            normalized.side,
            normalized.amount,
            normalized.price,
        )
        results = await asyncio.gather(
            *(cb(normalized) for cb in self._callbacks),
            return_exceptions=True,
        )
        for exc in results:
            if isinstance(exc, Exception):
                logger.error("[kucoin] callback error: {}", exc)
