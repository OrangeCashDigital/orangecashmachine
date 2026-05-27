"""
market_data/adapters/inbound/bybit_feed_adapter.py
───────────────────────────────────────────────────
Anti-Corruption Layer: cryptofeed Bybit WebSocket → NormalizedTrade.

Implements MarketDataSource (ports/inbound/market_data_source.py)
via structural subtyping.  cryptofeed internals NEVER leak past
this boundary.

Lifecycle
─────────
    adapter = BybitFeedAdapter()
    adapter.subscribe_trades(symbols=["BTC-USDT-PERP"], callback=publisher)
    task = asyncio.create_task(adapter.start())   # long-running
    ...
    await adapter.stop()
    await task
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING

from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade
from market_data.ports.inbound.market_data_source import TradeCallback

if TYPE_CHECKING:
    from cryptofeed.types import Trade as CryptoTrade


class BybitFeedAdapter:
    """Adapts cryptofeed Bybit stream to canonical NormalizedTrade callbacks."""

    exchange: str = "bybit"

    def __init__(self) -> None:
        self._callbacks: list[TradeCallback] = []
        self._symbols: list[str] = []
        self._stop_event: asyncio.Event | None = None
        self._running: bool = False

    # ── MarketDataSource protocol ─────────────────────────────────────────────

    def subscribe_trades(
        self,
        symbols: list[str],
        callback: TradeCallback,
    ) -> None:
        """Register symbols and callback.  Fail-fast if already started."""
        if self._running:
            raise RuntimeError(
                "BybitFeedAdapter: subscribe_trades() called after start(). Subscribe before starting the feed."
            )
        if not symbols:
            raise ValueError("BybitFeedAdapter: symbols list must not be empty.")

        new = [s for s in symbols if s not in self._symbols]
        self._symbols.extend(new)
        self._callbacks.append(callback)
        logger.debug("[bybit] subscribed {} symbol(s): {}", len(new), new)

    async def start(self) -> None:
        """Start WebSocket feed.  Blocks until stop() is called."""
        if not self._symbols:
            raise RuntimeError("BybitFeedAdapter: no symbols registered. Call subscribe_trades() before start().")
        if self._running:
            logger.warning("[bybit] start() called while already running — ignored.")
            return

        self._stop_event = asyncio.Event()
        self._running = True

        from cryptofeed import FeedHandler
        from cryptofeed.defines import TRADES
        from cryptofeed.exchanges import Bybit

        handler = FeedHandler()
        handler.add_feed(
            Bybit(
                symbols=self._symbols,
                channels=[TRADES],
                callbacks={TRADES: self._on_trade},
            )
        )

        logger.info(
            "[bybit] feed starting | mode=websocket symbols={}",
            self._symbols,
        )
        handler.run(start_loop=False, install_signal_handlers=False)

        await self._stop_event.wait()
        logger.info("[bybit] feed stopped cleanly.")

    async def stop(self) -> None:
        """Signal graceful shutdown.  Idempotent."""
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
        self._running = False

    # ── internal cryptofeed callback ──────────────────────────────────────────

    async def _on_trade(
        self,
        trade: "CryptoTrade",
        receipt_timestamp: float,
    ) -> None:
        """Translate cryptofeed Trade → NormalizedTrade, fan-out to callbacks."""
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
            "[bybit] {} | {} {} @ {}",
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
                logger.error("[bybit] callback error: {}", exc)
