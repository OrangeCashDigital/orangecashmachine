# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/kucoin_feed_adapter.py
────────────────────────────────────────────────────
KuCoinFeedAdapter — implementa MarketDataSource (structural subtyping).

KuCoin soporta solo spot en cryptofeed. Símbolos: BTC-USDT (sin -PERP).

Mismos principios de diseño que BybitFeedAdapter:
SRP / DIP / BC-39 — ver bybit_feed_adapter.py para la documentación completa.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade
from market_data.ports.inbound.market_data_source import TradeCallback

if TYPE_CHECKING:
    from market_data.adapters.inbound.websocket.feed_runner_protocol import FeedRunnerProtocol


class KuCoinFeedAdapter:
    """Adapter limpio para el feed WebSocket de KuCoin (spot)."""

    exchange: str = "kucoin"

    def __init__(self, runner: "FeedRunnerProtocol") -> None:
        self._runner = runner
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
        if self._running:
            raise RuntimeError("KuCoinFeedAdapter: subscribe_trades() llamado después de start().")
        if not symbols:
            raise ValueError("KuCoinFeedAdapter: symbols no puede estar vacío.")

        new = [s for s in symbols if s not in self._symbols]
        self._symbols.extend(new)
        self._callbacks.append(callback)
        logger.debug("[kucoin] subscribed {} symbol(s): {}", len(new), new)

    async def start(self) -> None:
        if not self._symbols:
            raise RuntimeError(
                "KuCoinFeedAdapter: no hay símbolos registrados. Llamar subscribe_trades() antes de start()."
            )
        if self._running:
            logger.warning("[kucoin] start() llamado mientras ya corre — ignorado.")
            return

        self._stop_event = asyncio.Event()
        self._running = True

        logger.info("[kucoin] feed starting | mode=websocket symbols={}", self._symbols)

        await self._runner.run_until_stopped(
            symbols=self._symbols,
            on_trade=self._dispatch,
            stop_event=self._stop_event,
        )

    async def stop(self) -> None:
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
        self._running = False

    # ── fan-out interno ───────────────────────────────────────────────────────

    async def _dispatch(self, trade: NormalizedTrade) -> None:
        results = await asyncio.gather(
            *(cb(trade) for cb in self._callbacks),
            return_exceptions=True,
        )
        for exc in results:
            if isinstance(exc, Exception):
                logger.error("[kucoin] callback error: {}", exc)
