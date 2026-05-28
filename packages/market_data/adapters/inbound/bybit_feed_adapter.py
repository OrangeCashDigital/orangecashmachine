# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/bybit_feed_adapter.py
───────────────────────────────────────────────────
BybitFeedAdapter — implementa MarketDataSource (structural subtyping).

Responsabilidad única (SRP)
---------------------------
Gestionar suscripciones, ciclo de vida (start/stop) y fan-out de callbacks.
La traducción vendor → NormalizedTrade es responsabilidad del runner (DIP).

Frontera de dependencias (BC-39)
---------------------------------
Este módulo no importa cryptofeed ni ningún vendor SDK.
Depende de FeedRunnerProtocol (abstracción) inyectado desde el Composition Root.
El runner concreto (BybitCryptofeedRunner) vive en websocket/ y es desconocido aquí.

Lifecycle
─────────
    runner  = BybitCryptofeedRunner()          # Composition Root
    adapter = BybitFeedAdapter(runner=runner)
    adapter.subscribe_trades(symbols=[…], callback=publisher)
    task = asyncio.create_task(adapter.start())   # long-running
    …
    await adapter.stop()
    await task
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from loguru import logger

from market_data.domain.value_objects.normalized_trade import NormalizedTrade
from market_data.ports.inbound.market_data_source import TradeCallback

if TYPE_CHECKING:
    from market_data.adapters.inbound.websocket.feed_runner_protocol import FeedRunnerProtocol


class BybitFeedAdapter:
    """
    Adapter limpio para el feed WebSocket de Bybit.

    No conoce cryptofeed — delega el streaming al runner inyectado.
    """

    exchange: str = "bybit"

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
        """Registra símbolos y callback. Fail-fast si ya está corriendo."""
        if self._running:
            raise RuntimeError(
                "BybitFeedAdapter: subscribe_trades() llamado después de start(). Suscribir antes de iniciar el feed."
            )
        if not symbols:
            raise ValueError("BybitFeedAdapter: symbols no puede estar vacío.")

        new = [s for s in symbols if s not in self._symbols]
        self._symbols.extend(new)
        self._callbacks.append(callback)
        logger.debug("[bybit] subscribed {} symbol(s): {}", len(new), new)

    async def start(self) -> None:
        """Inicia el feed WebSocket. Bloquea hasta que stop() sea llamado."""
        if not self._symbols:
            raise RuntimeError(
                "BybitFeedAdapter: no hay símbolos registrados. Llamar subscribe_trades() antes de start()."
            )
        if self._running:
            logger.warning("[bybit] start() llamado mientras ya corre — ignorado.")
            return

        self._stop_event = asyncio.Event()
        self._running = True

        logger.info("[bybit] feed starting | mode=websocket symbols={}", self._symbols)

        await self._runner.run_until_stopped(
            symbols=self._symbols,
            on_trade=self._dispatch,
            stop_event=self._stop_event,
        )

    async def stop(self) -> None:
        """Señaliza parada limpia. Idempotente."""
        if self._stop_event and not self._stop_event.is_set():
            self._stop_event.set()
        self._running = False

    # ── fan-out interno ───────────────────────────────────────────────────────

    async def _dispatch(self, trade: NormalizedTrade) -> None:
        """Distribuye NormalizedTrade a todos los callbacks registrados."""
        results = await asyncio.gather(
            *(cb(trade) for cb in self._callbacks),
            return_exceptions=True,
        )
        for exc in results:
            if isinstance(exc, Exception):
                logger.error("[bybit] callback error: {}", exc)
