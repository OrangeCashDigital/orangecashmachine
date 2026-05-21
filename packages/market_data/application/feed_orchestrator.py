"""
market_data/application/feed_orchestrator.py
─────────────────────────────────────────────
FeedOrchestrator — lifecycle manager for event-driven market data feeds.

Responsibilities
────────────────
  • Build adapters from config (via injected get_adapter callable).
  • Wire publisher as the shared TradeCallback.
  • Start all adapters as concurrent asyncio Tasks.
  • Handle SIGINT / SIGTERM → graceful shutdown sequence.
  • Enforce ingestion_mode:  rest | websocket | dual

Migration phases
────────────────
  rest      Phase 1 — legacy REST polling only (feeds module loaded, not started)
  dual      Phase 2 — both run; compare trade counts for validation
  websocket Phase 3 — WebSocket only; REST deprecated and idle

Dependencies are injected — not instantiated here (DIP).
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from dataclasses import dataclass, field

from loguru import logger

from market_data.ports.inbound.market_data_source import MarketDataSource, TradeCallback


# ── config dataclass ──────────────────────────────────────────────────────────


@dataclass
class ExchangeFeedConfig:
    exchange: str
    symbols: list[str]
    enabled: bool = True


@dataclass
class OrchestratorConfig:
    """Runtime configuration for FeedOrchestrator.

    Populated from Hydra/Pydantic — see core/config/schema.py FeedsConfig.
    """

    ingestion_mode: str  # 'rest' | 'websocket' | 'dual'
    feeds: list[ExchangeFeedConfig] = field(default_factory=list)


# ── orchestrator ──────────────────────────────────────────────────────────────


class FeedOrchestrator:
    """Manages the full lifecycle of event-driven market data feeds.

    Usage
    -----
        cfg = OrchestratorConfig(...)
        orch = FeedOrchestrator(cfg, publisher, get_adapter)
        asyncio.run(orch.run())   # blocks until SIGINT/SIGTERM
    """

    def __init__(
        self,
        config: OrchestratorConfig,
        publisher: TradeCallback,
        get_adapter: Callable[[str], type[MarketDataSource]],
    ) -> None:
        self._config = config
        self._publisher = publisher
        self._get_adapter = get_adapter
        self._adapters: list[MarketDataSource] = []
        self._stop_event = asyncio.Event()

    # ── public entry-point ────────────────────────────────────────────────────

    async def run(self) -> None:
        """Start all feeds and block until shutdown is signalled."""
        mode = self._config.ingestion_mode

        if mode == "rest":
            logger.info(
                "[orchestrator] ingestion_mode=rest — "
                "WebSocket feeds loaded but NOT started. "
                "Switch to 'dual' or 'websocket' to activate."
            )
            return

        if mode not in ("websocket", "dual"):
            raise ValueError(
                f"[orchestrator] Unknown ingestion_mode={mode!r}. Valid values: 'rest', 'websocket', 'dual'."
            )

        self._install_signal_handlers()

        # Build and configure adapters from registry.
        self._adapters = self._build_adapters()

        if not self._adapters:
            logger.warning("[orchestrator] No adapters enabled. Check feeds config — all exchanges may be disabled.")
            return

        # Launch each adapter as an independent Task.
        tasks = [asyncio.create_task(adapter.start(), name=f"feed-{adapter.exchange}") for adapter in self._adapters]
        logger.info(
            "[orchestrator] started | mode={} adapters={}",
            mode,
            [a.exchange for a in self._adapters],
        )

        if mode == "dual":
            logger.warning(
                "[orchestrator] mode=dual — WebSocket + REST both active. "
                "Monitor trade counts in Grafana; switch to 'websocket' "
                "once parity is confirmed."
            )

        # Block until SIGINT/SIGTERM.
        await self._stop_event.wait()

        # Graceful shutdown — stop adapters, then cancel tasks.
        logger.info("[orchestrator] shutdown initiated...")
        for adapter in self._adapters:
            await adapter.stop()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("[orchestrator] shutdown complete.")

    # ── internal helpers ──────────────────────────────────────────────────────

    def _build_adapters(self) -> list[MarketDataSource]:
        """Instantiate and subscribe adapters for all enabled feed configs."""
        adapters: list[MarketDataSource] = []
        for feed_cfg in self._config.feeds:
            if not feed_cfg.enabled:
                logger.debug(
                    "[orchestrator] skipping disabled feed: {}",
                    feed_cfg.exchange,
                )
                continue
            adapter_cls = self._get_adapter(feed_cfg.exchange)
            adapter = adapter_cls()
            adapter.subscribe_trades(symbols=feed_cfg.symbols, callback=self._publisher)
            adapters.append(adapter)
            logger.debug(
                "[orchestrator] adapter registered: {} symbols={}",
                feed_cfg.exchange,
                feed_cfg.symbols,
            )
        return adapters

    def _install_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM → graceful stop (orchestrator owns signals)."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop_event.set)
        logger.debug("[orchestrator] signal handlers installed (SIGINT, SIGTERM).")
