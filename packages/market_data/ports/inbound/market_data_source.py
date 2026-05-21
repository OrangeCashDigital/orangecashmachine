"""
market_data/ports/inbound/market_data_source.py
────────────────────────────────────────────────
Capability contract for all event-driven market data sources (DIP).

Orchestrators and consumers depend ONLY on these abstractions —
never on concrete adapter classes.  Structural subtyping via
runtime_checkable Protocol: adapters need not inherit, only conform.
"""
from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Protocol, runtime_checkable

from market_data.domain.value_objects.normalized_trade import NormalizedTrade

# ── callback type ─────────────────────────────────────────────────────────────
# Any async callable that accepts a NormalizedTrade and returns None.
TradeCallback = Callable[[NormalizedTrade], Coroutine[Any, Any, None]]


@runtime_checkable
class MarketDataSource(Protocol):
    """Capability contract for any event-driven market data source.

    Concrete implementations: BybitFeedAdapter, KuCoinFeedAdapter.
    Checked at runtime via:   isinstance(obj, MarketDataSource)
    """

    exchange: str  # lowercase identifier, e.g. 'bybit'

    def subscribe_trades(
        self,
        symbols: list[str],
        callback: TradeCallback,
    ) -> None:
        """Register canonical symbols and attach an async trade callback.

        Must be called BEFORE start().
        Calling after start() MUST raise RuntimeError (fail-fast).
        """
        ...

    async def start(self) -> None:
        """Connect and begin streaming.

        Long-running coroutine — wrap in asyncio.Task.
        Returns only after stop() is called.
        """
        ...

    async def stop(self) -> None:
        """Signal graceful shutdown.  Idempotent."""
        ...
