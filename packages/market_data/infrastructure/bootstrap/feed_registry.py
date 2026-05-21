"""
market_data/infrastructure/bootstrap/feed_registry.py
───────────────────────────────────────────────────────
SSOT registry: exchange name → adapter class.

Add new exchanges HERE and nowhere else.
The orchestrator reads this registry — it never imports adapters directly.
Lives in infrastructure/bootstrap because mapping exchange → concrete adapter
is wiring / composition root concern (avoids DIP leak from application layer).
"""
from __future__ import annotations

from typing import Final

from market_data.adapters.inbound.bybit_feed_adapter import BybitFeedAdapter
from market_data.adapters.inbound.kucoin_feed_adapter import KuCoinFeedAdapter
from market_data.ports.inbound.market_data_source import MarketDataSource

# ── canonical registry ────────────────────────────────────────────────────────
# key   : lowercase exchange identifier (must match adapter.exchange attribute)
# value : concrete adapter class (implements MarketDataSource protocol)

_ADAPTER_REGISTRY: Final[dict[str, type[MarketDataSource]]] = {
    "bybit": BybitFeedAdapter,
    "kucoin": KuCoinFeedAdapter,
}

# ── import-time integrity guard (fail-fast) ───────────────────────────────────
for _exchange, _cls in _ADAPTER_REGISTRY.items():
    assert isinstance(_cls(), MarketDataSource), (
        f"_ADAPTER_REGISTRY integrity error: "
        f"{_cls.__name__} does not satisfy MarketDataSource protocol."
    )


def get_adapter_class(exchange: str) -> type[MarketDataSource]:
    """Return the adapter class for a given exchange name.

    Raises
    ------
    ValueError
        If no adapter is registered for the requested exchange.
    """
    key = exchange.lower().strip()
    try:
        return _ADAPTER_REGISTRY[key]
    except KeyError:
        available = sorted(_ADAPTER_REGISTRY)
        raise ValueError(
            f"No adapter registered for exchange {exchange!r}. "
            f"Available: {available}"
        ) from None
