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

from market_data.ports.inbound.market_data_source import MarketDataSource

# Adapter imports son LAZY — cryptofeed/ccxt solo se cargan cuando el
# caller invoca get_adapter_class() en runtime (dentro del event loop).
# Importar en module-level rompe entornos sin cryptofeed instalado.
_ADAPTER_CLASSES: dict[str, str] = {
    "bybit": "market_data.adapters.inbound.bybit_feed_adapter.BybitFeedAdapter",
    "kucoin": "market_data.adapters.inbound.kucoin_feed_adapter.KuCoinFeedAdapter",
}

# ── canonical registry ────────────────────────────────────────────────────────
# key   : lowercase exchange identifier (must match adapter.exchange attribute)
# value : dotted import path de la clase concreta (lazy — no eager import)
# Añadir nuevos exchanges AQUÍ y solo aquí.

# ── import-time integrity guard (eliminado) ──────────────────────────────────
# MOTIVO: instanciar _cls() en import-time importa cryptofeed/ccxt en el grafo
# de módulos antes del event loop, rompiendo el arranque en entornos sin las
# dependencias opcionales instaladas (host dev, CI sin cryptofeed, etc.).
# La verificación de conformance al protocolo ocurre en runtime en get_adapter_class()
# cuando el caller instancia el adapter — Fail-Fast en el momento correcto.
# Ref: SafeOps — no hacer I/O ni instanciar adapters en module-level.


def get_adapter_class(exchange: str) -> type[MarketDataSource]:
    """Return the adapter class for a given exchange name.

    Import lazy — cryptofeed/ccxt se carga solo cuando se llama esta función,
    no en import-time del registry. Fail-Fast: ValueError si exchange desconocido.

    Raises
    ------
    ValueError
        If no adapter is registered for the requested exchange.
    """
    import importlib

    key = exchange.lower().strip()
    dotted = _ADAPTER_CLASSES.get(key)
    if dotted is None:
        available = sorted(_ADAPTER_CLASSES)
        raise ValueError(f"No adapter registered for exchange {exchange!r}. Available: {available}")
    module_path, class_name = dotted.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
