"""
market_data.trades.sources
============================
Abstracciones e implementaciones de fuentes de trades.

Exports públicos
-----------------
TradesSourceProtocol   — port canónico (DIP boundary)
                         SSOT: ports/inbound/trades_source.py
TradesSourceManager    — árbitro de fuente única (SSOT operacional)
TradesSourceKind       — enum de roles de fuente
WSTradesSource         — stub WebSocket (live stream — pendiente)

NO exportado aquí
------------------
RESTTradesPoller       — adapter, no abstracción
                         importar desde: market_data.adapters.inbound.rest.rest_trades_poller
"""

# Re-exporta el port canónico — consumers solo necesitan importar desde aquí
from market_data.ports.inbound.trades_source import TradesSourceProtocol
from market_data.trades.sources.manager      import TradesSourceManager, TradesSourceKind
from market_data.trades.sources.ws_source    import WSTradesSource

__all__ = [
    "TradesSourceProtocol",
    "TradesSourceManager",
    "TradesSourceKind",
    "WSTradesSource",
]
