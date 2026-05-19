"""
market_data.trades.sources
============================
Re-exports de conveniencia — apunta a las capas hexagonales correctas.

SSOT de cada símbolo
---------------------
TradesSourceProtocol  → ports/inbound/trades_source.py
TradesSourceManager   → application/source_manager.py
TradesSourceKind      → application/source_manager.py
WSTradesSource        → adapters/inbound/websocket/ws_trades_source.py

Este módulo NO contiene lógica propia.
Existe únicamente para que el código existente no rompa imports
durante la migración. Remover en la siguiente iteración limpia.
"""

from market_data.ports.inbound.trades_source                       import TradesSourceProtocol
from market_data.application.source_manager                        import TradesSourceManager, TradesSourceKind
from market_data.adapters.inbound.websocket.ws_trades_source       import WSTradesSource

__all__ = [
    "TradesSourceProtocol",
    "TradesSourceManager",
    "TradesSourceKind",
    "WSTradesSource",
]
