"""
market_data.adapters.inbound.websocket
=======================================
Adapters WebSocket inbound.

Exports públicos
-----------------
WSTradesSource       — adapter de trades WS (stub, pendiente implementación)
WSOrderBookStream    — adapter de order book WS (stub, pendiente implementación)
"""

from market_data.adapters.inbound.websocket.ws_trades_source import WSTradesSource

__all__ = ["WSTradesSource"]
