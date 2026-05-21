"""
market_data.adapters.inbound.websocket.trades_stream
=====================================================
DEPRECATED — sustituido por ws_trades_source.py

Razón
-----
TradesStream levantaba NotImplementedError en el constructor.
WSTradesSource implementa el mismo contrato (TradesSourceProtocol)
con fail-soft: stub que emite StopAsyncIteration en lugar de levantar.

Migración
---------
    # antes
    from market_data.adapters.inbound.websocket.trades_stream import TradesStream
    # después
    from market_data.adapters.inbound.websocket.ws_trades_source import WSTradesSource
"""

from market_data.adapters.inbound.websocket.ws_trades_source import WSTradesSource

# Alias de compatibilidad — remover en siguiente iteración
TradesStream = WSTradesSource

__all__ = ["WSTradesSource", "TradesStream"]
