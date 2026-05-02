"""
market_data.ingestion.websocket.manager
========================================
WebSocket connection manager — NOT IMPLEMENTED.

Responsabilidades futuras
-------------------------
- Ciclo de vida de conexiones WS (connect / reconnect / close)
- Multiplexado de streams sobre una sola conexión (donde lo soporte el exchange)
- Backpressure y buffer de mensajes entrantes
- Integración con AdaptiveThrottle y ResilienceLayer

Referencia
----------
ccxt pro: https://docs.ccxt.com/#/?id=websockets
"""
from __future__ import annotations


class WebSocketManager:
    """Gestiona conexiones WebSocket hacia exchanges. NOT IMPLEMENTED."""

    def __init__(self) -> None:
        raise NotImplementedError(
            "WebSocketManager no está implementado. "
            "Usar REST polling (OHLCVFetcher) para ingestion de datos."
        )
