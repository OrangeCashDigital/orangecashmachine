"""
market_data.ingestion.websocket.orderbook_stream
=================================================
Stream de order book vía WebSocket — NOT IMPLEMENTED.

Responsabilidades futuras
-------------------------
- Subscripción a canal orderbook del exchange
- Mantenimiento de snapshot L2 (bids/asks) en memoria
- Publicación de diffs via asyncio.Queue o callback
"""
from __future__ import annotations


class OrderBookStream:
    """Stream de order book L2 en tiempo real. NOT IMPLEMENTED."""

    def __init__(self) -> None:
        raise NotImplementedError(
            "OrderBookStream no está implementado. "
            "El sistema opera con datos OHLCV via REST."
        )
