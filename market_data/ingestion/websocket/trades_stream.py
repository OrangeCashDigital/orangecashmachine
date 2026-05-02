"""
market_data.ingestion.websocket.trades_stream
==============================================
Stream de trades individuales vía WebSocket — NOT IMPLEMENTED.

Responsabilidades futuras
-------------------------
- Subscripción a canal de trades del exchange
- Parsing de mensajes a TradeRecord (domain model)
- Publicación via asyncio.Queue para consumers downstream
"""
from __future__ import annotations


class TradesStream:
    """Stream de trades individuales en tiempo real. NOT IMPLEMENTED."""

    def __init__(self) -> None:
        raise NotImplementedError(
            "TradesStream no está implementado. "
            "Usar TradesFetcher (REST) para datos de trades históricos."
        )
