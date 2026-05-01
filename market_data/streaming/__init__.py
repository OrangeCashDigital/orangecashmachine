# -*- coding: utf-8 -*-
"""
market_data/streaming
======================

Primitivas event-driven para ingesta streaming.

Exports públicos
----------------
  EventPayload     — payload normalizado de un batch OHLCV
  OHLCVBar         — una vela individual
  EventHandler     — protocolo de handler (contrato, structural subtyping)
  DispatchHandler  — handler que dispara el orquestador activo (Dagster v0.3+)
  EventRouter      — fan-out de eventos a múltiples handlers
  StreamingContext — contexto ligero serializable (sin AppConfig)
  StreamPublisher  — producer-side: EventPayload → Redis Stream
  StreamSource     — consumer-side: Redis Stream → EventRouter → ACK


Uso completo
------------
    from market_data.streaming import (
        EventRouter, DispatchHandler,
        StreamPublisher, StreamSource,
    )

    router = EventRouter(handlers=[DispatchHandler()])
    pub    = build_stream_publisher(stream_name="ohlcv")
    source = build_stream_source(router=router, stream_name="ohlcv")

    pub.publish(event)   # producer
    source.run()         # consumer loop (blocking)

Principios: SRP · DIP · OCP
"""

from market_data.streaming.payloads  import EventPayload, OHLCVBar
from market_data.streaming.context   import StreamingContext
from market_data.streaming.consumer  import EventHandler, DispatchHandler
from market_data.streaming.router    import EventRouter
from market_data.streaming.publisher import StreamPublisher
from market_data.streaming.source    import StreamSource

__all__ = [
    # Activos
    "EventPayload",
    "OHLCVBar",
    "StreamingContext",
    "EventHandler",
    "DispatchHandler",
    "EventRouter",
    "StreamPublisher",
    "StreamSource",
]
