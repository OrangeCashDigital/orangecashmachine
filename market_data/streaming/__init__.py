"""
market_data/streaming
======================

Primitivas event-driven para ingesta streaming (Fase 3).

Exports públicos
----------------
  EventPayload          — payload normalizado de un batch OHLCV
  OHLCVBar              — una vela individual
  EventHandler          — protocolo de handler (contrato)
  PrefectTriggerHandler — handler que dispara flows Prefect
  EventRouter           — fan-out de eventos a handlers
  StreamingContext      — contexto ligero serializable (sin AppConfig)
  StreamPublisher       — producer-side: EventPayload → Redis Stream
  StreamSource          — consumer-side: Redis Stream → EventRouter → ACK

Uso completo — Fase 3
----------------------
    from market_data.streaming import (
        EventRouter, PrefectTriggerHandler,
        StreamPublisher, StreamSource,
    )
    from infra.state.factories import build_stream_publisher, build_stream_source

    router = EventRouter(handlers=[PrefectTriggerHandler()])
    pub    = build_stream_publisher(stream_name="ohlcv")
    source = build_stream_source(router=router, stream_name="ohlcv")

    pub.publish(event)      # producer
    source.run()            # consumer loop (blocking)
"""

from market_data.streaming.payloads  import EventPayload, OHLCVBar
from market_data.streaming.context   import StreamingContext
from market_data.streaming.consumer  import EventHandler, PrefectTriggerHandler
from market_data.streaming.router    import EventRouter
from market_data.streaming.publisher import StreamPublisher
from market_data.streaming.source    import StreamSource

__all__ = [
    "EventPayload",
    "OHLCVBar",
    "StreamingContext",
    "EventHandler",
    "PrefectTriggerHandler",
    "EventRouter",
    "StreamPublisher",
    "StreamSource",
]
