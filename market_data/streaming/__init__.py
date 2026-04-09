"""
market_data/streaming
======================

Primitivas event-driven para ingesta streaming (Fase 2).

Exports públicos
----------------
  EventPayload          — payload normalizado de un batch OHLCV
  OHLCVBar              — una vela individual
  EventHandler          — protocolo de handler (contrato)
  PrefectTriggerHandler — handler que dispara flows Prefect
  EventRouter           — fan-out de eventos a handlers
  StreamingContext      — contexto ligero serializable (sin AppConfig)

Uso mínimo — sin contexto (Fase 1, backward compat)
----------------------------------------------------
    from market_data.streaming import EventRouter, PrefectTriggerHandler
    router = EventRouter(handlers=[PrefectTriggerHandler()])
    router.route(event_payload)

Uso con contexto — Fase 2
--------------------------
    from market_data.streaming import EventRouter, PrefectTriggerHandler, StreamingContext
    ctx    = StreamingContext.from_runtime(runtime_context)
    router = EventRouter(handlers=[PrefectTriggerHandler(context=ctx)])
    router.route(event_payload)
"""

from market_data.streaming.payloads import EventPayload, OHLCVBar
from market_data.streaming.context import StreamingContext
from market_data.streaming.consumer import EventHandler, PrefectTriggerHandler
from market_data.streaming.router import EventRouter

__all__ = [
    "EventPayload",
    "OHLCVBar",
    "StreamingContext",
    "EventHandler",
    "PrefectTriggerHandler",
    "EventRouter",
]
