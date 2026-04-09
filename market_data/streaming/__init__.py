"""
market_data/streaming
======================

Primitivas event-driven para ingesta streaming (Fase 1).

Exports públicos
----------------
  EventPayload        — payload normalizado de un batch OHLCV
  OHLCVBar            — una vela individual
  EventHandler        — protocolo de handler (contrato)
  PrefectTriggerHandler — handler que dispara flows Prefect
  EventRouter         — fan-out de eventos a handlers

Uso mínimo
----------
    from market_data.streaming import EventRouter, PrefectTriggerHandler

    router = EventRouter(handlers=[PrefectTriggerHandler()])
    router.route(event_payload)
"""

from market_data.streaming.payloads import EventPayload, OHLCVBar
from market_data.streaming.consumer import EventHandler, PrefectTriggerHandler
from market_data.streaming.router import EventRouter

__all__ = [
    "EventPayload",
    "OHLCVBar",
    "EventHandler",
    "PrefectTriggerHandler",
    "EventRouter",
]
