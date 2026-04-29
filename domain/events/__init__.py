# -*- coding: utf-8 -*-
"""
domain/events/
==============
Domain events — hechos de negocio que ocurrieron.
Publicados entre bounded contexts vía streaming o callbacks.

Convención de naming: pasado + sustantivo del contexto.
  OrderFilled, PositionOpened, RebalanceTriggered, MarketDataUpdated
"""
