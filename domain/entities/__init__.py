# -*- coding: utf-8 -*-
"""
domain/entities/
================

Entidades de dominio — tipos con identidad propia y ciclo de vida.

Diferencia vs value objects
----------------------------
- Value object: igualdad por valor  (Signal, Timeframe, PositionSnapshot)
- Entity:       igualdad por identidad (OHLCVBar.identity_key, order_id)

Entidades activas
-----------------
  OHLCVBar   — vela OHLCV con identidad (exchange, symbol, timeframe, timestamp)
               Invariantes de dominio validadas en construcción (Fail-Fast).

Entidades pendientes de migración
----------------------------------
  Order      — en trading/execution/order.py (order_id único)
  TradeRecord — en trading/analytics/ (trade_id único)

  Migrarán aquí cuando se centralice el modelo de dominio
  y se elimine el acoplamiento en trading/ — Fase futura del roadmap.

Importar desde aquí (SSOT):
    from domain.entities import OHLCVBar

Principios: DDD · SSOT · Orden Lógico
"""
from domain.entities.ohlcv import OHLCVBar

__all__ = ["OHLCVBar"]
