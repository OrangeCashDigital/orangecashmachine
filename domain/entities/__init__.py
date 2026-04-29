# -*- coding: utf-8 -*-
"""
domain/entities/
================

Entidades de dominio — tipos con identidad propia y ciclo de vida.

Diferencia vs value objects
----------------------------
- Value object: igualdad por valor (Signal, PositionSnapshot)
- Entity:       igualdad por identidad (order_id, trade_id)

Entidades actuales
------------------
  (en trading/execution/order.py)  Order      — orden con order_id único
  (en trading/analytics/)          TradeRecord — trade cerrado con trade_id

Nota: Order y TradeRecord viven en trading/ por cohesión con su BC.
      Se migrarán aquí en una fase posterior del audit si se decide
      centralizar el modelo de dominio.

Principios: DDD · SSOT · Orden Lógico
"""
