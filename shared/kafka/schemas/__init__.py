# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/
=====================

Wire payloads por dominio. Cada módulo es independiente.

Importar siempre por submódulo explícito — el __init__ NO re-exporta
para evitar acoplar al consumidor a todos los dominios:

    from shared.kafka.schemas.ohlcv     import EventPayload
    from shared.kafka.schemas.signals   import SignalPayload
    from shared.kafka.schemas.orders    import OrderFilledPayload, OrderRejectedPayload
    from shared.kafka.schemas.positions import PositionOpenedPayload, PositionClosedPayload

Nombres correctos por módulo
-----------------------------
  ohlcv.py        → EventPayload, KafkaOHLCVBar
  signals.py      → SignalPayload, ApprovedSignalPayload, RejectedSignalPayload
  orders.py       → OrderFilledPayload, OrderRejectedPayload
  positions.py    → PositionOpenedPayload, PositionClosedPayload   ← (no PositionPayload)
  trades.py       → TradePayload, TradeSeriesPayload
  orderbook.py    → OrderBookSnapshotPayload, OrderBookDeltaPayload
  funding.py      → FundingRatePayload
  oi.py           → OpenInterestPayload
  liquidations.py → LiquidationPayload

Nota: PositionPayload no existe — nombre incorrecto en versión anterior.
"""

# __all__ vacío intencional — API pública via submódulo, no via __init__.
__all__: list[str] = []
