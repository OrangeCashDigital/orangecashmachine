# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/
=====================

Wire payloads por dominio. Cada módulo es independiente — importar solo
el dominio que se necesita. No importar este __init__ directamente.

    from shared.kafka.schemas.ohlcv    import EventPayload
    from shared.kafka.schemas.signals  import SignalPayload
    from shared.kafka.schemas.orders   import OrderFilledPayload
    from shared.kafka.schemas.positions import PositionPayload
"""
