# -*- coding: utf-8 -*-
"""
shared/kafka/
=============

Kafka Domain Model — SSOT de contratos wire para todos los bounded contexts.

Estructura
----------
  topics.py      — topics, consumer groups, headers (SSOT global)
  serializer.py  — serialize/deserialize + routing key (genérico)
  schemas/       — wire payloads por dominio
    _base.py     — BasePayload común (envelope de transporte)
    ohlcv.py     — EventPayload, KafkaOHLCVBar
    signals.py   — SignalPayload, ApprovedSignalPayload, RejectedSignalPayload
    orders.py    — OrderFilledPayload, OrderRejectedPayload
    positions.py — PositionOpenedPayload, PositionClosedPayload

Regla de dependencia (BC-01)
-----------------------------
shared/ → solo stdlib y third-party (json, dataclasses, uuid, datetime).
PROHIBIDO importar desde market_data, trading, portfolio, ocm.

Regla de uso
------------
Importar siempre por submódulo explícito — NO usar `from shared.kafka import *`.
La razón: cada schema es un contrato independiente; importarlos todos acopla
al consumidor a todos los dominios aunque solo necesite uno.

    # CORRECTO
    from shared.kafka.topics            import TOPIC_OHLCV_RAW
    from shared.kafka.schemas.ohlcv     import EventPayload
    from shared.kafka.schemas.signals   import SignalPayload
    from shared.kafka.serializer        import serialize, deserialize

    # INCORRECTO — acoplamiento implícito
    from shared.kafka import EventPayload
"""

# __all__ vacío intencional — la API pública se importa por submódulo.
# Ver docstring arriba para la razón de diseño.
__all__: list[str] = []
