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
    _base.py     — BasePayload común
    ohlcv.py     — EventPayload, KafkaOHLCVBar
    signals.py   — SignalPayload, ApprovedSignalPayload
    orders.py    — OrderFilledPayload, OrderRejectedPayload
    positions.py — PositionPayload

Regla de dependencia (BC-01)
-----------------------------
shared/ → solo stdlib y third-party (json, dataclasses, uuid, datetime).
PROHIBIDO importar desde market_data, trading, portfolio, ocm.

Regla de uso
------------
Producers y consumers importan desde shared.kafka, nunca desde
market_data.infrastructure.kafka.payloads (que será deprecado).

    from shared.kafka.topics   import TOPIC_OHLCV_RAW, TOPIC_SIGNALS_APPROVED
    from shared.kafka.schemas.ohlcv    import EventPayload
    from shared.kafka.schemas.signals  import SignalPayload
    from shared.kafka.serializer       import serialize, deserialize
"""
