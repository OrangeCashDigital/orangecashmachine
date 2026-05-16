# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/serializer.py
===============================================

Serialización EventPayload ↔ bytes para el wire format Kafka.

Responsabilidad
---------------
SSOT de cómo los EventPayload se convierten a bytes JSON para
publicar en Kafka y cómo se reconstruyen al consumir.

Wire format
-----------
JSON UTF-8. Schema definido por EventPayload.to_dict() — SSOT en
kafka/payloads.py. Versionado explícito via PAYLOAD_SCHEMA_VERSION.

Routing key
-----------
"{exchange}:{symbol}:{timeframe}" como bytes UTF-8.
Kafka asigna partición por hash(key) — mismo par/timeframe siempre
va a la misma partición → orden garantizado por símbolo.

Principios: SSOT · SRP · Fail-Fast · KISS
"""
from __future__ import annotations

import json

from market_data.infrastructure.kafka.payloads import EventPayload


def serialize(event: EventPayload) -> bytes:
    """
    Serializa un EventPayload a bytes JSON UTF-8.

    Usa EventPayload.to_dict() como SSOT del schema.
    Compatible con deserialize() via from_dict().

    Raises
    ------
    ValueError : si event es None (Fail-Fast)
    """
    if event is None:
        raise ValueError("serialize: event no puede ser None")
    return json.dumps(event.to_dict(), ensure_ascii=False).encode("utf-8")


def deserialize(data: bytes) -> EventPayload:
    """
    Deserializa bytes JSON UTF-8 a EventPayload.

    Fail-Fast: lanza SchemaVersionError si la versión es incompatible.

    Raises
    ------
    SchemaVersionError : versión de schema incompatible
    ValueError         : bytes no son JSON válido
    """
    payload = json.loads(data.decode("utf-8"))
    return EventPayload.from_dict(payload)


def make_routing_key(
    exchange:  str,
    symbol:    str,
    timeframe: str,
) -> bytes:
    """
    Construye la routing key canónica como bytes UTF-8.

    Formato: "{exchange}:{symbol}:{timeframe}"
    Ejemplo: b"bybit:BTC/USDT:1h"

    Kafka asigna partición según hash(key) — mismo routing key
    siempre va a la misma partición → orden por par/timeframe.
    """
    return f"{exchange}:{symbol}:{timeframe}".encode("utf-8")


__all__ = ["serialize", "deserialize", "make_routing_key"]
