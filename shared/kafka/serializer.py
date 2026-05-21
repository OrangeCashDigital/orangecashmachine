# -*- coding: utf-8 -*-
"""
shared/kafka/serializer.py
===========================

Serialización genérica BasePayload ↔ bytes para el wire format Kafka.

Migración desde market_data/infrastructure/kafka/serializer.py
--------------------------------------------------------------
Este módulo reemplaza al original. Durante la transición, el original
re-exportará desde aquí. Luego será eliminado.

Wire format
-----------
JSON UTF-8. Schema definido por BasePayload.to_dict() en cada subclase.
Versionado explícito via event_version en cada payload.

Routing keys canónicas
----------------------
  make_ohlcv_key(exchange, symbol, timeframe) → b"bybit:BTC/USDT:1m"
  make_symbol_key(exchange, symbol)           → b"bybit:BTC/USDT"

La routing key determina la partición Kafka (hash(key)):
  - ohlcv.*   → make_ohlcv_key  (orden por par+timeframe)
  - signals.* → make_symbol_key (orden por símbolo)
  - orders.*  → make_symbol_key (orden por símbolo)
  - positions.→ make_symbol_key (orden por símbolo)

Principios: SSOT · SRP · Fail-Fast · KISS
"""

from __future__ import annotations

import json
from typing import Any, Dict, Type, TypeVar

from shared.kafka.schemas._base import BasePayload

P = TypeVar("P", bound=BasePayload)


# ---------------------------------------------------------------------------
# Serialización
# ---------------------------------------------------------------------------


def serialize(payload: BasePayload) -> bytes:
    """
    Serializa un BasePayload a bytes JSON UTF-8.

    Fail-Fast: lanza ValueError si payload es None.
    Usa payload.to_dict() como SSOT del schema.
    """
    if payload is None:
        raise ValueError("serialize: payload no puede ser None")
    return json.dumps(payload.to_dict(), ensure_ascii=False).encode("utf-8")


def deserialize(data: bytes, payload_cls: Type[P]) -> P:
    """
    Deserializa bytes JSON UTF-8 a un payload concreto.

    Fail-Fast: lanza SchemaVersionError si la versión es incompatible.
    Fail-Fast: lanza ValueError si data no es JSON válido.

    Uso:
        ep = deserialize(raw_bytes, EventPayload)
        sp = deserialize(raw_bytes, SignalPayload)
    """
    if data is None:
        raise ValueError("deserialize: data no puede ser None")
    raw: Dict[str, Any] = json.loads(data.decode("utf-8"))
    return payload_cls.from_dict(raw)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Routing keys — SSOT de construcción de partition keys
# ---------------------------------------------------------------------------


def make_ohlcv_key(exchange: str, symbol: str, timeframe: str) -> bytes:
    """
    Routing key para topics OHLCV.

    Formato: "{exchange}:{symbol}:{timeframe}"
    Ejemplo: b"bybit:BTC/USDT:1m"

    Garantía: mismo (exchange, symbol, timeframe) → misma partición → FIFO.
    """
    return f"{exchange}:{symbol}:{timeframe}".encode("utf-8")


def make_symbol_key(exchange: str, symbol: str) -> bytes:
    """
    Routing key para topics de señales, órdenes y posiciones.

    Formato: "{exchange}:{symbol}"
    Ejemplo: b"bybit:BTC/USDT"

    Una señal/orden/posición de BTC/USDT es la misma decisión
    independiente del timeframe — ordering por símbolo.
    """
    return f"{exchange}:{symbol}".encode("utf-8")


# Alias de compatibilidad — ELIMINAR en Fase 2 cuando se borre
# market_data/infrastructure/kafka/serializer.py.
# Condición de eliminación: grep -r "make_routing_key" — debe devolver cero resultados
# fuera de este archivo antes de borrar el alias.
make_routing_key = make_ohlcv_key


__all__ = [
    "serialize",
    "deserialize",
    "make_ohlcv_key",
    "make_symbol_key",
    "make_routing_key",
]
