# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/serializer.py
===============================================

SHIM DE COMPATIBILIDAD — FASE DE TRANSICIÓN.

SSOT canónico: shared.kafka.serializer
Este módulo re-exporta desde shared para no romper importers existentes.

Deprecado: eliminar en Fase 2 cuando todos los importers apunten a shared.
Condición: grep -r "infrastructure.kafka.serializer" devuelve cero resultados.

Diferencia de firma en deserialize()
--------------------------------------
  Local (legacy): deserialize(data: bytes) → EventPayload
  shared:         deserialize(data: bytes, payload_cls: Type[P]) → P

El shim expone la firma legacy para compatibilidad, pero llama a shared
internamente con EventPayload como clase concreta.
Callers que necesiten deserializar otros tipos deben migrar a shared directamente.
"""
from __future__ import annotations

import warnings

from shared.kafka.serializer import (
    serialize,
    make_ohlcv_key,
    make_symbol_key,
    make_routing_key,
    deserialize as _shared_deserialize,
)
from shared.kafka.schemas.ohlcv import EventPayload


def deserialize(data: bytes) -> EventPayload:
    """
    Firma legacy — deserializa a EventPayload (shared).

    Deprecado: migrar a shared.kafka.serializer.deserialize(data, PayloadClass).
    """
    warnings.warn(
        "infrastructure.kafka.serializer.deserialize() está deprecado. "
        "Usar shared.kafka.serializer.deserialize(data, EventPayload).",
        DeprecationWarning,
        stacklevel=2,
    )
    return _shared_deserialize(data, EventPayload)


__all__ = [
    "serialize",
    "deserialize",
    "make_ohlcv_key",
    "make_symbol_key",
    "make_routing_key",
]
