# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/payloads.py
=============================================

SHIM DE COMPATIBILIDAD — FASE DE TRANSICIÓN.

SSOT canónico: shared.kafka.schemas.ohlcv
Este módulo re-exporta desde shared para no romper importers existentes.

Deprecado: eliminar en Fase 2.
Condición: grep -r "infrastructure.kafka.payloads" devuelve cero resultados.

Excepción mapeada
-----------------
SchemaVersionError local → OHLCVSchemaVersionError en shared.
Re-exportamos ambas para compatibilidad con catchers existentes.
"""
from __future__ import annotations

import warnings as _warnings

from shared.kafka.schemas.ohlcv import (
    OHLCV_SCHEMA_VERSION,
    OHLCV_SCHEMA_VERSION as PAYLOAD_SCHEMA_VERSION,   # alias legacy
    DataSource,
    DATASOURCE_LIVE,
    DATASOURCE_BACKFILL,
    DATASOURCE_REPLAY,
    OHLCVSchemaVersionError,
    OHLCVSchemaVersionError as SchemaVersionError,    # alias legacy
    KafkaOHLCVBar,
    EventPayload,
)

_warnings.warn(
    "market_data.infrastructure.kafka.payloads está deprecado. "
    "Importar desde shared.kafka.schemas.ohlcv.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "KafkaOHLCVBar",
    "EventPayload",
    "SchemaVersionError",
    "OHLCVSchemaVersionError",
    "PAYLOAD_SCHEMA_VERSION",
    "OHLCV_SCHEMA_VERSION",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
]
