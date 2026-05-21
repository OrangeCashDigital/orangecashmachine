# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/topics.py
==========================================

MÓDULO DEPRECADO — shim de compatibilidad temporal.

Este módulo re-exporta desde shared.kafka.topics (SSOT global).
Será eliminado cuando todos los importers hayan migrado.

Condición de eliminación:
    grep -r "market_data.infrastructure.kafka.topics" --include="*.py" .
    → debe retornar 0 resultados fuera de este archivo.

Migración:
    ANTES: from market_data.infrastructure.kafka.topics import TOPIC_OHLCV_RAW
    AHORA: from shared.kafka.topics                     import TOPIC_OHLCV_RAW

Divergencia corregida:
    TOPIC_DLQ legacy = "ohlcv.dlq"  ← INCORRECTO (BC local)
    TOPIC_DLQ shared = "ocm.dlq"    ← CORRECTO   (SSOT global)
    Este shim usa el valor correcto de shared.

Principios: SSOT · DRY · SRP
"""

from __future__ import annotations

import warnings as _warnings

_warnings.warn(
    "market_data.infrastructure.kafka.topics está deprecado. "
    "Importar desde shared.kafka.topics (SSOT global). "
    "Ver condición de eliminación en el docstring de este módulo.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export completo desde shared — sin redefinir strings (DRY · SSOT)
from shared.kafka.topics import (  # noqa: E402, F401
    TOPIC_OHLCV_RAW,
    TOPIC_OHLCV_VALIDATED,
    TOPIC_OHLCV_FEATURES,
    TOPIC_SIGNALS_RAW,
    TOPIC_SIGNALS_APPROVED,
    TOPIC_SIGNALS_REJECTED,
    TOPIC_ORDERS_FILLED,
    TOPIC_ORDERS_REJECTED,
    TOPIC_POSITIONS_OPENED,
    TOPIC_POSITIONS_CLOSED,
    TOPIC_DLQ,
    GROUP_BRONZE_WRITER,
    GROUP_QUALITY_GATE,
    GROUP_FEATURES,
    GROUP_STRATEGY,
    GROUP_RISK_GATE,
    GROUP_EXECUTION,
    GROUP_PORTFOLIO,
    HEADER_SOURCE,
    HEADER_VERSION,
    HEADER_RUN_ID,
    HEADER_DOMAIN,
)

__all__ = [
    "TOPIC_OHLCV_RAW",
    "TOPIC_OHLCV_VALIDATED",
    "TOPIC_OHLCV_FEATURES",
    "TOPIC_SIGNALS_RAW",
    "TOPIC_SIGNALS_APPROVED",
    "TOPIC_SIGNALS_REJECTED",
    "TOPIC_ORDERS_FILLED",
    "TOPIC_ORDERS_REJECTED",
    "TOPIC_POSITIONS_OPENED",
    "TOPIC_POSITIONS_CLOSED",
    "TOPIC_DLQ",
    "GROUP_BRONZE_WRITER",
    "GROUP_QUALITY_GATE",
    "GROUP_FEATURES",
    "GROUP_STRATEGY",
    "GROUP_RISK_GATE",
    "GROUP_EXECUTION",
    "GROUP_PORTFOLIO",
    "HEADER_SOURCE",
    "HEADER_VERSION",
    "HEADER_RUN_ID",
    "HEADER_DOMAIN",
]
