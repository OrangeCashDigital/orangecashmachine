# -*- coding: utf-8 -*-
"""
market_data/exceptions.py
==========================

FACADE — backward-compatibility re-export.

Todos los tipos han migrado a:
    market_data.domain.exceptions

Este módulo se mantiene para no romper imports existentes (OCP).
Los nuevos consumidores deben importar desde market_data.domain.exceptions.

TODO: deprecar en la siguiente major version.
"""
from market_data.domain.exceptions import (  # noqa: F401
    MarketDataError,
    IngestionError,
    RetryExhaustedError,
    ExchangeCircuitOpenError,
    StorageError,
    ChunkRejectedError,
    QualityError,
    SchemaValidationError,
    AnomalyDetectedError,
    PipelineError,
    MissingStartDateError,
    CursorError,
    # Exchange adapter (anteriormente solo en adapters/exchange/errors.py)
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
)

__all__ = [
    "MarketDataError",
    "IngestionError",
    "RetryExhaustedError",
    "ExchangeCircuitOpenError",
    "StorageError",
    "ChunkRejectedError",
    "QualityError",
    "SchemaValidationError",
    "AnomalyDetectedError",
    "PipelineError",
    "MissingStartDateError",
    "CursorError",
    "ExchangeAdapterError",
    "UnsupportedExchangeError",
    "ExchangeConnectionError",
]
