# -*- coding: utf-8 -*-
"""
market_data/exceptions.py
==========================

Re-export de compatibilidad hacia domain/exceptions (SSOT real).

SSOT: market_data/domain/exceptions/__init__.py
Este módulo es un alias delgado — NO añadir lógica aquí.

Consumidores legacy (migrar en próximo sprint):
  application/use_cases/pipeline_orchestrator.py
  infrastructure/bootstrap/container.py
"""
from market_data.domain.exceptions import (  # noqa: F401
    MarketDataError,
    # Ingestión
    IngestionError,
    FetcherError,
    ChunkFetchError,
    NoDataAvailableError,
    SymbolNotFoundError,
    InvalidMarketTypeError,
    RetryExhaustedError,
    # Storage
    StorageError,
    ChunkRejectedError,
    DataNotFoundError,
    DataReadError,
    VersionNotFoundError,
    MarketDataLoaderError,
    # Calidad
    QualityError,
    SchemaValidationError,
    AnomalyDetectedError,
    SchemaVersionError,
    DataQualityError,
    # Pipeline
    PipelineError,
    MissingStartDateError,
    CursorError,
    PipelineBuildError,
    PipelineExecutionError,
    PipelineTimeoutError,
    # Configuración
    ConfigurationError,
    # Exchange adapter
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
    ExchangeCircuitOpenError,
)

__all__ = [
    "MarketDataError",
    "IngestionError", "FetcherError", "ChunkFetchError",
    "NoDataAvailableError", "SymbolNotFoundError",
    "InvalidMarketTypeError", "RetryExhaustedError",
    "StorageError", "ChunkRejectedError",
    "DataNotFoundError", "DataReadError",
    "VersionNotFoundError", "MarketDataLoaderError",
    "QualityError", "SchemaValidationError", "AnomalyDetectedError",
    "SchemaVersionError", "DataQualityError",
    "PipelineError", "MissingStartDateError", "CursorError",
    "PipelineBuildError", "PipelineExecutionError", "PipelineTimeoutError",
    "ConfigurationError",
    "ExchangeAdapterError", "UnsupportedExchangeError",
    "ExchangeConnectionError", "ExchangeCircuitOpenError",
]
