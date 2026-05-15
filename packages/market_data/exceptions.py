# -*- coding: utf-8 -*-
"""
market_data/exceptions.py
==========================

Jerarquía de excepciones tipadas del bounded context market_data.

Principios
----------
- SRP: un módulo, un propósito — definir contratos de error.
- Fail-Fast: el caller sabe exactamente qué falló sin inspeccionar strings.
- DDD: las excepciones hablan el lenguaje del dominio, no de la infra.
- Clean Code: nombres descriptivos, sin abreviaturas opacas.

Jerarquía
---------
MarketDataError                         ← base del bounded context
├── PipelineBuildError                  ← construcción de pipeline fallida
├── PipelineExecutionError              ← ejecución de pipeline fallida
│   └── PipelineTimeoutError            ← timeout en ejecución
├── StorageError                        ← operaciones de storage
│   ├── StorageReadError
│   ├── StorageWriteError
│   └── StorageTimeoutError
├── ExchangeError                       ← comunicación con exchange
│   ├── ExchangeConnectionError
│   └── ExchangeTimeoutError
├── ResampleError                       ← resampling OHLCV
└── ConfigurationError                  ← configuración inválida
"""
from __future__ import annotations


# =============================================================================
# Base
# =============================================================================

class MarketDataError(Exception):
    """Base de todas las excepciones del bounded context market_data."""


# =============================================================================
# Pipeline
# =============================================================================

class PipelineBuildError(MarketDataError):
    """
    No se pudo construir el pipeline solicitado.

    Raised by: PipelineOrchestrator._build_pipeline()
    Context: tipo de pipeline inválido, dependencias no disponibles.
    """


class PipelineExecutionError(MarketDataError):
    """
    Error durante la ejecución del pipeline.

    Raised by: PipelineOrchestrator.run()
    Wraps: excepciones de OHLCVPipeline, TradesPipeline, etc.
    """


class PipelineTimeoutError(PipelineExecutionError):
    """El pipeline superó el timeout configurado."""


# =============================================================================
# Storage
# =============================================================================

class StorageError(MarketDataError):
    """Base para errores de persistencia."""


class StorageReadError(StorageError):
    """Fallo al leer datos del storage."""


class StorageWriteError(StorageError):
    """Fallo al escribir datos en el storage."""


class StorageTimeoutError(StorageError):
    """Operación de storage superó el timeout configurado."""


# =============================================================================
# Exchange / CCXT
# =============================================================================

class ExchangeError(MarketDataError):
    """Base para errores de comunicación con exchanges."""


class ExchangeConnectionError(ExchangeError):
    """No se pudo conectar al exchange."""


class ExchangeTimeoutError(ExchangeError):
    """La llamada al exchange superó el timeout configurado."""


# =============================================================================
# Resample
# =============================================================================

class ResampleError(MarketDataError):
    """Error durante el resampling de OHLCV."""


# =============================================================================
# Configuration
# =============================================================================

class ConfigurationError(MarketDataError):
    """Configuración del sistema inválida o incompleta."""
