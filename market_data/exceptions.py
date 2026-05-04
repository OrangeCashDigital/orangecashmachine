# -*- coding: utf-8 -*-
"""
market_data/exceptions.py
==========================

SSOT de excepciones del bounded context market_data.

Jerarquía
---------
MarketDataError                      — base de todo el BC
  IngestionError                     — fallos en descarga de datos
    RetryExhaustedError              — reintentos agotados sin éxito
    ExchangeCircuitOpenError         — circuit breaker abierto
  StorageError                       — fallos en escritura/lectura
    ChunkRejectedError               — chunk rechazado por calidad
  QualityError                       — violaciones de invariantes de datos
    SchemaValidationError            — schema OHLCV incorrecto
    AnomalyDetectedError             — anomalía detectada en series
  PipelineError                      — errores de orquestación
    MissingStartDateError            — fecha de inicio no resuelta
    CursorError                      — fallo en persistencia de cursor

Principios
----------
SSOT  — una sola fuente de verdad para todas las excepciones del BC
SRP   — cada excepción tiene un único motivo de existir
DRY   — la jerarquía evita duplicación de catch clauses
Fail-Fast — nombres explícitos facilitan diagnóstico inmediato
"""
from __future__ import annotations


# =============================================================================
# Base
# =============================================================================

class MarketDataError(Exception):
    """Base para todas las excepciones del bounded context market_data."""


# =============================================================================
# Ingestion
# =============================================================================

class IngestionError(MarketDataError):
    """Fallo durante la descarga o ingesta de datos desde un exchange."""


class RetryExhaustedError(IngestionError):
    """Todos los reintentos se agotaron sin obtener datos válidos."""


class ExchangeCircuitOpenError(IngestionError):
    """El circuit breaker está abierto — exchange no disponible temporalmente."""


# =============================================================================
# Storage
# =============================================================================

class StorageError(MarketDataError):
    """Fallo durante escritura o lectura en el backend de almacenamiento."""


class ChunkRejectedError(StorageError):
    """Un chunk OHLCV fue rechazado por no superar los controles de calidad."""


# =============================================================================
# Quality
# =============================================================================

class QualityError(MarketDataError):
    """Violación de invariantes o reglas de calidad de datos."""


class SchemaValidationError(QualityError):
    """El DataFrame recibido no cumple el schema OHLCV esperado."""


class AnomalyDetectedError(QualityError):
    """Se detectó una anomalía estadística en la serie temporal."""


# =============================================================================
# Pipeline / Orchestration
# =============================================================================

class PipelineError(MarketDataError):
    """Error de orquestación o coordinación de pipeline."""


class MissingStartDateError(PipelineError):
    """No se pudo resolver la fecha de inicio del backfill."""


class CursorError(PipelineError):
    """Fallo en la persistencia o lectura del cursor de ingesta."""
