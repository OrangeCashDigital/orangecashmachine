# -*- coding: utf-8 -*-
"""
market_data/domain/exceptions/__init__.py
==========================================

Excepciones canónicas del bounded context market_data.

Jerarquía
---------
MarketDataError                     ← raíz del BC
├── IngestionError                  ← fallos en ingestión de datos
│   ├── FetcherError                ← error genérico de fetcher
│   │   ├── ChunkFetchError         ← fallo en un chunk específico
│   │   ├── NoDataAvailableError    ← exchange sin datos para el rango
│   │   ├── SymbolNotFoundError     ← símbolo no listado en exchange
│   │   └── InvalidMarketTypeError  ← market_type inválido (spot/swap)
│   └── RetryExhaustedError         ← reintentos agotados
├── StorageError                    ← fallos en escritura/lectura de capas
│   └── ChunkRejectedError          ← chunk rechazado por schema/validación
├── QualityError                    ← violación de calidad de datos
│   ├── SchemaValidationError       ← schema Silver/Gold no satisfecho
│   └── AnomalyDetectedError        ← anomalía estadística detectada
├── PipelineError                   ← fallo de orquestación de pipeline
│   ├── MissingStartDateError       ← cursor sin fecha de inicio definida
│   └── CursorError                 ← fallo de cursor/estado de avance
└── ExchangeAdapterError            ← fallo en capa de adaptador exchange
    ├── UnsupportedExchangeError    ← exchange no soportado por CCXT/OCM
    ├── ExchangeConnectionError     ← fallo de red/conexión al exchange
    └── ExchangeCircuitOpenError    ← circuit breaker abierto

Principios
----------
SSOT   — única fuente de verdad para excepciones del BC
DIP    — capas superiores importan desde aquí, nunca desde infra/adapters
OCP    — agregar subclases no modifica la jerarquía existente
DRY    — no duplicar definiciones entre exceptions.py y processing/exceptions.py
Clean Architecture — excepciones de dominio no dependen de frameworks externos
"""
from __future__ import annotations


# ===========================================================================
# Raíz del bounded context
# ===========================================================================

class MarketDataError(Exception):
    """Raíz de todas las excepciones del bounded context market_data."""


# ===========================================================================
# Ingestión
# ===========================================================================

class IngestionError(MarketDataError):
    """Fallo durante la ingestión de datos desde un exchange o proveedor."""


class FetcherError(IngestionError):
    """Error genérico de fetcher — base para errores de descarga."""


class ChunkFetchError(FetcherError):
    """Fallo al descargar un chunk específico de datos históricos."""


class NoDataAvailableError(FetcherError):
    """
    El exchange no tiene datos para el rango solicitado.

    Fail-soft semántico: el pipeline marca el gap como irrecuperable
    (irreversible=True en GapRegistry) y continúa sin abortar.
    """


class SymbolNotFoundError(FetcherError):
    """El símbolo solicitado no está listado en el exchange."""


class InvalidMarketTypeError(FetcherError):
    """El market_type solicitado (spot/swap/futures) no es válido para este exchange."""


class RetryExhaustedError(IngestionError):
    """
    Reintentos agotados sin éxito.

    Fail-fast: si todos los reintentos fallan, el pipeline aborta
    el símbolo afectado y lo registra en lineage como FAILED.
    """


# ===========================================================================
# Storage
# ===========================================================================

class StorageError(MarketDataError):
    """Fallo durante escritura o lectura en Bronze/Silver/Gold."""


class ChunkRejectedError(StorageError):
    """
    Chunk rechazado antes de escritura por fallo de schema o validación.

    Distinto de StorageError: el rechazo ocurre en la capa de calidad,
    no en la capa de I/O. El chunk nunca llega al storage.
    """


# ===========================================================================
# Calidad de datos
# ===========================================================================

class QualityError(MarketDataError):
    """Violación de una regla de calidad de datos."""


class SchemaValidationError(QualityError):
    """El DataFrame no satisface el schema Silver o Gold esperado."""


class AnomalyDetectedError(QualityError):
    """Anomalía estadística detectada en el dataset (outlier, drift, gap)."""


# ===========================================================================
# Pipeline / Orquestación
# ===========================================================================

class PipelineError(MarketDataError):
    """Fallo de orquestación — el pipeline no puede continuar."""


class MissingStartDateError(PipelineError):
    """
    El cursor no tiene fecha de inicio definida para el par solicitado.

    Fail-fast: el pipeline no puede inferir desde dónde arrancar
    sin una fecha de inicio explícita.
    """


class CursorError(PipelineError):
    """Fallo al leer o escribir el estado de avance (cursor) del pipeline."""


# ===========================================================================
# Exchange adapter
# ===========================================================================

class ExchangeAdapterError(MarketDataError):
    """Fallo en la capa de adaptador de exchange (CCXT wrapper)."""


class UnsupportedExchangeError(ExchangeAdapterError):
    """El exchange solicitado no está soportado por CCXT o por OCM."""


class ExchangeConnectionError(ExchangeAdapterError):
    """Fallo de red o conexión al conectar con el exchange."""


class ExchangeCircuitOpenError(ExchangeAdapterError):
    """
    Circuit breaker abierto — demasiados fallos consecutivos.

    Fail-fast: mientras el circuit esté abierto, las llamadas al
    exchange se rechazan inmediatamente sin intentar la conexión.
    """


# ===========================================================================
# __all__ — API pública explícita
# ===========================================================================

__all__ = [
    # Raíz
    "MarketDataError",
    # Ingestión
    "IngestionError",
    "FetcherError",
    "ChunkFetchError",
    "NoDataAvailableError",
    "SymbolNotFoundError",
    "InvalidMarketTypeError",
    "RetryExhaustedError",
    # Storage
    "StorageError",
    "ChunkRejectedError",
    # Calidad
    "QualityError",
    "SchemaValidationError",
    "AnomalyDetectedError",
    # Pipeline
    "PipelineError",
    "MissingStartDateError",
    "CursorError",
    # Exchange adapter
    "ExchangeAdapterError",
    "UnsupportedExchangeError",
    "ExchangeConnectionError",
    "ExchangeCircuitOpenError",
]
