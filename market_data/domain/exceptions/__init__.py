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
│   ├── AnomalyDetectedError        ← anomalía estadística detectada
│   └── SchemaVersionError          ← payload con versión de schema incompatible
├── PipelineError                   ← fallo de orquestación de pipeline
│   ├── MissingStartDateError       ← cursor sin fecha de inicio definida
│   └── CursorError                 ← fallo de cursor/estado de avance
└── ExchangeAdapterError            ← fallo en capa de adaptador exchange
    ├── UnsupportedExchangeError    ← exchange no soportado por CCXT/OCM
    ├── ExchangeConnectionError     ← fallo de red/conexión al exchange (transitorio)
    └── ExchangeCircuitOpenError    ← circuit breaker abierto (transitorio)

Atributo `is_transient`
-----------------------
Las subclases de ExchangeAdapterError declaran `is_transient: bool` como
atributo de clase. Permite que ResilienceLayer decida si hacer retry
sin importar la clase concreta (OCP · DIP).

  is_transient=True  → retry seguro (red, rate limit, cooldown)
  is_transient=False → fallo permanente (config inválida, exchange no soportado)

Principios
----------
SSOT   — única fuente de verdad para excepciones del BC
DIP    — capas superiores importan desde aquí, nunca desde infra/adapters
OCP    — agregar subclases no modifica la jerarquía existente
DRY    — elimina duplicación entre exceptions.py y processing/exceptions.py
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


class SchemaVersionError(QualityError):
    """
    El payload tiene una versión de schema incompatible con la esperada.

    Fail-fast: un consumer no puede procesar un payload de versión
    desconocida sin riesgo de corrupción silenciosa.

    Anteriormente definida en streaming/payloads.py como subclase de
    ValueError — migrada aquí para SSOT y jerarquía correcta (DDD).
    """


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
    """
    Fallo en la capa de adaptador de exchange (CCXT wrapper).

    Atributo de clase `is_transient`
    ---------------------------------
    Permite que ResilienceLayer inspeccione si debe reintentar
    sin hacer isinstance() contra subclases concretas (OCP · DIP).

    is_transient=False por defecto — asumir permanente si no se especifica.
    """
    is_transient: bool = False


class UnsupportedExchangeError(ExchangeAdapterError):
    """
    El exchange solicitado no está soportado por CCXT o por OCM.

    Permanente: no tiene sentido reintentar con la misma config.
    """
    is_transient: bool = False


class ExchangeConnectionError(ExchangeAdapterError):
    """
    Fallo de red o conexión al conectar con el exchange.

    Transitorio: la red o el exchange pueden recuperarse.
    """
    is_transient: bool = True


class ExchangeCircuitOpenError(ExchangeAdapterError):
    """
    Circuit breaker abierto — exchange en cooldown tras fallos consecutivos.

    Transitorio: el breaker se cerrará tras reset_timeout.
    Fail-fast: mientras esté abierto, las llamadas se rechazan inmediatamente.
    """
    is_transient: bool = True


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
    "SchemaVersionError",
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
