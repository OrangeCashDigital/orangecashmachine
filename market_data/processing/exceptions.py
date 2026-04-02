"""
market_data/processing/exceptions.py
=====================================

Excepciones del dominio de processing.

Taxonomía de errores
--------------------
Cada excepción declara `is_transient: bool` como atributo de clase.

  is_transient=True  → retry seguro (red, rate limit, timeout)
  is_transient=False → fallo permanente (datos corruptos, símbolo inválido)

Esto permite clasificación por isinstance en lugar de strings frágiles.
Los consumidores deben usar `isinstance(exc, FetcherError) and exc.is_transient`
en lugar de comparar nombres de clase.

Jerarquía
---------
FetcherError
├── ChunkFetchError         — is_transient=True  (chunk falló tras retries)
├── NoDataAvailableError    — is_transient=False (exchange no tiene datos para el rango)
├── MissingStartDateError   — is_transient=False (config incompleta)
├── SymbolNotFoundError     — is_transient=False (símbolo no existe)
└── InvalidMarketTypeError  — is_transient=False (market_type incompatible)
"""

from __future__ import annotations


class FetcherError(Exception):
    """
    Base para errores de ingestion REST.

    Atributo de clase `is_transient` define si el error es recuperable por retry.
    Las subclases deben sobreescribir este atributo — nunca usar la base directamente.
    """
    is_transient: bool = False  # default conservador: asumir permanente


class ChunkFetchError(FetcherError):
    """
    Un chunk falló tras agotar todos los reintentos.
    Transitorio: red, timeout, rate limit del exchange.
    """
    is_transient: bool = True


class NoDataAvailableError(FetcherError):
    """
    El exchange no tiene datos para el rango solicitado.
    Permanente: el exchange no proveerá más datos aunque se reintente.
    Usado en repair para distinguir "sin datos" de "fallo de fetch".
    """
    is_transient: bool = False


class MissingStartDateError(FetcherError):
    """No hay start_date disponible para iniciar descarga. Error de config."""
    is_transient: bool = False


class SymbolNotFoundError(FetcherError):
    """El símbolo solicitado no existe en el exchange. Error permanente."""
    is_transient: bool = False


class InvalidMarketTypeError(FetcherError):
    """El market_type solicitado es incompatible con el símbolo. Error permanente."""
    is_transient: bool = False
