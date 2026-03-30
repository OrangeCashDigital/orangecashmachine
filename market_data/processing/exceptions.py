"""
market_data/processing/exceptions.py
=====================================

Excepciones del dominio de processing.

Separadas del fetcher para que processing/ pueda referenciarlas
sin crear dependencias circulares hacia ingestion/.

Jerarquía
---------
FetcherError
├── MissingStartDateError   — falta start_date en modo incremental/backfill
├── ChunkFetchError         — chunk falló tras MAX_RETRIES intentos
├── SymbolNotFoundError     — símbolo no existe en el exchange
└── InvalidMarketTypeError  — market_type incompatible con el símbolo
"""

from __future__ import annotations


class FetcherError(Exception):
    """Base para errores de ingestion REST."""


class MissingStartDateError(FetcherError):
    """No hay start_date disponible para iniciar descarga."""


class ChunkFetchError(FetcherError):
    """Un chunk falló tras agotar todos los reintentos."""


class SymbolNotFoundError(FetcherError):
    """El símbolo solicitado no existe en el exchange."""


class InvalidMarketTypeError(FetcherError):
    """El market_type solicitado es incompatible con el símbolo."""
