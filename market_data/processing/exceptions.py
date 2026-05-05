# -*- coding: utf-8 -*-
"""
market_data/processing/exceptions.py
======================================

FACADE — backward-compatibility re-export.

Todos los tipos han migrado a:
    market_data.domain.exceptions

Este módulo se mantiene para no romper imports existentes (OCP).
Los nuevos consumidores deben importar desde market_data.domain.exceptions.

TODO: deprecar en la siguiente major version.
"""
from market_data.domain.exceptions import (  # noqa: F401
    FetcherError,
    ChunkFetchError,
    NoDataAvailableError,
    MissingStartDateError,
    SymbolNotFoundError,
    InvalidMarketTypeError,
)

__all__ = [
    "FetcherError",
    "ChunkFetchError",
    "NoDataAvailableError",
    "MissingStartDateError",
    "SymbolNotFoundError",
    "InvalidMarketTypeError",
]
