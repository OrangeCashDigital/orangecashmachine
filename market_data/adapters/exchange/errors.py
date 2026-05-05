# -*- coding: utf-8 -*-
"""
market_data/adapters/exchange/errors.py
=========================================

FACADE — backward-compatibility re-export.

Todos los tipos han migrado a:
    market_data.domain.exceptions

Este módulo se mantiene para no romper imports existentes (OCP).
Los nuevos consumidores deben importar desde market_data.domain.exceptions.

Semántica de `is_transient`
----------------------------
Conservada en domain/exceptions como atributo de clase — no se pierde
ninguna información al migrar desde este módulo.

TODO: deprecar en la siguiente major version.
"""
from market_data.domain.exceptions import (  # noqa: F401
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
    ExchangeCircuitOpenError,
)

__all__ = [
    "ExchangeAdapterError",
    "UnsupportedExchangeError",
    "ExchangeConnectionError",
    "ExchangeCircuitOpenError",
]
