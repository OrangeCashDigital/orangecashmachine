"""
services/exchange/errors.py
============================
Excepciones del layer de exchange.

Separadas del adapter para que los consumidores puedan importar
solo los tipos de error sin arrastrar toda la infraestructura ccxt.
"""
from __future__ import annotations


class ExchangeAdapterError(Exception):
    """Base adapter error."""


class UnsupportedExchangeError(ExchangeAdapterError):
    """Exchange no soportado por ccxt."""


class ExchangeConnectionError(ExchangeAdapterError):
    """Fallo de conexión tras retries."""


class ExchangeCircuitOpenError(ExchangeAdapterError):
    """Circuit breaker abierto — exchange en cooldown."""
