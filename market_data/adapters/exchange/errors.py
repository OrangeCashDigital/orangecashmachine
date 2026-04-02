"""
services/exchange/errors.py
============================
Excepciones del layer de exchange.

Taxonomía de errores
--------------------
Cada excepción declara `is_transient: bool` como atributo de clase.

  is_transient=True  → retry seguro (red, rate limit, cooldown)
  is_transient=False → fallo permanente (exchange no soportado, config inválida)

Separadas del adapter para que los consumidores puedan importar
solo los tipos de error sin arrastrar toda la infraestructura ccxt.
"""
from __future__ import annotations


class ExchangeAdapterError(Exception):
    """
    Base adapter error.
    `is_transient=False` por defecto — asumir permanente si no se especifica.
    """
    is_transient: bool = False


class UnsupportedExchangeError(ExchangeAdapterError):
    """Exchange no soportado por ccxt. Config inválida — permanente."""
    is_transient: bool = False


class ExchangeConnectionError(ExchangeAdapterError):
    """
    Fallo de conexión tras retries.
    Transitorio: la red o el exchange pueden recuperarse.
    """
    is_transient: bool = True


class ExchangeCircuitOpenError(ExchangeAdapterError):
    """
    Circuit breaker abierto — exchange en cooldown.
    Transitorio: el breaker se cerrará tras reset_timeout.
    """
    is_transient: bool = True
