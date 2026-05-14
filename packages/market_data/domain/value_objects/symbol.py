# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/symbol.py
============================================

Symbol — Value Object canónico de un par de trading.

Principios
----------
DDD   — VO puro: definido por (base, quote); sin identidad de negocio
SSOT  — única definición del concepto "par de trading" en el dominio
KISS  — solo lo que el dominio necesita: base, quote, representación canónica
"""
from __future__ import annotations

from dataclasses import dataclass


# ===========================================================================
# Symbol — Value Object
# ===========================================================================

@dataclass(frozen=True)
class Symbol:
    """
    Par de trading — Value Object del dominio.

    Immutable (frozen=True): un par no cambia de definición.
    Equality por valor: Symbol("BTC", "USDT") == Symbol("BTC", "USDT").

    Campos
    ------
    base  : moneda base  ("BTC", "ETH", "SOL")
    quote : moneda quote ("USDT", "BTC", "ETH")

    Representación canónica
    -----------------------
    str(symbol) → "BTC/USDT"  (formato CCXT estándar)
    """
    base:  str
    quote: str

    def __str__(self) -> str:
        """Representación canónica: "BTC/USDT"."""
        return f"{self.base}/{self.quote}"

    def __repr__(self) -> str:
        return f"Symbol({self.base!r}/{self.quote!r})"

    @classmethod
    def from_string(cls, s: str) -> "Symbol":
        """
        Construye desde string canónico CCXT ("BTC/USDT").

        Fail-Fast: lanza ValueError si el formato es inválido.

        Parameters
        ----------
        s : str — par en formato "BASE/QUOTE"

        Raises
        ------
        ValueError — si el string no contiene exactamente un "/"
        """
        parts = s.split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Symbol.from_string esperaba formato 'BASE/QUOTE', "
                f"recibió: {s!r}"
            )
        return cls(base=parts[0].upper(), quote=parts[1].upper())

    @property
    def canonical(self) -> str:
        """String canónico — equivalente a str(self)."""
        return f"{self.base}/{self.quote}"


# ===========================================================================
# __all__
# ===========================================================================

__all__ = ["Symbol"]
