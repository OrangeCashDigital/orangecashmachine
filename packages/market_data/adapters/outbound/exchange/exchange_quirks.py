# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/exchange/exchange_quirks.py
=========================================================

STUB de backward compatibility — re-exporta desde domain.

ExchangeQuirks vive ahora en:
    market_data.domain.value_objects.exchange_quirks

Motivo: es un value object puro de dominio (sin deps de infraestructura).
Los nuevos imports deben apuntar directamente al dominio.

Principios: DIP · SSOT · OCP (nueva SSOT sin romper contratos existentes)
"""
from market_data.domain.value_objects.exchange_quirks import (  # noqa: F401
    ExchangeQuirks,
    EXCHANGE_QUIRKS,
    get_quirks,
    get_origin_fallback_ms,
)

__all__ = [
    "ExchangeQuirks",
    "EXCHANGE_QUIRKS",
    "get_quirks",
    "get_origin_fallback_ms",
]
