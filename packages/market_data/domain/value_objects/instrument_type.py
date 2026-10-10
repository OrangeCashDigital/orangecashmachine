# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/instrument_type.py
=====================================================

InstrumentType — categoría fundamental de instrumento (G1-D04).

D04 (ADR-0049): el instrument type forma parte del contexto identitario
de la Listing Identity a nivel de categoría fundamental. SPOT, PERPETUAL,
FUTURE y OPTION representan categorías distintas de entidades negociables
y NO deben compartir una misma identidad solo por tratarse de valores
distintos de un atributo.

Nota
----
Estas son las ÚNICAS categorías de D04. NO se agregan categorías nuevas
aquí; cualquier extensión sería una decisión de la serie G1 (no inferida).

Principios
----------
DDD  — VO puro: Enum inmutable
SSOT — única definición de la taxonomía en el dominio
KISS — sin lógica; solo taxonomía identitaria
"""

from __future__ import annotations

from enum import Enum

# ===========================================================================
# InstrumentType
# ===========================================================================


class InstrumentType(str, Enum):
    """
    Categoría fundamental de instrumento (G1-D04).

    SPOT      — cash market, sin vencimiento.
    PERPETUAL — swap perpetuo, sin vencimiento.
    FUTURE    — contrato con vencimiento (D06 aplica).
    OPTION    — opción: strike + right identity-defining (D07).

    str-compatible: InstrumentType.SPOT == "spot" → True.
    """

    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    OPTION = "option"


__all__ = ["InstrumentType"]
