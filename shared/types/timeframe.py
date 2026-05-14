# -*- coding: utf-8 -*-
"""
shared/types/timeframe.py
==========================

SSOT canónico de Timeframe — tipo transversal compartido entre todos
los bounded contexts (market_data, trading, portfolio, research).

Sin dependencias externas: stdlib puro.
Importable desde cualquier capa sin violar BC-09.

Exports
-------
Timeframe             — enum canónico (str-compatible, hashable, O(1) lookup)
timeframe_to_ms       — conversión timeframe → milisegundos (O(1), tabla)
InvalidTimeframeError — excepción Fail-Fast; lanza si timeframe desconocido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alinea timestamp al inicio del período del timeframe

Principios
----------
SSOT   — única fuente de verdad; market_data.domain.value_objects.timeframe
         re-exporta desde aquí (dependencia invertida correctamente)
DDD    — Value Object puro: inmutable, definido por valor, sin identidad
KISS   — tabla de lookup, sin regex ni parsing dinámico
Fail-Fast — timeframe desconocido → InvalidTimeframeError inmediato
BC-09  — shared no importa ningún BC; este archivo es stdlib puro
"""
from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


# ===========================================================================
# InvalidTimeframeError
# ===========================================================================

class InvalidTimeframeError(ValueError):
    """
    Lanzada cuando un string de timeframe no es reconocido.

    Fail-Fast: nunca retornar un valor por defecto silencioso.
    """


# ===========================================================================
# Lookup table — SSOT de conversión timeframe → ms
# ===========================================================================

_TF_TO_MS: dict[str, int] = {
    "1s":  1_000,
    "1m":  60_000,
    "3m":  3  * 60_000,
    "5m":  5  * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h":  60 * 60_000,
    "2h":  2  * 60 * 60_000,
    "4h":  4  * 60 * 60_000,
    "6h":  6  * 60 * 60_000,
    "8h":  8  * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d":  24 * 60 * 60_000,
    "3d":  3  * 24 * 60 * 60_000,
    "1w":  7  * 24 * 60 * 60_000,
    "1M":  30 * 24 * 60 * 60_000,
}

VALID_TIMEFRAMES: frozenset[str] = frozenset(_TF_TO_MS.keys())


# ===========================================================================
# Timeframe — Value Object enum
# ===========================================================================

class Timeframe(str, Enum):
    """
    Enum canónico de timeframes soportados.

    str-compatible: Timeframe.H1 == "1h" → True.
    Hashable: seguro en sets y dict keys.
    """
    S1  = "1s"
    M1  = "1m"
    M3  = "3m"
    M5  = "5m"
    M15 = "15m"
    M30 = "30m"
    H1  = "1h"
    H2  = "2h"
    H4  = "4h"
    H6  = "6h"
    H8  = "8h"
    H12 = "12h"
    D1  = "1d"
    D3  = "3d"
    W1  = "1w"
    MN1 = "1M"

    @property
    def ms(self) -> int:
        """Duración en milisegundos. O(1)."""
        return _TF_TO_MS[self.value]

    @classmethod
    def from_string(cls, value: str) -> "Timeframe":
        """Fail-Fast: lanza InvalidTimeframeError si el string no es reconocido."""
        try:
            return cls(value)
        except ValueError:
            raise InvalidTimeframeError(
                f"Timeframe desconocido: {value!r}. "
                f"Válidos: {sorted(VALID_TIMEFRAMES)}"
            )


# ===========================================================================
# Pure functions
# ===========================================================================

def timeframe_to_ms(timeframe: str) -> int:
    """
    Convierte un timeframe string a milisegundos. O(1).

    Fail-Fast: lanza InvalidTimeframeError si el timeframe no existe.
    """
    try:
        return _TF_TO_MS[timeframe]
    except KeyError:
        raise InvalidTimeframeError(
            f"Timeframe desconocido: {timeframe!r}. "
            f"Válidos: {sorted(VALID_TIMEFRAMES)}"
        )


def align_to_grid(ts: "pd.Timestamp", timeframe: str) -> "pd.Timestamp":
    """
    Alinea un timestamp al inicio del período del timeframe.

    Import diferido de pandas — shared no depende de pandas en import-time.
    """
    import pandas as pd

    ms = timeframe_to_ms(timeframe)
    epoch_ms = int(ts.timestamp() * 1000)
    aligned_ms = (epoch_ms // ms) * ms
    return pd.Timestamp(aligned_ms, unit="ms", tz=ts.tz)


__all__ = [
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
]
