# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/timeframe.py
===============================================

SSOT canónico de timeframe para el bounded context market_data.

Responsabilidad
---------------
Definir el vocabulario de timeframe como Value Object de dominio puro.
Sin dependencias externas — solo stdlib. Importable desde cualquier capa.

Exports
-------
Timeframe             — enum canónico (str-compatible, hashable, O(1) lookup)
timeframe_to_ms       — conversión timeframe → milisegundos (O(1), tabla)
InvalidTimeframeError — excepción Fail-Fast; lanza si timeframe desconocido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alinea timestamp al inicio del período del timeframe

Principios
----------
DDD    — Value Object puro: inmutable, definido por valor, sin identidad
SSOT   — única fuente de verdad; processing/utils/timeframe.py re-exporta aquí
KISS   — tabla de lookup, sin regex ni parsing dinámico
Fail-Fast — timeframe desconocido → InvalidTimeframeError inmediato
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
    El caller debe proveer un timeframe válido o manejar el error explícitamente.
    """


# ===========================================================================
# Lookup table — SSOT de conversión timeframe → ms
# ===========================================================================

#: Mapeo canónico timeframe string → milisegundos.
#: O(1) lookup. Extender aquí, nunca en callers.
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
    "1M":  30 * 24 * 60 * 60_000,  # aproximación; suficiente para gaps/grids
}

#: frozenset para validación O(1) sin instanciar Timeframe.
VALID_TIMEFRAMES: frozenset[str] = frozenset(_TF_TO_MS.keys())


# ===========================================================================
# Timeframe — Value Object enum
# ===========================================================================

class Timeframe(str, Enum):
    """
    Enum canónico de timeframes soportados.

    str-compatible: Timeframe.H1 == "1h" → True.
    Permite uso directo como string en CCXT, pandas, logs y YAML config.
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
        """
        Construye un Timeframe desde string.

        Fail-Fast: lanza InvalidTimeframeError si el string no es reconocido.
        """
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
    Convierte un timeframe string a milisegundos.

    O(1) — tabla de lookup directa.
    Fail-Fast: lanza InvalidTimeframeError si el timeframe no existe.

    Parameters
    ----------
    timeframe : str — e.g. "1m", "1h", "4h", "1d"

    Returns
    -------
    int — duración en milisegundos

    Raises
    ------
    InvalidTimeframeError — si el timeframe no está en VALID_TIMEFRAMES
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

    Ejemplo
    -------
    align_to_grid(Timestamp("2024-01-01 14:37:00"), "1h")
    → Timestamp("2024-01-01 14:00:00+00:00")

    Parameters
    ----------
    ts        : pd.Timestamp — timestamp a alinear (tz-aware recomendado)
    timeframe : str          — timeframe canónico ("1m", "1h", "4h", "1d" …)

    Returns
    -------
    pd.Timestamp alineado al grid del timeframe, misma tz que ts.

    Raises
    ------
    InvalidTimeframeError — si el timeframe no es válido
    """
    import pandas as pd  # import diferido — domain no depende de pandas en import-time

    ms = timeframe_to_ms(timeframe)
    epoch_ms = int(ts.timestamp() * 1000)
    aligned_ms = (epoch_ms // ms) * ms
    return pd.Timestamp(aligned_ms, unit="ms", tz=ts.tz)


# ===========================================================================
# __all__
# ===========================================================================

__all__ = [
    "Timeframe",
    "timeframe_to_ms",
    "InvalidTimeframeError",
    "VALID_TIMEFRAMES",
    "align_to_grid",
]
