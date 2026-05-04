# -*- coding: utf-8 -*-
"""
domain/value_objects/timeframe.py
==================================

Value object canónico de Timeframe para OrangeCashMachine.

SSOT único de:
  - representación de timeframes válidos (Timeframe enum)
  - conversión a milisegundos (timeframe_to_ms)
  - validación de formato (InvalidTimeframeError, VALID_TIMEFRAMES)

Regla de dependencias
---------------------
Este módulo NO importa desde market_data/, trading/, ni ningún
bounded context. Solo stdlib. Es el núcleo del Shared Kernel.

Compatibilidad str
------------------
Timeframe hereda de str para comparar directamente con strings externos:
    Timeframe.H1 == "1h"   → True
    f"tf={Timeframe.H1}"   → "tf=1h"
Elimina casting explícito en paths de filesystem y manifests JSON.

Migración desde legacy
----------------------
Antes:  from market_data.processing.utils.timeframe import timeframe_to_ms
Ahora:  from domain.value_objects.timeframe import timeframe_to_ms

El legacy (market_data/processing/utils/timeframe.py) redirige aquí
como tombstone — no rompe imports existentes durante la migración.

Principios: DDD · SSOT · Fail-Fast · OCP · KISS
"""
from __future__ import annotations

from enum import Enum

# ===========================================================================
# Tabla canónica de unidades → milisegundos
# SSOT: única definición en todo el sistema.
# ===========================================================================

_UNIT_MS: dict[str, int] = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}


# ===========================================================================
# Timeframe — value object canónico
# ===========================================================================

class Timeframe(str, Enum):
    """
    Timeframes soportados por OCM.

    Hereda de str: Timeframe.H1 == "1h" es True sin .value.
    OCP: añadir un timeframe nuevo no modifica el contrato existente.
    """
    M1  = "1m"
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
    W1  = "1w"

    def __str__(self) -> str:
        # Python 3.11+ cambió str(StrEnum) al nombre del miembro.
        # Forzamos el valor para compatibilidad con f-strings y paths.
        return self.value

    @property
    def seconds(self) -> int:
        """Duración del timeframe en segundos."""
        return timeframe_to_ms(self.value) // 1000

    @property
    def milliseconds(self) -> int:
        """Duración del timeframe en milisegundos."""
        return timeframe_to_ms(self.value)

    @classmethod
    def from_str(cls, value: str) -> "Timeframe":
        """
        Construye un Timeframe desde string.

        Fail-Fast: lanza InvalidTimeframeError si el valor no es válido.

        Examples
        --------
        >>> Timeframe.from_str("1h")
        <Timeframe.H1: '1h'>
        >>> Timeframe.from_str("99x")
        InvalidTimeframeError: Timeframe inválido: '99x'. Válidos: [...]
        """
        try:
            return cls(value)
        except ValueError:
            raise InvalidTimeframeError(value)


# Frozenset de strings para validación rápida O(1).
# Compatible con código legacy que valida con `if tf in VALID_TIMEFRAMES`.
VALID_TIMEFRAMES: frozenset[str] = frozenset(tf.value for tf in Timeframe)


# ===========================================================================
# Exceptions
# ===========================================================================

class InvalidTimeframeError(ValueError):
    """
    Timeframe no reconocido o con formato inválido.

    Fail-Fast: lanzada en construcción, no en runtime tardío.
    """

    def __init__(self, timeframe: str) -> None:
        super().__init__(
            f"Timeframe inválido: '{timeframe}'. "
            f"Válidos: {sorted(VALID_TIMEFRAMES)}"
        )
        self.timeframe = timeframe


# ===========================================================================
# API pública — conversión a milisegundos
# ===========================================================================

def timeframe_to_ms(timeframe: str) -> int:
    """
    Convierte un timeframe string a milisegundos.

    Acepta tanto strings crudos ("1h") como instancias de Timeframe
    (Timeframe.H1) porque Timeframe hereda de str.

    Parameters
    ----------
    timeframe : str
        Ej: "1m", "4h", "1d"

    Returns
    -------
    int
        Duración en milisegundos.

    Raises
    ------
    InvalidTimeframeError
        Si el formato es inválido o la unidad no está soportada.

    Examples
    --------
    >>> timeframe_to_ms("1m")
    60000
    >>> timeframe_to_ms("4h")
    14400000
    >>> timeframe_to_ms(Timeframe.H1)
    3600000
    """
    try:
        quantity = int(timeframe[:-1])
        unit     = timeframe[-1]
        if unit not in _UNIT_MS:
            raise InvalidTimeframeError(timeframe)
        return quantity * _UNIT_MS[unit]
    except (ValueError, IndexError):
        raise InvalidTimeframeError(timeframe)
