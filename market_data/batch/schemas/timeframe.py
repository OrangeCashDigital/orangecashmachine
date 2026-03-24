"""
timeframe.py
============

Utilidades canónicas de timeframe para OrangeCashMachine.

Responsabilidad
---------------
Definir la representación canónica de timeframes y su conversión
a milisegundos. Es conocimiento del dominio, no del fetcher.

Ubicación deliberada
--------------------
Vive en schemas/ porque es una definición del dominio (qué es un
timeframe válido y cuánto dura), no lógica de transporte o storage.
Cualquier capa del sistema puede importar desde aquí sin crear
dependencias circulares.

Uso
---
    from market_data.batch.schemas.timeframe import timeframe_to_ms, VALID_TIMEFRAMES

    ms = timeframe_to_ms("1h")   # 3_600_000
    ms = timeframe_to_ms("4h")   # 14_400_000
"""

from __future__ import annotations

from enum import Enum

# ==========================================================
# Tabla canónica de unidades → milisegundos
# ==========================================================

_UNIT_MS: dict[str, int] = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}


# ==========================================================
# Timeframe — tipo de dominio canónico
# ==========================================================
# Usar str como base permite comparar directamente con strings:
#   Timeframe.H1 == "1h"  → True
#   f"timeframe={Timeframe.H1}"  → "timeframe=1h"
# Esto mantiene compatibilidad con paths de filesystem y manifests JSON
# sin necesidad de llamar a .value en cada uso.
# ==========================================================

class Timeframe(str, Enum):
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


# Frozenset de strings para validación rápida O(1) — compatible con código legacy
VALID_TIMEFRAMES: frozenset[str] = frozenset(tf.value for tf in Timeframe)


# ==========================================================
# Exceptions
# ==========================================================

class InvalidTimeframeError(ValueError):
    """Timeframe no reconocido o con formato inválido."""

    def __init__(self, timeframe: str) -> None:
        super().__init__(
            f"Timeframe inválido: '{timeframe}'. "
            f"Válidos: {sorted(VALID_TIMEFRAMES)}"
        )
        self.timeframe = timeframe


# ==========================================================
# API pública
# ==========================================================

def timeframe_to_ms(timeframe: str) -> int:
    """
    Convierte un timeframe string a milisegundos.

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
    >>> timeframe_to_ms("1h")
    3600000
    >>> timeframe_to_ms("4h")
    14400000
    """
    try:
        quantity = int(timeframe[:-1])
        unit     = timeframe[-1]
        if unit not in _UNIT_MS:
            raise InvalidTimeframeError(timeframe)
        return quantity * _UNIT_MS[unit]
    except (ValueError, IndexError):
        raise InvalidTimeframeError(timeframe)
