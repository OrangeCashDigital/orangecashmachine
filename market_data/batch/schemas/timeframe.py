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

# ==========================================================
# Tabla canónica de unidades → milisegundos
# ==========================================================

_UNIT_MS: dict[str, int] = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}

# Timeframes válidos en el sistema — fuente de verdad única.
# Si agregas un timeframe nuevo, agrégalo aquí primero.
VALID_TIMEFRAMES: frozenset[str] = frozenset({
    "1m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "1w",
})


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
