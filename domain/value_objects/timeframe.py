# -*- coding: utf-8 -*-
"""
domain/value_objects/timeframe.py
===================================

Timeframe — value object canónico para intervalos temporales.

Por qué existe
--------------
En todo OCM, timeframe se pasaba como str desnudo: "1h", "4h", "1d".
Esto permite valores ilegales en tiempo de construcción, hace que
la validación se repita en cada capa (DRY violation) y elimina la
capacidad de derivar propiedades (segundos, periodos/año) sin SSOT.

Este enum resuelve los tres problemas:
  - Fail-Fast en construcción: Timeframe("5x") → ValueError inmediato
  - SSOT de segundos por periodo y factor de anualización (crypto 24/7)
  - Comparable, hashable, serializable como str canónico

Compatibilidad
--------------
Timeframe("1h") == "1h"   → False  (son tipos distintos, usar .value)
str(Timeframe.H1)         → "1h"   (via __str__)
Timeframe.H1.value        → "1h"   (acceso directo al str canónico)

Uso en pipelines existentes
----------------------------
    # Antes (frágil):
    timeframe: str = "1h"

    # Ahora (fail-fast):
    timeframe: Timeframe = Timeframe.H1

    # Para inyectar en APIs externas que esperan str:
    adapter.fetch_ohlcv(timeframe=Timeframe.H1.value)

Principios: DDD · SSOT · Fail-Fast · KISS · OCP
"""
from __future__ import annotations

from enum import Enum


class Timeframe(str, Enum):
    """
    Intervalos temporales canónicos soportados por OCM.

    Hereda de str para que isinstance(tf, str) sea True —
    compatibilidad con APIs externas que esperan str sin casting explícito.
    El valor del enum ES el string canónico (e.g. Timeframe.H1 == "1h").

    Propiedades derivadas
    ---------------------
    seconds       — segundos por periodo (para cálculos de offset)
    periods_per_year — factor de anualización crypto 24/7 (365d × 24h × ...)
    is_intraday   — True para timeframes < 1d

    Extensión (OCP)
    ---------------
    Agregar "3d", "2w" etc. no modifica el contrato existente —
    solo añadir el miembro y los cases en los dicts internos.
    """

    M1  = "1m"
    M5  = "5m"
    M15 = "15m"
    M30 = "30m"
    H1  = "1h"
    H4  = "4h"
    H8  = "8h"
    D1  = "1d"
    W1  = "1w"

    # ------------------------------------------------------------------
    # Derivadas — SSOT de constantes temporales
    # ------------------------------------------------------------------

    @property
    def seconds(self) -> int:
        """Segundos por periodo. Usado para calcular offsets y ventanas."""
        _MAP: dict[str, int] = {
            "1m":    60,
            "5m":   300,
            "15m":  900,
            "30m": 1_800,
            "1h":  3_600,
            "4h": 14_400,
            "8h": 28_800,
            "1d": 86_400,
            "1w": 604_800,
        }
        return _MAP[self.value]

    @property
    def periods_per_year(self) -> int:
        """
        Periodos por año calendario (crypto 24/7, 365 días).

        SSOT del factor de anualización — reemplaza _ANNUALIZATION_MAP
        disperso en gold/transformer.py y otros módulos.

        Calculado como: 365 * 24 * 3600 / seconds
        Valores almacenados explícitamente para claridad y evitar
        errores de redondeo en divisiones enteras.
        """
        _MAP: dict[str, int] = {
            "1m":  525_600,
            "5m":  105_120,
            "15m":  35_040,
            "30m":  17_520,
            "1h":    8_760,
            "4h":    2_190,
            "8h":    1_095,
            "1d":      365,
            "1w":       52,
        }
        return _MAP[self.value]

    @property
    def is_intraday(self) -> bool:
        """True si el timeframe es menor a un día completo."""
        return self.seconds < 86_400

    # ------------------------------------------------------------------
    # Conversión y representación
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Timeframe({self.value!r})"

    @classmethod
    def from_str(cls, value: str) -> "Timeframe":
        """
        Construye un Timeframe desde string. Fail-Fast si inválido.

        Preferir sobre Timeframe(value) cuando el string viene de input
        externo y se quiere un error explícito con mensaje claro.

        Raises
        ------
        ValueError
            Si value no corresponde a ningún Timeframe soportado.
        """
        try:
            return cls(value)
        except ValueError:
            valid = [tf.value for tf in cls]
            raise ValueError(
                f"Timeframe inválido: {value!r}. "
                f"Valores soportados: {valid}"
            ) from None
