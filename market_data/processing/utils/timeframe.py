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
    from market_data.processing.utils.timeframe import timeframe_to_ms, VALID_TIMEFRAMES

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

    def __str__(self) -> str:
        # Python 3.11+ cambió str(StrEnum) para mostrar el nombre.
        # Forzamos el valor para compatibilidad con f-strings y paths.
        return self.value


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

import pandas as pd


# ==========================================================
# API pública — alineación al grid temporal
# ==========================================================

def align_to_grid(
    df:        pd.DataFrame,
    timeframe: str,
    exchange:  str = "unknown",
    symbol:    str = "unknown",
) -> pd.DataFrame:
    """
    Alinea los timestamps de un DataFrame OHLCV al grid canónico
    del timeframe y agrega semánticamente las velas que colapsen
    en el mismo bucket post-floor.

    Por qué floor y no round
    ------------------------
    Round puede avanzar un timestamp al bucket siguiente, creando
    una vela "del futuro" que rompe joins y backtests. Floor siempre
    retrocede al inicio del intervalo — comportamiento correcto para
    series financieras (una vela pertenece al bucket que la abre).

    Invariantes de agregación post-floor
    -------------------------------------
    - open   → primer valor del bucket  (precio de apertura real)
    - high   → máximo                   (extremo superior real)
    - low    → mínimo                   (extremo inferior real)
    - close  → último valor             (precio de cierre real)
    - volume → suma                     (volumen total del bucket)

    Métricas emitidas (SafeOps: nunca lanza si Prometheus falla)
    -------------------------------------------------------------
    - ocm_timestamp_drift_corrected_total   → velas corregidas
    - ocm_timestamp_grid_collisions_total   → colisiones post-floor
    """
    from market_data.observability.metrics import (
        TIMESTAMP_DRIFT_CORRECTED,
        TIMESTAMP_GRID_COLLISIONS,
    )

    tf_ms     = timeframe_to_ms(timeframe)
    tf_offset = pd.tseries.frequencies.to_offset(f"{tf_ms}ms")

    # --- Floor al grid ---
    floored = df["timestamp"].dt.floor(freq=tf_offset)
    drifted = int((floored != df["timestamp"]).sum())

    if drifted > 0:
        try:
            TIMESTAMP_DRIFT_CORRECTED.labels(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            ).inc(drifted)
        except Exception:
            pass

    df = df.copy()
    df["timestamp"] = floored

    # --- Detectar colisiones post-floor ---
    collisions = int(df.duplicated(subset="timestamp", keep=False).sum())
    if collisions > 0:
        try:
            TIMESTAMP_GRID_COLLISIONS.labels(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            ).inc(collisions)
        except Exception:
            pass

    # --- Agregación semántica OHLCV por bucket ---
    # Ordenar primero garantiza que first/last sean open/close correctos
    df = df.sort_values("timestamp")

    df = (
        df.groupby("timestamp", sort=False)
        .agg(
            open=("open",     "first"),
            high=("high",     "max"),
            low=("low",       "min"),
            close=("close",   "last"),
            volume=("volume", "sum"),
        )
        .reset_index()
    )

    return df[["timestamp", "open", "high", "low", "close", "volume"]]
