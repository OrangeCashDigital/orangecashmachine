# -*- coding: utf-8 -*-
"""
market_data/processing/utils/grid_alignment.py
===============================================

Servicio de alineación temporal de datos OHLCV al grid canónico.

Responsabilidad
---------------
Alinear timestamps de un DataFrame OHLCV al intervalo del timeframe
y agregar semánticamente las velas que colapsen en el mismo bucket.

Separación de timeframe.py
--------------------------
align_to_grid fue separada de timeframe.py porque necesita:
  1. pandas (dependencia de datos)
  2. market_data.observability.metrics (Prometheus)

Estas dependencias son de application layer, no de dominio puro.
timeframe.py es un value object: solo define qué es un timeframe
y su duración en ms. No debe conocer pandas ni Prometheus.

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

Principios
----------
SRP    — este módulo hace una sola cosa: alinear al grid
DIP    — Prometheus se importa localmente (fail-soft, no en module-level)
KISS   — lógica lineal sin estado
SafeOps — todos los paths de Prometheus están en try/except
"""
from __future__ import annotations

import pandas as pd

from domain.value_objects.timeframe import timeframe_to_ms


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

    Parameters
    ----------
    df        : DataFrame con columnas [timestamp, open, high, low, close, volume].
                timestamp debe ser datetime64[ns, UTC].
    timeframe : Intervalo temporal (ej: "1m", "4h", "1d").
    exchange  : Nombre del exchange — solo para métricas Prometheus.
    symbol    : Par de trading — solo para métricas Prometheus.

    Returns
    -------
    pd.DataFrame
        Columnas: [timestamp, open, high, low, close, volume].
        Sin duplicados de timestamp. Ordenado por timestamp.

    Raises
    ------
    InvalidTimeframeError
        Si el timeframe no es reconocido por timeframe_to_ms().
    """
    tf_ms     = timeframe_to_ms(timeframe)
    tf_offset = pd.tseries.frequencies.to_offset(f"{tf_ms}ms")

    # ── Floor al grid ──────────────────────────────────────────────────────
    floored = df["timestamp"].dt.floor(freq=tf_offset)
    drifted = int((floored != df["timestamp"]).sum())

    if drifted > 0:
        try:
            from market_data.observability.metrics import TIMESTAMP_DRIFT_CORRECTED
            TIMESTAMP_DRIFT_CORRECTED.labels(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            ).inc(drifted)
        except Exception:
            pass  # SafeOps: Prometheus no disponible — degradar silenciosamente

    df = df.copy()
    df["timestamp"] = floored

    # ── Detectar colisiones post-floor ─────────────────────────────────────
    collisions = int(df.duplicated(subset="timestamp", keep=False).sum())
    if collisions > 0:
        try:
            from market_data.observability.metrics import TIMESTAMP_GRID_COLLISIONS
            TIMESTAMP_GRID_COLLISIONS.labels(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            ).inc(collisions)
        except Exception:
            pass  # SafeOps: Prometheus no disponible — degradar silenciosamente

    # ── Agregación semántica OHLCV por bucket ──────────────────────────────
    # Ordenar antes garantiza que first/last sean open/close correctos.
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
