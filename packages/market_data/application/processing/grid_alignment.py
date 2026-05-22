# -*- coding: utf-8 -*-
"""
market_data/application/processing/grid_alignment.py
===================================================

Servicio de alineación temporal de datos OHLCV al grid canónico.

Responsabilidad
---------------
Alinear timestamps de un DataFrame OHLCV al intervalo del timeframe
y agregar semánticamente las velas que colapsen en el mismo bucket.

Separación de timeframe.py
--------------------------
align_to_grid fue separada de timeframe.py porque requiere una librería
de DataFrames. timeframe.py es un value object puro: solo define qué es
un timeframe y su duración en ms. No debe conocer polars.

Migración Fase 2
----------------
Migrado de pd.DataFrame → pl.DataFrame (polars 1.x).
- pd.tseries.frequencies.to_offset()  →  timedelta(milliseconds=tf_ms)
- df.sort_values("timestamp")          →  df.sort("timestamp")
- df["timestamp"].dt.floor()           →  df["timestamp"].dt.truncate()
- df.duplicated(...).sum()             →  df["timestamp"].is_duplicated().sum()
- df.groupby(...).agg(...)             →  df.group_by(...).agg(...)
- df[["timestamp", ...]]               →  df.select([...])

Por qué truncate y no round
---------------------------
round() puede avanzar un timestamp al bucket siguiente, creando
una vela "del futuro" que rompe joins y backtests. truncate() siempre
retrocede al inicio del intervalo — comportamiento correcto para
series financieras (una vela pertenece al bucket que la abre).

Invariantes de agregación post-truncate
----------------------------------------
- open   → primer valor del bucket  (precio de apertura real)
- high   → máximo                   (extremo superior real)
- low    → mínimo                   (extremo inferior real)
- close  → último valor             (precio de cierre real)
- volume → suma                     (volumen total del bucket)

Callbacks de observabilidad (DIP — sin imports de infraestructura)
------------------------------------------------------------------
on_drift(count)     → llamado cuando se detectan y corrigen timestamps
on_collision(count) → llamado cuando se detectan colisiones post-truncate

Los callers en application/ inyectan los callbacks de Prometheus.
En dominio puro (tests, otros value objects): no se pasa ningún callback.

Principios
----------
SRP    — este módulo hace una sola cosa: alinear al grid
DIP    — Prometheus se recibe por callback (no por import)
KISS   — lógica lineal sin estado
SafeOps — callbacks en try/except; nunca lanzan
"""

from __future__ import annotations

from datetime import timedelta
from typing import Callable, Optional

import polars as pl

from market_data.domain.value_objects.timeframe import timeframe_to_ms


def align_to_grid(
    df: pl.DataFrame,
    timeframe: str,
    exchange: str = "unknown",
    symbol: str = "unknown",
    on_drift: Optional[Callable[[int], None]] = None,
    on_collision: Optional[Callable[[int], None]] = None,
) -> pl.DataFrame:
    """
    Alinea los timestamps de un DataFrame OHLCV al grid canónico
    del timeframe y agrega semánticamente las velas que colapsen
    en el mismo bucket post-truncate.

    Parameters
    ----------
    df           : DataFrame con columnas [timestamp, open, high, low, close, volume].
                   timestamp debe ser Datetime con timezone UTC.
    timeframe    : Intervalo temporal (ej: "1m", "4h", "1d").
    exchange     : Nombre del exchange — solo para callbacks de observabilidad.
    symbol       : Par de trading — solo para callbacks de observabilidad.
    on_drift     : Callback opcional invocado con el conteo de timestamps corregidos.
                   Firma: (count: int) -> None
                   Inyectado desde application/ con la llamada a Prometheus.
    on_collision : Callback opcional invocado con el conteo de colisiones post-truncate.
                   Firma: (count: int) -> None
                   Inyectado desde application/ con la llamada a Prometheus.

    Returns
    -------
    pl.DataFrame
        Columnas: [timestamp, open, high, low, close, volume].
        Sin duplicados de timestamp. Ordenado por timestamp.

    Raises
    ------
    InvalidTimeframeError
        Si el timeframe no es reconocido por timeframe_to_ms().
    """
    tf_ms = timeframe_to_ms(timeframe)
    tf_delta = timedelta(milliseconds=tf_ms)

    # ── Truncate al grid ────────────────────────────────────────────────────
    # truncate() es el equivalente polars de floor(): retrocede al inicio del bucket.
    floored = df["timestamp"].dt.truncate(tf_delta)
    drifted = int((floored != df["timestamp"]).sum())

    if drifted > 0 and on_drift is not None:
        try:
            on_drift(drifted)
        except Exception:
            pass  # SafeOps: callback de observabilidad — nunca interrumpe el procesamiento

    df = df.with_columns(floored.alias("timestamp"))

    # ── Detectar colisiones post-truncate ───────────────────────────────────
    # is_duplicated() → True para cada elemento que aparece más de una vez.
    collisions = int(df["timestamp"].is_duplicated().sum())
    if collisions > 0 and on_collision is not None:
        try:
            on_collision(collisions)
        except Exception:
            pass  # SafeOps: callback de observabilidad — nunca interrumpe el procesamiento

    # ── Agregación semántica OHLCV por bucket ──────────────────────────────
    # Ordenar antes garantiza que first/last sean open/close correctos.
    df = df.sort("timestamp")

    df = df.group_by("timestamp", maintain_order=True).agg(
        pl.col("open").first(),
        pl.col("high").max(),
        pl.col("low").min(),
        pl.col("close").last(),
        pl.col("volume").sum(),
    )

    return df.select(["timestamp", "open", "high", "low", "close", "volume"])
