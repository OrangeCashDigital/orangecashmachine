# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/gap_scanner.py
================================================

Utilidad de detección de huecos temporales en DataFrames OHLCV.

Migración Fase 2
----------------
Migrado de pd.DataFrame → pl.DataFrame (polars 1.x).
La interfaz pública es idéntica; solo cambia el tipo del parámetro df.
dt.timestamp("ms") normaliza a epoch ms independiente de tz-aware/naive.

Responsabilidad
---------------
Única: implementar `scan_gaps()` — detectar huecos temporales en un
DataFrame OHLCV ordenado y retornar una lista de GapRange.

No valida calidad, no escribe, no hace I/O — SRP estricto.

Dependencia de dominio
----------------------
GapRange se importa desde domain/value_objects (DIP · SSOT).
Este módulo depende del dominio, nunca al revés.

Historial
---------
Extraído de repair.py para romper el import circular con quality/pipeline.py.
Ambos módulos importan desde processing/utils/ — sin dependencias cruzadas.

Principios
----------
SRP    — solo detecta gaps, no actúa sobre ellos
DIP    — GapRange importado desde domain/value_objects
KISS   — algoritmo lineal O(n), sin dependencias externas
DRY    — lógica de threshold centralizada en una sola constante
"""

from __future__ import annotations

from typing import List

import polars as pl

# GapRange importado desde domain — DIP correcto (utils depende del dominio)
from market_data.domain.value_objects import GapRange  # noqa: F401
from market_data.domain.value_objects.timeframe import timeframe_to_ms

# Tolerancia: gap debe ser ≥ 2× el timeframe para ser detectado.
# Evita falsos positivos por microsegundos de desfase en timestamps.
_GAP_FACTOR = 2


def scan_gaps(
    df: pl.DataFrame,
    timeframe: str,
    tolerance: int = 0,
) -> List[GapRange]:
    """
    Detecta huecos temporales en un DataFrame OHLCV.

    Parameters
    ----------
    df        : DataFrame con columna 'timestamp' (Datetime UTC o epoch ms)
    timeframe : intervalo canónico ("1m", "1h", …)
    tolerance : velas adicionales de tolerancia antes de declarar gap
                (tolerance=0 → threshold = 2× timeframe)

    Returns
    -------
    Lista de GapRange ordenada cronológicamente.
    Lista vacía = sin huecos detectados.

    Complejidad
    -----------
    O(n) — una pasada sobre los timestamps ordenados.
    """
    if df is None or df.is_empty() or len(df) < 2:
        return []

    tf_ms = timeframe_to_ms(timeframe)
    threshold = tf_ms * (_GAP_FACTOR + tolerance)

    df_sorted = df.sort("timestamp")

    # dt.timestamp("ms") → epoch ms Int64, independiente de tz-aware o naive.
    ts_ms = df_sorted["timestamp"].dt.timestamp("ms").to_numpy()

    gaps: List[GapRange] = []
    for i in range(len(ts_ms) - 1):
        delta = int(ts_ms[i + 1]) - int(ts_ms[i])
        if delta >= threshold:
            expected = delta // tf_ms - 1
            gaps.append(
                GapRange(
                    start_ms=int(ts_ms[i]),
                    end_ms=int(ts_ms[i + 1]),
                    expected=int(expected),
                )
            )
    return gaps


__all__ = [
    "GapRange",  # re-export backward-compat
    "scan_gaps",
]
