# -*- coding: utf-8 -*-
"""
market_data/processing/utils/timeframe.py
==========================================

Internal Port — SSOT de timeframe para el bounded context market_data.

Responsabilidad única
---------------------
Centralizar la dependencia de market_data/ hacia el Shared Kernel
(domain.value_objects.timeframe). Ningún módulo dentro de este BC
importa directamente desde domain/ — toda dependencia del Shared Kernel
pasa por este módulo (DIP · SSOT · Anti-Corruption Layer).

Beneficio
---------
Si el Shared Kernel cambia de ubicación, nombre o implementación,
solo este archivo necesita actualizarse — los 10+ consumidores internos
permanecen intactos (OCP).

Re-exports públicos
-------------------
Timeframe             — enum canónico de timeframes (str-compatible)
timeframe_to_ms       — conversión timeframe string → milisegundos
InvalidTimeframeError — excepción Fail-Fast de timeframe inválido
VALID_TIMEFRAMES      — frozenset[str] para validación O(1)
align_to_grid         — alineación de timestamps a la grilla del timeframe

Consumidores internos (no importar domain directamente)
-------------------------------------------------------
  ingestion/rest/ohlcv_fetcher.py
  processing/pipelines/resample_pipeline.py
  processing/strategies/backfill.py
  processing/strategies/repair.py
  processing/utils/gap_utils.py
  processing/utils/grid_alignment.py
  processing/validation/candle_validator.py
  quality/invariants/invariants.py
  quality/schemas/ohlcv_schema.py
  quality/validators/data_quality.py

Principios: DIP · SSOT · OCP · DRY · Anti-Corruption Layer · Clean Architecture
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Re-exports desde Shared Kernel — punto único de contacto con domain/
# ---------------------------------------------------------------------------
from domain.value_objects.timeframe import (  # noqa: F401
    Timeframe,
    timeframe_to_ms,
    InvalidTimeframeError,
    VALID_TIMEFRAMES,
)

# align_to_grid puede vivir en domain o definirse aquí si es market_data-específico.
# Si domain lo exporta:
try:
    from domain.value_objects.timeframe import align_to_grid  # noqa: F401
except ImportError:
    # Fallback: implementación local si el Shared Kernel aún no la expone.
    # TODO: mover a domain.value_objects.timeframe cuando sea estable.
    import pandas as pd

    def align_to_grid(ts: "pd.Timestamp", timeframe: str) -> "pd.Timestamp":
        """
        Alinea un timestamp al inicio del período del timeframe.

        Ejemplo: align_to_grid("2024-01-01 14:37:00", "1h") → "2024-01-01 14:00:00"

        Parameters
        ----------
        ts        : timestamp a alinear (tz-aware recomendado)
        timeframe : string canónico ("1m", "5m", "1h", "4h", "1d" ...)

        Returns
        -------
        pd.Timestamp alineado al grid del timeframe.
        """
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
