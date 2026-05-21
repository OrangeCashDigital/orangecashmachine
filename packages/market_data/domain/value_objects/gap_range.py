# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/gap_range.py
=============================================

GapRange — rango temporal de un gap detectado en un dataset OHLCV.

SSOT: definición canónica. Importar desde aquí, nunca desde __init__.

Principios: DDD · SSOT · KISS · frozen VO
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class GapRange:
    """
    Rango temporal de un gap (hueco) en un dataset OHLCV.

    Inmutable — los gaps son hechos históricos, no se modifican.

    Attributes
    ----------
    start_ms : timestamp Unix ms del último punto ANTES del gap.
    end_ms   : timestamp Unix ms del primer punto DESPUÉS del gap.
    expected : número de velas faltantes en el rango.
    run_id   : run que detectó este gap (correlación de lineage).

    Properties
    ----------
    duration_ms : duración total del gap en milisegundos.
    severity    : "low" / "medium" / "high" según velas faltantes.
    """

    start_ms: int
    end_ms: int
    expected: int
    run_id: str = ""

    def __str__(self) -> str:
        start = pd.Timestamp(self.start_ms, unit="ms", tz="UTC").isoformat()
        end = pd.Timestamp(self.end_ms, unit="ms", tz="UTC").isoformat()
        return f"Gap[{start} → {end} expected={self.expected}]"

    @property
    def duration_ms(self) -> int:
        """Duración total del gap en milisegundos."""
        return self.end_ms - self.start_ms

    @property
    def severity(self) -> str:
        """Severidad del gap: low / medium / high."""
        if self.expected <= 2:
            return "low"
        if self.expected <= 10:
            return "medium"
        return "high"


__all__ = ["GapRange"]
