# -*- coding: utf-8 -*-
"""
market_data/domain/events/_lineage.py
========================================

Value Objects del sistema de lineage — SSOT de tipos de trazabilidad.

Responsabilidad
---------------
Definir los tipos inmutables que representan transiciones entre capas
del pipeline (RAW → SILVER → GOLD) y su estado de resultado.

Principios
----------
SSOT  — única definición; __init__.py re-exporta, nadie más importa directo
DDD   — value objects puros: frozen dataclass + enum, sin efectos laterales
KISS  — sin lógica de negocio; solo representación y una propiedad derivada
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


# ===========================================================================
# Enums
# ===========================================================================

class PipelineLayer(str, Enum):
    """Capa de destino de un evento de lineage."""
    RAW    = "raw"
    SILVER = "silver"
    GOLD   = "gold"


class LineageStatus(str, Enum):
    """Resultado del procesamiento de un lote en una capa."""
    SUCCESS = "success"
    PARTIAL = "partial"   # algunos registros descartados
    FAILED  = "failed"    # lote rechazado completamente


# ===========================================================================
# LineageEvent — Value Object
# ===========================================================================

@dataclass(frozen=True)
class LineageEvent:
    """
    Snapshot inmutable de una transición entre capas del pipeline.

    Fields
    ------
    run_id       : UUID v4 — correlación entre capas del mismo lote
    layer        : capa de destino (SILVER, GOLD, …)
    exchange     : exchange de origen del dato
    symbol       : par de trading (e.g. "BTC/USDT")
    timeframe    : resolución temporal (e.g. "1m", "1h")
    rows_in      : filas que entraron a la transformación
    rows_out     : filas que salieron (después de filtros/drops)
    status       : resultado del procesamiento
    quality_score: puntuación de calidad [0.0–100.0]; None si no aplica
    params       : dict con parámetros relevantes para trazabilidad
    ts           : timestamp UTC del evento (auto-generado)

    Properties
    ----------
    loss_rate : fracción de filas descartadas [0.0–1.0]
    """
    run_id:        str
    layer:         PipelineLayer
    exchange:      str
    symbol:        str
    timeframe:     str
    rows_in:       int
    rows_out:      int
    status:        LineageStatus
    quality_score: Optional[float]   = None
    params:        Dict[str, Any]    = field(default_factory=dict)
    ts:            str               = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def loss_rate(self) -> float:
        """Fracción de filas descartadas. 0.0 = ninguna pérdida."""
        if self.rows_in == 0:
            return 0.0
        return (self.rows_in - self.rows_out) / self.rows_in


# ===========================================================================
# __all__
# ===========================================================================

__all__ = [
    "PipelineLayer",
    "LineageStatus",
    "LineageEvent",
]
