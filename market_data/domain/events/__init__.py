# -*- coding: utf-8 -*-
"""
market_data/domain/events/__init__.py
=======================================

Eventos de dominio del bounded context market_data.

Eventos presentes
-----------------
LineageEvent   — snapshot inmutable de una transición de datos entre capas
PipelineLayer  — enum de capas del pipeline (RAW / SILVER / GOLD)
LineageStatus  — enum de resultado de un lote (SUCCESS / PARTIAL / FAILED)

Domain Events (DDD)
-------------------
Un Domain Event representa algo que ocurrió en el dominio y que
otros bounded contexts pueden observar. LineageEvent es el evento
más maduro del BC: captura qué datos pasaron por qué capa, cuándo,
y con qué resultado — garantiza reproducibilidad y auditoría.

Los eventos de streaming (OHLCVBar publicado en Redis) son eventos
de infraestructura, no de dominio — viven en market_data/streaming/.

SSOT
----
PipelineLayer, LineageStatus y LineageEvent se definen aquí.
lineage/tracker.py los importa desde aquí (DIP correcto).
Antes era al revés (domain/events/ re-exportaba desde tracker.py),
lo que creaba una dependencia domain → infra — violación de Clean Architecture.

Principios
----------
OCP    — nuevos eventos se agregan sin modificar consumers existentes
SSOT   — única definición de los tipos de eventos de dominio
DIP    — tracker.py (infra) depende del dominio, nunca al revés
DDD    — eventos son inmutables (frozen=True en LineageEvent)
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
    """
    Capa del pipeline medallion a la que pertenece un evento.

    RAW    — datos crudos del exchange (Bronze)
    SILVER — datos validados y normalizados (Silver / Iceberg)
    GOLD   — features calculadas para estrategias (Gold)
    """
    RAW    = "raw"
    SILVER = "silver"
    GOLD   = "gold"


class LineageStatus(str, Enum):
    """
    Resultado del procesamiento de un lote de datos.

    SUCCESS — todos los registros procesados sin pérdida
    PARTIAL — algunos registros descartados (quality flags, dedup)
    FAILED  — lote rechazado completamente (quality REJECT, error crítico)
    """
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED  = "failed"


# ===========================================================================
# LineageEvent — Value Object inmutable
# ===========================================================================

@dataclass(frozen=True)
class LineageEvent:
    """
    Snapshot inmutable de una transición de datos entre capas del pipeline.

    Immutable (frozen=True): los eventos de lineage son hechos históricos
    que no se modifican una vez registrados.

    Attributes
    ----------
    run_id        : UUID v4 — clave de correlación entre capas del mismo lote
    layer         : capa de destino del evento
    exchange      : exchange de origen del dato
    symbol        : par de trading (e.g. "BTC/USDT")
    timeframe     : resolución temporal (e.g. "1m", "1h")
    rows_in       : filas que entraron a la transformación
    rows_out      : filas que salieron (después de filtros/drops)
    status        : resultado del procesamiento
    quality_score : puntuación de calidad [0.0–100.0]; None si no aplica
    params        : dict con parámetros relevantes para trazabilidad
    ts            : timestamp UTC del evento (auto-generado si no se pasa)

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
# __all__ — API pública explícita
# ===========================================================================

__all__ = [
    "PipelineLayer",
    "LineageStatus",
    "LineageEvent",
]
