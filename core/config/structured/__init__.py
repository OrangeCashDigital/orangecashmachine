from __future__ import annotations

"""
core/config/structured
======================

Hydra Structured Configs (dataclasses) — exclusivo para el path Hydra.

Propósito
---------
Registrar schemas tipados en el ConfigStore de Hydra para validación
temprana en tiempo de composición, antes de que OmegaConf resuelva
interpolaciones y antes de que Pydantic aplique reglas de negocio.

Consumer único: main.py (cs.store(...) para Hydra).
Si no usas Hydra, ignora este paquete — usa schema.py directamente.

Arquitectura de validación (tres capas)
---------------------------------------
    Hydra Structured Config (dataclass)   ← tipos primitivos, estructura
                ↓
    OmegaConf (merge + interpolación)     ← resolución de ${oc.env:...}
                ↓
    Pydantic AppConfig (schema.py)        ← constraints de negocio

Regla: los dataclasses aquí solo tienen tipos primitivos + Optional.
Sin lógica de negocio — eso es responsabilidad exclusiva de schema.py.
"""

from core.config.structured.pipeline import (
    PipelineConfig,
    HistoricalConfig,
    RealtimeConfig,
)
from core.config.structured.observability import (
    ObservabilityConfig,
    LoggingConfig as HydraLoggingConfig,
)

__all__ = [
    "PipelineConfig",
    "HistoricalConfig",
    "RealtimeConfig",
    "ObservabilityConfig",
    "HydraLoggingConfig",
]
