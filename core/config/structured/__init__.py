from __future__ import annotations
"""
core/config/structured
======================
Hydra Structured Configs (dataclasses).

Arquitectura de validación:
    Hydra Structured Config (dataclass)  ← tipado temprano, estructura
            ↓
    OmegaConf (merge + interpolación)    ← resolución de ${oc.env:...}
            ↓
    Pydantic (validación final + reglas) ← constraints de negocio

No reemplaza Pydantic — lo complementa.
"""
from core.config.structured.pipeline import PipelineConfig, HistoricalConfig, RealtimeConfig
from core.config.structured.observability import ObservabilityConfig, LoggingConfig as HydraLoggingConfig

__all__ = [
    "PipelineConfig",
    "HistoricalConfig",
    "RealtimeConfig",
    "ObservabilityConfig",
    "HydraLoggingConfig",
]
