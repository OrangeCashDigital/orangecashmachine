"""
ocm_platform/config/structured/__init__.py
==========================================

Exportaciones públicas del paquete structured config.

Hydra Structured Configs — validación de tipos en tiempo de composición,
antes de OmegaConf y antes de Pydantic.

Regla: este __init__.py es el único punto de importación autorizado
para callers externos. Nunca importar desde los submódulos directamente.
SSOT · OCP · Fail-Fast en import time.
"""
from __future__ import annotations

from ocm_platform.config.structured.pipeline import (
    PipelineConfig,
    HistoricalConfig,
    ResampleConfig,
    RealtimeConfig,
    RetryPolicyConfig,
)
from ocm_platform.config.structured.observability import ObservabilityConfig

__all__ = [
    "PipelineConfig",
    "HistoricalConfig",
    "ResampleConfig",
    "RealtimeConfig",
    "RetryPolicyConfig",
    "ObservabilityConfig",
]
