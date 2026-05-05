# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/
====================================

Casos de uso del bounded context market_data.

Cada módulo implementa exactamente un caso de uso (SRP).
Los casos de uso orquestan ports y pipelines — nunca contienen
lógica de negocio ni acceden a infraestructura directamente.

Casos de uso activos
--------------------
  PipelineOrchestrator — selecciona y ejecuta el pipeline correcto
                         según exchange, market_type y mode.

Principios: SRP · DIP · KISS
"""
from market_data.application.use_cases.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineRequest,
    PipelineMode,
)
from market_data.application.use_cases.resample_ohlcv import (
    ResampleUseCase,
    ResampleRequest,
    ResampleUseCaseResult,
)

__all__ = [
    "PipelineOrchestrator",
    "PipelineRequest",
    "PipelineMode",
    "ResampleUseCase",
    "ResampleRequest",
    "ResampleUseCaseResult",
]
