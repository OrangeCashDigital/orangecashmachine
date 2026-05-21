# -*- coding: utf-8 -*-
"""
market_data/application/
=========================

Capa de aplicación del bounded context market_data.

Responsabilidad
---------------
Orquestar casos de uso: coordina ports, pipelines y configuración
sin contener lógica de negocio ni lógica de infraestructura.

Regla de dependencias (Clean Architecture)
------------------------------------------
application/ → importa desde ports/ y domain/
application/ → NO importa desde adapters/ ni infrastructure/ directamente
adapters/    → implementan los ports que application/ consume

Submódulos
----------
  use_cases/   — casos de uso concretos (PipelineOrchestrator, ResampleUseCase)
  pipelines/   — pipelines de datos (OHLCVPipeline, ResamplePipeline)
  consumers/   — consumidores de streams (QualityConsumer)
  strategies/  — estrategias de ingestión (BackfillStrategy, IncrementalStrategy, RepairStrategy)

Re-exports públicos
-------------------
Exclusivamente para Composition Roots (infrastructure/dagster/assets/).
El dominio y los ports NO deben importar desde aquí.

Principios: SRP · DIP · OCP · KISS · Clean Architecture
"""

# Re-exports para Composition Roots (Dagster assets)
from market_data.application.use_cases.pipeline_orchestrator import (  # noqa: F401
    PipelineOrchestrator,
    PipelineRequest,
)
from market_data.application.use_cases.resample_ohlcv import (  # noqa: F401
    ResampleUseCase,
    ResampleRequest,
)

__all__ = [
    "PipelineOrchestrator",
    "PipelineRequest",
    "ResampleUseCase",
    "ResampleRequest",
]
