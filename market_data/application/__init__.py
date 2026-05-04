# -*- coding: utf-8 -*-
"""
market_data/application/
=========================

Capa de aplicación del bounded context market_data.

Responsabilidad
---------------
Orquestar casos de uso: coordina ports, pipelines y configuración
sin contener lógica de negocio ni lógica de infraestructura.

Regla de dependencias
---------------------
application/ → importa desde ports/ y domain/
application/ → NO importa desde adapters/ ni desde storage/ directamente
adapters/    → implementan los ports que application/ consume

Submódulos
----------
  use_cases/   — casos de uso concretos (run_pipeline, run_backfill, repair)

Importar desde aquí (SSOT):
    from market_data.application import PipelineOrchestrator

Principios: SRP · DIP · OCP · KISS · Clean Architecture
"""
from market_data.application.use_cases.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineRequest,
    PipelineMode,
)

__all__ = [
    "PipelineOrchestrator",
    "PipelineRequest",
    "PipelineMode",
]
