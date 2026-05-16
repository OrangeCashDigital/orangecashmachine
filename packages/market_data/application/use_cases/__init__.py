# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/
====================================

Casos de uso del bounded context market_data.

Cada módulo implementa exactamente un caso de uso (SRP).
Los casos de uso orquestan ports y pipelines — nunca contienen
lógica de negocio ni acceden a infraestructura directamente.

Casos de uso disponibles
------------------------
  pipeline_orchestrator — PipelineOrchestrator, PipelineRequest, PipelineMode
  resample_ohlcv        — ResampleUseCase, ResampleRequest, ResampleUseCaseResult

Importar siempre desde el submódulo específico:
    from market_data.application.use_cases.pipeline_orchestrator import PipelineOrchestrator
    from market_data.application.use_cases.resample_ohlcv        import ResampleUseCase

Este __init__.py está intencionalmente vacío de re-exports.

Principios: SRP · DIP · KISS
"""
