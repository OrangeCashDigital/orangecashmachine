# -*- coding: utf-8 -*-
"""
market_data/domain/services/__init__.py
=========================================

Servicios de dominio del bounded context market_data.

Servicios presentes
-------------------
DataQualityPolicy     — política de calidad: dado un QualityReport, decide
QualityDecision       — enum de decisión (ACCEPT / ACCEPT_WITH_FLAGS / REJECT)
PolicyResult          — resultado estructurado de aplicar la política
QualityPipeline       — orquestador: checker → policy → gaps → lineage
QualityPipelineResult — resultado completo del pipeline de calidad

Sobre Domain Services (DDD)
----------------------------
Un Domain Service implementa lógica de negocio que no pertenece
naturalmente a una sola entidad. Estos servicios coordinan múltiples
reglas, entidades y ports para producir decisiones de negocio.

Principios
----------
SRP    — cada servicio tiene una única responsabilidad de orquestación
DIP    — servicios dependen de puertos (ABCs/Protocols), no de infra
SSOT   — re-exports desde owners; sin duplicar lógica
"""
from __future__ import annotations

from market_data.quality.policies.data_quality_policy import (  # noqa: F401
    DataQualityPolicy,
    QualityDecision,
    PolicyResult,
    default_policy,
)
from market_data.quality.pipeline import (  # noqa: F401
    QualityPipeline,
    QualityPipelineResult,
    default_quality_pipeline,
)

__all__ = [
    "DataQualityPolicy",
    "QualityDecision",
    "PolicyResult",
    "default_policy",
    "QualityPipeline",
    "QualityPipelineResult",
    "default_quality_pipeline",
]
