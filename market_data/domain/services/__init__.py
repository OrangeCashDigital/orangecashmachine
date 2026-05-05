# -*- coding: utf-8 -*-
"""
market_data/domain/services/__init__.py
=========================================

Servicios de dominio del bounded context market_data.

Servicios presentes
-------------------
DataQualityPolicy   — política de calidad: dado un QualityReport, decide aceptar/rechazar
QualityDecision     — enum de decisión (ACCEPT, REJECT, QUARANTINE)
PolicyResult        — resultado estructurado de aplicar la política

Sobre Domain Services (DDD)
----------------------------
Un Domain Service implementa lógica de negocio que no pertenece
naturalmente a una sola entidad. DataQualityPolicy coordina múltiples
reglas de calidad y produce una decisión de negocio — es un servicio,
no una entidad ni una regla atómica.

Servicios pendientes de migrar aquí (TODO)
------------------------------------------
- OHLCVTransformer  → actualmente en processing/transformer.py
                      orquesta validación + normalización + lineage
- QualityPipeline   → actualmente en quality/pipeline.py
                      orquesta múltiples validators y produce QualityPipelineResult

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
)

__all__ = [
    "DataQualityPolicy",
    "QualityDecision",
    "PolicyResult",
]
