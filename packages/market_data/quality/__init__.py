# -*- coding: utf-8 -*-
"""
market_data/quality/
=====================
RE-EXPORT BRIDGE — módulos dispersados a sus capas canónicas.

SSOT de cada símbolo (importar directamente en código nuevo):
  QualityPipeline, QualityPipelineResult, DataTier
    → market_data.application.quality.pipeline
  DataQualityChecker, DataQualityReport, QualityIssue
    → market_data.application.quality.data_quality
  DataQualityPolicy, PolicyResult, QualityDecision, default_policy
    → market_data.domain.policies.data_quality_policy
  AnomalyRegistry, default_registry
    → market_data.infrastructure.quality.anomaly_registry

Este __init__.py es un bridge de compatibilidad para consumidores legacy.
Imports apuntan a módulos concretos (no a __init__) para evitar ciclos.
"""
# Pipeline — importar módulo concreto, no application.quality.__init__
from market_data.application.quality.pipeline import (  # noqa: F401
    QualityPipeline,
    QualityPipelineResult,
    DataTier,
    default_quality_pipeline,
)
# Policy — importar módulo concreto, no domain.policies.__init__
from market_data.domain.policies.data_quality_policy import (  # noqa: F401
    DataQualityPolicy,
    PolicyResult,
    QualityDecision,
    default_policy,
)
# Validators — importar módulo concreto
from market_data.application.quality.data_quality import (  # noqa: F401
    DataQualityChecker,
    DataQualityReport,
    QualityIssue,
)
# Registry — importar módulo concreto
from market_data.infrastructure.quality.anomaly_registry import (  # noqa: F401
    AnomalyRegistry,
    default_registry,
)

__all__ = [
    "QualityPipeline", "QualityPipelineResult", "DataTier", "default_quality_pipeline",
    "DataQualityPolicy", "PolicyResult", "QualityDecision", "default_policy",
    "DataQualityChecker", "DataQualityReport", "QualityIssue",
    "AnomalyRegistry", "default_registry",
]
