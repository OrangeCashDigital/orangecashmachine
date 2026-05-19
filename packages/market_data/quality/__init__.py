# -*- coding: utf-8 -*-
"""
market_data/quality/
=====================
RE-EXPORT BRIDGE — los módulos fueron dispersados a sus capas canónicas.

SSOT de cada símbolo:
  QualityPipeline, QualityPipelineResult, DataTier
    → application/quality/pipeline.py
  DataQualityChecker, DataQualityReport, QualityIssue
    → application/quality/data_quality.py
  DataQualityPolicy, PolicyResult, QualityDecision, default_policy
    → domain/policies/data_quality_policy.py
  AnomalyRegistry, default_registry
    → infrastructure/quality/anomaly_registry.py

Este __init__.py es solo un re-export bridge de compatibilidad.
No agregar lógica aquí. Deprecar en el próximo sprint de limpieza.
"""

# Pipeline (application)
from market_data.application.quality.pipeline import (  # noqa: F401
    QualityPipeline,
    QualityPipelineResult,
    DataTier,
    default_quality_pipeline,
)
# Policy (domain)
from market_data.domain.policies.data_quality_policy import (  # noqa: F401
    DataQualityPolicy,
    PolicyResult,
    QualityDecision,
    default_policy,
)
# Validators / Report (application)
from market_data.application.quality.data_quality import (  # noqa: F401
    DataQualityChecker,
    DataQualityReport,
    QualityIssue,
)
# Registry (infrastructure)
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
