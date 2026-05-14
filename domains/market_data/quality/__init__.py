# -*- coding: utf-8 -*-
"""
market_data/quality/
=====================

Bounded context de calidad de datos OHLCV.

Exports públicos
----------------
  QualityPipeline        — orquesta checks + política de decisión
  QualityPipelineResult  — resultado tipado (df, report, policy, tier)
  DataTier               — CLEAN | FLAGGED | REJECTED
  DataQualityChecker     — checks individuales sobre DataFrame
  DataQualityReport      — resultado de checks
  DataQualityPolicy      — política de scoring y decisión
  PolicyResult           — resultado de política
  QualityDecision        — ACCEPT | ACCEPT_WITH_FLAGS | REJECT
  AnomalyRegistry        — registro persistente de anomalías (SQLite)
  default_registry       — singleton de módulo (injectable en tests)
  default_quality_pipeline — singleton de módulo (injectable en tests)

Regla de dependencias (Clean Architecture)
------------------------------------------
quality/ puede importar desde:
  - domain/               (value objects puros)
  - market_data/ports/    (contratos abstractos)
  - market_data/lineage/  (trazabilidad)
  - market_data/observability/ (métricas)

quality/ NO debe importar desde:
  - market_data/adapters/  (infraestructura)
  - market_data/storage/   (infraestructura)
  - market_data/ingestion/ (infraestructura)

Principios: SRP · DIP · OCP · SSOT
"""

from market_data.quality.pipeline import (
    QualityPipeline,
    QualityPipelineResult,
    DataTier,
    default_quality_pipeline,
)
from market_data.quality.policies.data_quality_policy import (
    DataQualityPolicy,
    PolicyResult,
    QualityDecision,
    default_policy,
)
from market_data.quality.validators.data_quality import (
    DataQualityChecker,
    DataQualityReport,
    QualityIssue,
)
from market_data.quality.anomaly_registry import (
    AnomalyRegistry,
    default_registry,
)

__all__ = [
    # Pipeline
    "QualityPipeline",
    "QualityPipelineResult",
    "DataTier",
    "default_quality_pipeline",
    # Policy
    "DataQualityPolicy",
    "PolicyResult",
    "QualityDecision",
    "default_policy",
    # Validators
    "DataQualityChecker",
    "DataQualityReport",
    "QualityIssue",
    # Registry
    "AnomalyRegistry",
    "default_registry",
]
