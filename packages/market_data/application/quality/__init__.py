# -*- coding: utf-8 -*-
"""
market_data/application/quality/
==================================
Lógica de calidad de datos en capa de aplicación:
  data_quality.py  — DataQualityChecker + DataQualityReport + QualityIssue
  report.py        — alias de DataQualityReport para consumidores legacy
  pipeline.py      — QualityPipeline (orquestador) [se añade en fase 2f]
"""
from market_data.application.quality.data_quality import (  # noqa: F401
    DataQualityChecker,
    DataQualityReport,
    QualityIssue,
    DataQualityError,
)

__all__ = [
    "DataQualityChecker",
    "DataQualityReport",
    "QualityIssue",
    "DataQualityError",
]
