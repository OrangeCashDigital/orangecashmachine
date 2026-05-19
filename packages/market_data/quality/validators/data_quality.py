# -*- coding: utf-8 -*-
"""
market_data/quality/validators/data_quality.py
===============================================
RE-EXPORT BRIDGE — SSOT movido a application/quality/data_quality.py
No agregar lógica aquí.
"""
from market_data.application.quality.data_quality import (  # noqa: F401
    QualityIssue,
    DataQualityReport,
    DataQualityError,
    DataQualityChecker,
)

__all__ = [
    "QualityIssue",
    "DataQualityReport",
    "DataQualityError",
    "DataQualityChecker",
]
