# -*- coding: utf-8 -*-
"""
market_data/application/quality/report.py
==========================================
Re-export de DataQualityReport y QualityIssue para consumidores
que importan desde application/quality/report (e.g. ge_checker.py).

SSOT: domain/quality/types.py
"""
from market_data.domain.quality.types import (  # noqa: F401
    DataQualityReport,
    QualityIssue,
)

__all__ = ["DataQualityReport", "QualityIssue"]
