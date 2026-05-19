# -*- coding: utf-8 -*-
"""
market_data/quality/pipeline.py
================================
RE-EXPORT BRIDGE — SSOT movido a application/quality/pipeline.py
No agregar lógica aquí.
"""
from market_data.application.quality.pipeline import (  # noqa: F401
    QualityPipelineResult,
    QualityPipeline,
    default_quality_pipeline,
    DataTier,
)

__all__ = [
    "QualityPipelineResult",
    "QualityPipeline",
    "default_quality_pipeline",
    "DataTier",
]
