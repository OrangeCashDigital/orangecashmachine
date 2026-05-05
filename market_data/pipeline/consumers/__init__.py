# -*- coding: utf-8 -*-
"""
market_data/pipeline/consumers/
=================================

⚠️  DEPRECATED — usar market_data.application.consumers directamente.

Este módulo es un shim de compatibilidad. Se eliminará en el próximo
ciclo de refactoring una vez que todos los callers migren a:
  from market_data.application.consumers import BaseConsumer, QualityPipelineConsumer

Principios: OCP (callers existentes no se rompen durante migración)
"""
# Re-export desde nueva ubicación canónica (SSOT)
from market_data.application.consumers.base             import BaseConsumer             # noqa: F401
from market_data.application.consumers.quality_consumer import QualityPipelineConsumer  # noqa: F401

__all__ = [
    "BaseConsumer",
    "QualityPipelineConsumer",
]
