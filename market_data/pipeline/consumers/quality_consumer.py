# -*- coding: utf-8 -*-
"""
market_data/pipeline/consumers/quality_consumer.py
====================================================

⚠️  DEPRECATED — shim de compatibilidad.

Re-exporta desde la ubicación canónica:
  market_data.application.consumers.quality_consumer

Eliminar este archivo una vez que todos los callers migren.
"""
from market_data.application.consumers.quality_consumer import (  # noqa: F401
    QualityPipelineConsumer,
)

__all__ = ["QualityPipelineConsumer"]
