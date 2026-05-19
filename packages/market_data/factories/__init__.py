# -*- coding: utf-8 -*-
"""
market_data/factories/
=======================
RE-EXPORT BRIDGE — SSOT movido a infrastructure/bootstrap/pipeline_factory.py

Consumidores legacy:
  infrastructure/bootstrap/container.py (lazy import en línea 157)
"""
from market_data.infrastructure.bootstrap.pipeline_factory import (  # noqa: F401
    ConcretePipelineFactory,
)

__all__ = ["ConcretePipelineFactory"]
