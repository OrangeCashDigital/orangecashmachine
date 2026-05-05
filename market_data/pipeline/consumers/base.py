# -*- coding: utf-8 -*-
"""
market_data/pipeline/consumers/base.py
========================================

⚠️  DEPRECATED — shim de compatibilidad.

Re-exporta desde la ubicación canónica:
  market_data.application.consumers.base

Eliminar este archivo una vez que todos los callers migren.
"""
from market_data.application.consumers.base import BaseConsumer  # noqa: F401

__all__ = ["BaseConsumer"]
