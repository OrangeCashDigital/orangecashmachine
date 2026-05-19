# -*- coding: utf-8 -*-
"""
market_data/quality/anomaly_registry.py
========================================
RE-EXPORT BRIDGE — SSOT movido a infrastructure/quality/anomaly_registry.py
No agregar lógica aquí.
"""
from market_data.infrastructure.quality.anomaly_registry import (  # noqa: F401
    AnomalyRegistry,
    default_registry,
)

# Alias legacy — quality_consumer.py importa _registry directamente
_registry = default_registry

__all__ = ["AnomalyRegistry", "default_registry", "_registry"]
