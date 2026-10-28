"""
market_data/ports/outbound/quality.py
=====================================
Shim de compatibilidad.

SSOT del contrato: market_data.ports.outbound.anomaly_registry.
Este módulo solo reexporta para no romper importadores existentes.
"""

from __future__ import annotations

from market_data.ports.outbound.anomaly_registry import AnomalyRegistryPort

__all__ = ["AnomalyRegistryPort"]
