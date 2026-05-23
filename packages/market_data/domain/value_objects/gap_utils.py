# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/gap_utils.py
================================================

SHIM de compatibilidad — re-exporta GapRange desde su SSOT en domain/.

GapRange es un value object puro de dominio (domain/value_objects/gap_range.py).
scan_gaps NO se re-exporta desde aquí — pertenece a application/processing/
y viola BC-03 si se importa desde domain/.

Callers que necesiten scan_gaps deben importar directamente:
    from market_data.application.processing.gap_scanner import scan_gaps
"""

from __future__ import annotations

from market_data.domain.value_objects.gap_range import GapRange  # noqa: F401

__all__ = ["GapRange"]
