# -*- coding: utf-8 -*-
"""
market_data/domain/policies/repair.py
======================================
Shim de compatibilidad — re-exporta desde la ubicación canónica.

Ubicación canónica: market_data.application.strategies.repair
Este módulo existe para tests e imports legacy (domain.policies.repair).

Principios: DRY · SSOT · backward-compatibility
"""
from __future__ import annotations

from market_data.application.strategies.repair import (  # noqa: F401
    RepairStrategy,
    _MAX_HEALABLE_GAP_CANDLES,
)

__all__ = ["RepairStrategy", "_MAX_HEALABLE_GAP_CANDLES"]
