# -*- coding: utf-8 -*-
"""
market_data/domain/quality/
============================
Invariantes y contratos formales del dominio OHLCV.
"""
from market_data.domain.quality.invariants import (  # noqa: F401
    InvariantResult,
    check_dataset_invariants,
)

__all__ = ["InvariantResult", "check_dataset_invariants"]
