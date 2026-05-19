# -*- coding: utf-8 -*-
"""
market_data/quality/schemas/invariants.py
==========================================
RE-EXPORT BRIDGE — SSOT movido a domain/quality/invariants.py
Existe porque quality/invariants/invariants.py importaba desde aquí.
"""
from market_data.domain.quality.invariants import (  # noqa: F401
    InvariantResult,
    check_dataset_invariants,
)

__all__ = ["InvariantResult", "check_dataset_invariants"]
