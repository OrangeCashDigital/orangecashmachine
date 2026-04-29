# -*- coding: utf-8 -*-
"""
market_data/safety/execution_guard.py
======================================

.. deprecated::
    Shim de compatibilidad — importar desde ``ocm_platform.runtime.guard``.

    Este módulo re-exporta ExecutionGuard y ExecutionStoppedError desde
    su nuevo home canónico. Se mantiene para no romper imports existentes
    en market_data/orchestration/ durante la migración.

    Será eliminado una vez que todos los callers migren a:
        from ocm_platform.runtime.guard import ExecutionGuard
"""
from __future__ import annotations

import warnings as _warnings

_warnings.warn(
    "market_data.safety.execution_guard está deprecado. "
    "Importar desde ocm_platform.runtime.guard",
    DeprecationWarning,
    stacklevel=2,
)

from ocm_platform.runtime.guard import ExecutionGuard, ExecutionStoppedError  # noqa: F401, E402

__all__ = ["ExecutionGuard", "ExecutionStoppedError"]
