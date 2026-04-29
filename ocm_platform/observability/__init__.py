"""
ocm_platform/observability/__init__.py
=======================================

Exportaciones públicas del paquete observability.

Regla: único punto de importación autorizado para callers externos.
Nunca importar desde submódulos directamente fuera de este paquete.
SSOT · OCP · Fail-Fast en import time.
"""
from __future__ import annotations

from ocm_platform.observability.logger import (
    bootstrap_logging,
    configure_logging,
    bind_pipeline,
    is_logging_configured,
    setup_logging,
)
from ocm_platform.observability.bootstrap import (
    pre_log,
    drain,
)

__all__ = [
    "bootstrap_logging",
    "configure_logging",
    "bind_pipeline",
    "is_logging_configured",
    "setup_logging",
    "pre_log",
    "drain",
]
