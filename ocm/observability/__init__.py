"""
ocm/observability/__init__.py
=======================================

Exportaciones públicas del paquete observability.

Regla: único punto de importación autorizado para callers externos.
Nunca importar desde submódulos directamente fuera de este paquete.
SSOT · OCP · Fail-Fast en import time.
"""

from __future__ import annotations

from ocm.observability.bootstrap import (
    drain,
    pre_log,
)
from ocm.observability.logger import (
    bind_pipeline,
    bootstrap_logging,
    configure_logging,
    is_logging_configured,
    setup_logging,
)

__all__ = [
    "bootstrap_logging",
    "configure_logging",
    "bind_pipeline",
    "is_logging_configured",
    "setup_logging",
    "pre_log",
    "drain",
    "push_metrics",
    "start_metrics_server",
    "MetricsRuntime",
    "MetricsMode",
    "init_metrics_runtime",
    "get_metrics_runtime",
    "PrometheusPusher",
    "NoopPusher",
]

# ── Metrics runtime + Prometheus ─────────────────────────────────────────────
from ocm.observability.metrics_runtime import (
    MetricsMode,
    MetricsRuntime,
    get_metrics_runtime,
    init_metrics_runtime,
)
from ocm.observability.prometheus import (
    push_metrics,
    start_metrics_server,
)
from ocm.observability.pushers import (
    NoopPusher,
    PrometheusPusher,
)
