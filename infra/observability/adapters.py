from __future__ import annotations

"""
infra/observability/adapters.py
================================
Adaptadores concretos que implementan market_data.ports.observability.MetricsPusherPort.

REGLA: este módulo SÍ puede importar infra.*. Nadie del dominio
importa este módulo directamente — solo el composition root.
"""

from typing import Any, Mapping, Optional

from infra.observability.server import push_metrics as _push_metrics


class PrometheusPusher:
    """Adaptador Prometheus que satisface MetricsPusherPort."""

    def push(self, labels: Optional[Mapping[str, Any]] = None) -> None:
        _push_metrics(**(labels or {}))


class NoopPusher:
    """Pusher noop para tests y entornos sin Prometheus.

    Fail-Safe: no lanza excepciones, no bloquea el pipeline.
    """

    def push(self, labels: Optional[Mapping[str, Any]] = None) -> None:
        pass  # intencional
