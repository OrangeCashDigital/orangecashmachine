from __future__ import annotations

"""
infra/observability/adapters.py
================================
Adapters concretos que implementan MetricsPusherPort.

Regla de arquitectura
---------------------
Este módulo SÍ importa infra.*  — es la capa de infraestructura.
Nadie del dominio (market_data.*, trading.*) importa este módulo
directamente; solo el composition root (main.py) lo instancia.

Principios aplicados
--------------------
- DIP     : adapters implementan el puerto del dominio.
- SRP     : cada clase hace una sola cosa.
- SafeOps : NoopPusher nunca lanza; PrometheusPusher delega SafeOps
            a infra.observability.server.push_metrics.
- OCP     : añadir DatadogPusher, OtelPusher, etc. sin tocar el dominio.
"""

from typing import Any, Mapping, Optional

from ocm_platform.observability.prometheus import push_metrics as _push_metrics


class PrometheusPusher:
    """Adapter Prometheus que satisface MetricsPusherPort.

    Delega a infra.observability.server.push_metrics, que ya es SafeOps
    (captura excepciones internamente y loguea warning).

    Uso desde composition root
    --------------------------
        pusher = PrometheusPusher()
        pusher.push({"exchange": "binance", "gateway": "http://localhost:9091"})
    """

    def push(self, labels: Optional[Mapping[str, Any]] = None) -> None:
        _labels = labels or {}
        _push_metrics(
            exchange=_labels.get("exchange", "local"),
            gateway=_labels.get("gateway", "localhost:9091"),
        )


class NoopPusher:
    """Pusher no-op para tests y entornos sin Prometheus.

    Fail-Safe: nunca lanza excepción, nunca bloquea el pipeline.
    Úsalo cuando config.observability.metrics.enabled == False
    o en cualquier test que no necesite verificar el push real.
    """

    def push(self, labels: Optional[Mapping[str, Any]] = None) -> None:
        pass  # intencional — no-op
