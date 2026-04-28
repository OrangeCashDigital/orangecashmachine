from __future__ import annotations

"""
market_data/ports/observability.py
====================================
Puerto DIP para push de métricas.

Regla de arquitectura
---------------------
Este módulo NO importa nada de infra.*  ni de prometheus_client.
Solo define el contrato (Protocol). Los adapters concretos viven en
infra/observability/adapters.py y se inyectan desde el composition root.

Principios aplicados
---------------------
- DIP   : dominio depende de abstracción, nunca de infra concreta.
- ISP   : interfaz mínima — un solo método push().
- OCP   : nuevos pusher (Datadog, OTel, NOOP) sin tocar el dominio.
- SSOT  : un único lugar donde vive el contrato de métricas.
"""

from typing import Any, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class MetricsPusherPort(Protocol):
    """Contrato de push de métricas para el dominio market_data.

    Implementaciones concretas
    --------------------------
    - PrometheusPusher  : push real a Pushgateway (infra/observability/adapters.py)
    - NoopPusher        : no-op para tests y entornos sin Prometheus

    Convención de labels
    --------------------
    El caller pasa un mapping con las claves que el adapter necesita.
    Claves estándar usadas hoy:
        exchange (str) : nombre del exchange, ej. "binance"
        gateway  (str) : URL del Pushgateway, ej. "http://localhost:9091"
    """

    def push(self, labels: Optional[Mapping[str, Any]] = None) -> None:
        """Empuja métricas al backend configurado.

        Implementaciones deben ser SafeOps: nunca lanzar excepción al caller.

        Parameters
        ----------
        labels : mapping opcional con metadatos del push
            ej. {"exchange": "binance", "gateway": "http://localhost:9091"}
        """
        ...
