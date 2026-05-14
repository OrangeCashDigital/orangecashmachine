from __future__ import annotations

"""
core/config/structured/observability.py
========================================
Hydra Structured Config para el bloque ``observability``.

SSOT de tipos para Hydra. Solo define estructura y defaults.
Pydantic (schema.py / core/observability/config.py) es el
validador de reglas de negocio.

REGLA: los campos de LoggingConfig AQUÍ y en
core/observability/config.py::LoggingConfig deben ser idénticos.
El test tests/config/test_structured_parity.py verifica esto en CI.

Campos opcionales de Loki (loki_url, loki_labels): default None / {}
para mantener compatibilidad con entornos sin Loki (dev, CI).
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "text"
    log_dir: str = "logs"
    rotation: str = "1 day"
    retention: str = "14 days"
    console: bool = True
    file: bool = True
    pipeline: bool = True
    # ── Loki (opcional) ───────────────────────────────────────────────────────
    # Alineado con core/observability/config.py::LoggingConfig.
    # None = Loki deshabilitado. Hydra valida tipo; Pydantic valida formato URL.
    loki_url: Optional[str] = None
    loki_labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricsConfig:
    # Default False — alineado con observability/metrics.yaml.
    # Solo production.yaml activa métricas explícitamente.
    enabled: bool = False
    exporter: str = "prometheus"
    port: int = 8000


@dataclass
class TracingConfig:
    enabled: bool = False


@dataclass
class ObservabilityConfig:
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    tracing: TracingConfig = field(default_factory=TracingConfig)
