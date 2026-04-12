from __future__ import annotations

"""
core/config/structured/observability.py
========================================
Hydra Structured Config para el bloque ``observability``.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "text"
    log_dir: str = "logs"
    rotation: str = "1 day"
    retention: str = "30 days"
    console: bool = True
    file: bool = True
    pipeline: bool = True
    loki_url: Optional[str] = None


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
