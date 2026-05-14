from __future__ import annotations

"""
core/config/structured/pipeline.py
===================================
Hydra Structured Config para el bloque ``pipeline``.

Propósito: validación de TIPOS en tiempo de composición Hydra,
antes de que OmegaConf resuelva interpolaciones y antes de que
Pydantic aplique reglas de negocio.

Regla: solo tipos primitivos + Optional. Sin lógica de negocio
(eso es responsabilidad de Pydantic en core/config/schema.py).

Invariante del sistema
----------------------
  historical.timeframes = ["1m"]            — fuente base (exchange)
  resample.targets      = [5m,15m,1h,4h,1d] — derivados (local)
  union = todos los TF disponibles para estrategias
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class RetryPolicyConfig:
    max_attempts: int = 5
    backoff_factor: int = 2
    jitter: bool = True


@dataclass
class HistoricalConfig:
    """Structured config para pipeline.historical.

    timeframes: ["1m"] — SSOT: solo la fuente base se descarga del exchange.
    Los TF derivados (5m, 15m, 1h, 4h, 1d) son responsabilidad de ResampleConfig.

    max_concurrent_tasks: None = resuelto dinámicamente en código
                                 (cpu_count // 2). Hydra valida que
                                 si se pasa un valor sea int >= 1.
    """
    start_date: str = "auto"
    auto_lookback_days: int = 3650
    backfill_mode: bool = False
    max_concurrent_tasks: Optional[int] = None
    timeframes: List[str] = field(default_factory=lambda: ["1m"])
    retry_policy: RetryPolicyConfig = field(default_factory=RetryPolicyConfig)


@dataclass
class ResampleConfig:
    """Structured config para pipeline.resample.

    SSOT de los timeframes producidos localmente por ResamplePipeline.
    Nunca se piden al exchange — se construyen a partir de source_tf.
    """
    targets: List[str] = field(
        default_factory=lambda: ["5m", "15m", "1h", "4h", "1d"]
    )
    source_tf: str = "1m"


@dataclass
class RealtimeConfig:
    reconnect_delay_seconds: int = 5
    heartbeat_timeout_seconds: int = 30
    snapshot_interval_seconds: int = 60
    max_stream_buffer: int = 50000
    drop_policy: str = "reject"


@dataclass
class PipelineConfig:
    """Structured config raíz para el bloque ``pipeline``."""
    historical: HistoricalConfig = field(default_factory=HistoricalConfig)
    resample:   ResampleConfig   = field(default_factory=ResampleConfig)
    realtime:   RealtimeConfig   = field(default_factory=RealtimeConfig)
