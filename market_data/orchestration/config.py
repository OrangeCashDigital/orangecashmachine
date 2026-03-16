"""
market_data/orchestration/config.py
=====================================

Capa de compatibilidad hacia atrás.

Responsabilidad única
---------------------
Re-exportar todo desde core.config.schema para que los módulos
que aún importen desde aquí sigan funcionando sin cambios
durante la migración hacia core.config.

Migración
---------
El destino final es que todos los módulos importen directamente:

    from core.config import AppConfig, load_config

Una vez completada la migración, este archivo puede eliminarse.
Hasta entonces, actúa como alias transparente.

Principios aplicados
--------------------
• DRY    – no duplica código, solo re-exporta
• KISS   – archivo mínimo, sin lógica propia
• SOLID  – OCP: no modifica la fuente de verdad (core.config.schema)
"""

from __future__ import annotations

# Re-exportar todo desde la fuente de verdad
from core.config.schema import (
    AppConfig,
    ExchangeConfig,
    MarketsConfig,
    MarketConfig,
    PipelineConfig,
    HistoricalConfig,
    RealtimeConfig,
    RetryPolicy,
    DatasetsConfig,
    StorageConfig,
    SupportedExchange,
    DERIVATIVE_DATASET_KEYS,
    EXCHANGE_TASK_TIMEOUT,
    PIPELINE_TASK_TIMEOUT,
    CONFIG_PATH,
    _EXCHANGES_WITH_PASSPHRASE,
)

__all__ = [
    "AppConfig",
    "ExchangeConfig",
    "MarketsConfig",
    "MarketConfig",
    "PipelineConfig",
    "HistoricalConfig",
    "RealtimeConfig",
    "RetryPolicy",
    "DatasetsConfig",
    "StorageConfig",
    "SupportedExchange",
    "DERIVATIVE_DATASET_KEYS",
    "EXCHANGE_TASK_TIMEOUT",
    "PIPELINE_TASK_TIMEOUT",
    "CONFIG_PATH",
    "_EXCHANGES_WITH_PASSPHRASE",
]
