"""
ocm/config/structured/market_data_feeds.py
=============================================
Hydra Structured Config para el bloque ``market_data.feeds``.

SSOT de tipos para Hydra. Solo define estructura y defaults.
Pydantic (schema.py::FeedsConfig) es el validador de reglas de negocio.

Migra config/market_data/feeds.yaml al flujo oficial:
    YAML → Hydra compose → este Structured Config → OmegaConf →
    Pydantic (schema.py) → AppConfig.feeds

REGLA: los campos de FeedsConfig AQUÍ y en schema.py::FeedsConfig
deben ser idénticos — mismo patrón que ObservabilityConfig/LoggingConfig.
El test tests/config/test_structured_parity.py verifica esto en CI
(extendido en el paso 3 de la migración para cubrir también feeds).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ExchangeFeedEntry:
    """Configuración de un único exchange dentro de market_data.feeds.

    Alineado 1:1 con market_data.application.feed_orchestrator.ExchangeFeedConfig
    — ese dataclass sigue siendo el contrato interno de FeedOrchestrator;
    ExchangeFeedEntry es su equivalente en el lado de configuración tipada.
    """

    enabled: bool = False
    symbols: List[str] = field(default_factory=list)


@dataclass
class FeedsKafkaConfig:
    # topic_trades: WS feeds publican aquí. Kafka brokers vienen de
    # AppConfig.integrations.kafka (SSOT de infra) — nunca de aquí.
    topic_trades: str = "trades.raw"


@dataclass
class FeedsConfig:
    # ingestion_mode: validado como Literal en schema.py (Pydantic L4).
    # Aquí queda como str — Hydra Structured Config no soporta Literal
    # de forma nativa en todas las versiones; la validación real de
    # los 3 valores permitidos ocurre en el modelo Pydantic espejo.
    ingestion_mode: str = "rest"
    kafka: FeedsKafkaConfig = field(default_factory=FeedsKafkaConfig)
    feeds: Dict[str, ExchangeFeedEntry] = field(default_factory=dict)
