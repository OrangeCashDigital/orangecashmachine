# -*- coding: utf-8 -*-
"""
market_data.infrastructure.bootstrap.composition_root
======================================================
Composition Root único y formal para el bounded context market_data.

Responsabilidad única
---------------------
Este módulo es el ÚNICO punto donde se decide qué implementación concreta
se inyecta en cada abstracción. Toda la decisión de cableado vive aquí.

Ningún módulo fuera de infrastructure/bootstrap/ puede instanciar
adaptadores concretos — enforced por el contrato BC-38.

Principios aplicados
--------------------
DIP      — Las capas internas (domain, ports, application) reciben abstracciones.
            CompositionRoot las conecta con implementaciones concretas.
SRP      — Una sola razón para cambiar: cambiar qué implementación se usa.
KISS     — API pública: CompositionRoot.assemble(config) + build_feed_orchestrator(config).
Fail-Fast — Valida AppConfig antes de instanciar cualquier adaptador.
Fail-Soft — build_feed_orchestrator retorna None si feeds no están configurados.
SafeOps  — No lanza si feeds no están configurados; solo logea y retorna None.

Referencia
----------
Seemann, Mark. «Dependency Injection in .NET», capítulo Composition Root.
Martin, Robert C. «Clean Architecture», capítulo 26.

Contratos enforced: BC-38.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from market_data.adapters.inbound.websocket.funding_producer import FundingKafkaProducer
    from market_data.adapters.inbound.websocket.liquidations_producer import LiquidationsKafkaProducer
    from market_data.adapters.inbound.websocket.oi_producer import OIKafkaProducer
    from market_data.adapters.inbound.websocket.orderbook_producer import OrderBookKafkaProducer
    from market_data.application.external_ingestion.orchestrator import (
        ExternalIngestionOrchestrator as ExternalIngestionOrchestrator,
    )
    from market_data.application.feed_orchestrator import FeedOrchestrator
    from market_data.infrastructure.bootstrap.pipeline_factory import (
        ConcretePipelineFactory,
    )
    from market_data.ports.inbound.external.polling import PollingSourcePort
    from ocm.config.schema import AppConfig


@dataclass(frozen=True, slots=True)
class WSProducerBundle:
    """
    Bundle inmutable de los 4 producers WS reales.

    Creado por CompositionRoot.build_ws_producers().
    Usado por main.py para gestionar el lifecycle (start/close) de todos
    los producers en un único punto.

    Campos
    ------
    orderbook    : OrderBookKafkaProducer  → orderbook.raw
    funding      : FundingKafkaProducer    → funding.raw
    oi           : OIKafkaProducer         → oi.raw
    liquidations : LiquidationsKafkaProducer → liquidations.raw
    """

    orderbook: "OrderBookKafkaProducer"
    funding: "FundingKafkaProducer"
    oi: "OIKafkaProducer"
    liquidations: "LiquidationsKafkaProducer"

    async def start_all(self) -> None:
        """Inicia los 4 producers. SafeOps por producer."""
        for producer in (self.orderbook, self.funding, self.oi, self.liquidations):
            await producer.start()

    async def close_all(self) -> None:
        """Cierra los 4 producers. SafeOps por producer."""
        for producer in (self.orderbook, self.funding, self.oi, self.liquidations):
            await producer.close()


__all__ = ["CompositionRoot", "WSProducerBundle", "assemble"]


@dataclass(frozen=True, slots=True)
class CompositionRoot:
    """
    Grafo de dependencias ensamblado para market_data.

    Inmutable tras construcción (frozen=True) — garantiza que nadie
    puede inyectar dependencias distintas después del arranque.

    Uso canónico
    ------------
        config   = load_config()                      # ocm.config pipeline L1-L5
        root     = CompositionRoot.assemble(config)   # único punto de cableado
        pipeline = root.factory.build(request)        # flujo normal de negocio
    """

    factory: "ConcretePipelineFactory"

    @classmethod
    def assemble(cls, config: "AppConfig") -> "CompositionRoot":
        """
        Ensambla el grafo completo de dependencias para market_data.

        Fail-Fast: valida config antes de instanciar cualquier adaptador.
        Si AppConfig está incompleto, falla aquí — no en el primer request.

        Args:
            config: AppConfig validado por el pipeline L1-L5 de ocm.config.

        Returns:
            CompositionRoot inmutable listo para producción.

        Raises:
            ValueError: si config es None.
        """
        if config is None:
            raise ValueError(
                "CompositionRoot.assemble() requiere AppConfig no-nula. "
                "El pipeline de config (L1-L5) debe completar antes de ensamblar."
            )

        from market_data.infrastructure.bootstrap.pipeline_factory import (
            ConcretePipelineFactory,
        )

        factory = ConcretePipelineFactory(cfg=config)
        return cls(factory=factory)

    @classmethod
    def build_feed_orchestrator(
        cls,
        config: "AppConfig",
    ) -> "FeedOrchestrator | None":
        """Build a fully-wired FeedOrchestrator from config.feeds (AppConfig).

        Fail-Soft: retorna None si ingestion_mode='rest' o no hay feeds
        habilitados. Nunca lanza — el caller decide si es error.

        Config de feeds proviene de AppConfig.feeds — poblada por el
        Configuration Service (pipeline Hydra L1-L5). SSOT único de
        configuración: AppConfig. No se lee YAML directamente aquí.
        Kafka brokers vienen de AppConfig.integrations.kafka (SSOT de infra).

        Args:
            config: AppConfig con integrations.kafka y feeds configurados.

        Returns:
            FeedOrchestrator listo para run(), o None si WS feeds no aplican.
        """
        from loguru import logger

        from market_data.adapters.outbound.kafka_trade_publisher import (
            KafkaTradePublisher,
        )
        from market_data.application.feed_orchestrator import (
            ExchangeFeedConfig,
            FeedOrchestrator,
            OrchestratorConfig,
        )
        from market_data.infrastructure.bootstrap.feed_registry import (
            get_adapter_class,
        )

        # ── Config de feeds (SSOT: AppConfig.feeds, poblado por Hydra) ────
        feeds_cfg = config.feeds

        # ── Fail-Soft: modo REST no necesita WS feeds ─────────────────────
        ingestion_mode: str = feeds_cfg.ingestion_mode
        if ingestion_mode == "rest":
            logger.info("[composition-root] ingestion_mode=rest — WS feeds not started")
            return None

        # ── Construir lista de feeds habilitados ──────────────────────────
        feed_configs = [
            ExchangeFeedConfig(
                exchange=name,
                symbols=entry.symbols,
                enabled=entry.enabled,
            )
            for name, entry in feeds_cfg.feeds.items()
            if entry.enabled
        ]

        if not feed_configs:
            logger.warning("[composition-root] No enabled feeds in config.feeds — WS feeds disabled")
            return None

        orch_cfg = OrchestratorConfig(
            ingestion_mode=ingestion_mode,
            feeds=feed_configs,
        )

        # ── Kafka publisher ───────────────────────────────────────────────
        # brokers: AppConfig.integrations.kafka (SSOT de infraestructura)
        # topic:   AppConfig.feeds.kafka.topic_trades (SSOT de config WS feeds)
        kafka_topic: str = feeds_cfg.kafka.topic_trades
        publisher = KafkaTradePublisher(
            bootstrap_servers=config.integrations.kafka.bootstrap_servers,
            topic=kafka_topic,
        )

        return FeedOrchestrator(
            config=orch_cfg,
            publisher=publisher,
            get_adapter=get_adapter_class,
        )

    @classmethod
    def build_external_ingestion_orchestrator(
        cls,
        config: "AppConfig",
    ) -> "ExternalIngestionOrchestrator | None":
        """Build a fully-wired ExternalIngestionOrchestrator from config.

        Fail-Soft: retorna None si external_ingestion no está habilitado o
        no hay fuentes habilitadas. Nunca lanza — el caller decide.

        Config (SSOT): AppConfig.external_ingestion (poblada por Hydra).
        Brokers: AppConfig.integrations.kafka (SSOT de infra).
        Adapters: factory local identidad → clase concreta.
        Publisher: ExternalKafkaEventPublisher sobre KafkaProducerAdapter.

        Args:
            config: AppConfig con integrations.kafka y external_ingestion.

        Returns:
            ExternalIngestionOrchestrator listo para run(), o None si no aplica.
        """
        from loguru import logger

        from market_data.adapters.inbound.external import (
            CoinglassPollingSource,
            CoinMarketCapPollingSource,
        )
        from market_data.adapters.outbound.external_kafka_publisher import (
            ExternalKafkaEventPublisher,
        )
        from market_data.application.external_ingestion.orchestrator import (
            ExternalIngestionOrchestrator,
            ExternalSourceRuntime,
        )
        from market_data.infrastructure.kafka.producer import KafkaProducerAdapter

        ext_cfg = config.external_ingestion
        if not ext_cfg.enabled:
            logger.info("[composition-root] external_ingestion.enabled=false — no se inicia")
            return None

        enabled = {sid: cfg for sid, cfg in ext_cfg.sources.items() if cfg.enabled}
        if not enabled:
            logger.warning("[composition-root] external_ingestion habilitado pero sin fuentes activas")
            return None

        factory: dict[str, Callable[[str], PollingSourcePort]] = {
            "coinglass": lambda api_key: CoinglassPollingSource(api_key=api_key),
            "coinmarketcap": lambda api_key: CoinMarketCapPollingSource(api_key=api_key),
        }

        sources: list[ExternalSourceRuntime] = []
        for source_id, cfg in enabled.items():
            if source_id not in factory:
                logger.warning("[composition-root] fuente externa desconocida, ignorada: {}", source_id)
                continue
            kafka_key = f"{source_id.upper()}_API_KEY"
            api_key = os.environ.get(kafka_key, "")
            if not api_key:
                logger.warning("[composition-root] fuente '{}' sin API key (env {}) — omitida", source_id, kafka_key)
                continue
            sources.append(
                ExternalSourceRuntime(
                    source_id=source_id,
                    metric=cfg.metric,
                    topic=cfg.topic,
                    enabled=cfg.enabled,
                    symbols=tuple(cfg.symbols),
                    schedule_every_s=cfg.schedule.every,
                    rate_limit_per_minute=cfg.rate_limit.per_minute,
                )
            )

        if not sources:
            logger.warning("[composition-root] external_ingestion: sin fuentes cableables")
            return None

        producer_adapter = KafkaProducerAdapter(
            bootstrap_servers=config.integrations.kafka.bootstrap_servers,
            client_id="ocm-external-ingestion",
        )
        publisher = ExternalKafkaEventPublisher(producer_adapter)

        def get_source(source_id: str) -> PollingSourcePort:
            return factory[source_id](os.environ.get(f"{source_id.upper()}_API_KEY", ""))

        return ExternalIngestionOrchestrator(
            sources=sources,
            get_source=get_source,
            publisher=publisher,
        )

    @classmethod
    def build_ws_producers(
        cls,
        bootstrap_servers: str = "kafka:9092",
    ) -> "WSProducerBundle":
        """
        Instancia y cablea los 4 producers WS reales con KafkaProducerPort.

        Cada producer recibe su propia instancia de KafkaProducerAdapter
        con un client_id único — evita colisiones de group_id en el broker.

        Fail-Fast: si bootstrap_servers está vacío lanza ValueError.
        SafeOps: los producers no conectan al broker aquí — lo hacen en start().

        Returns:
            WSProducerBundle con los 4 producers listos para start().
        """
        if not bootstrap_servers:
            raise ValueError("build_ws_producers: bootstrap_servers no puede ser vacío")

        from market_data.adapters.inbound.websocket.funding_producer import (
            FundingKafkaProducer,
        )
        from market_data.adapters.inbound.websocket.liquidations_producer import (
            LiquidationsKafkaProducer,
        )
        from market_data.adapters.inbound.websocket.oi_producer import OIKafkaProducer
        from market_data.adapters.inbound.websocket.orderbook_producer import (
            OrderBookKafkaProducer,
        )
        from market_data.infrastructure.kafka.producer import KafkaProducerAdapter

        return WSProducerBundle(
            orderbook=OrderBookKafkaProducer(
                KafkaProducerAdapter(
                    bootstrap_servers=bootstrap_servers,
                    client_id="ocm-ws-orderbook",
                )
            ),
            funding=FundingKafkaProducer(
                KafkaProducerAdapter(
                    bootstrap_servers=bootstrap_servers,
                    client_id="ocm-ws-funding",
                )
            ),
            oi=OIKafkaProducer(
                KafkaProducerAdapter(
                    bootstrap_servers=bootstrap_servers,
                    client_id="ocm-ws-oi",
                )
            ),
            liquidations=LiquidationsKafkaProducer(
                KafkaProducerAdapter(
                    bootstrap_servers=bootstrap_servers,
                    client_id="ocm-ws-liquidations",
                )
            ),
        )

    def __repr__(self) -> str:
        return f"CompositionRoot(factory={type(self.factory).__name__})"


# ── Alias funcional ───────────────────────────────────────────────────────────
def assemble(config: "AppConfig") -> CompositionRoot:
    """Shorthand de CompositionRoot.assemble(config)."""
    return CompositionRoot.assemble(config)
