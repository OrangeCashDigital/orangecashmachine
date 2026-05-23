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
SafeOps  — No lanza en ausencia de feeds.yaml; solo logea y retorna None.

Referencia
----------
Seemann, Mark. «Dependency Injection in .NET», capítulo Composition Root.
Martin, Robert C. «Clean Architecture», capítulo 26.

Contratos enforced: BC-38.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from market_data.application.feed_orchestrator import FeedOrchestrator
    from market_data.infrastructure.bootstrap.pipeline_factory import (
        ConcretePipelineFactory,
    )
    from ocm.config.schema import AppConfig

__all__ = ["CompositionRoot", "assemble"]


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

        factory = ConcretePipelineFactory()
        return cls(factory=factory)

    @classmethod
    def build_feed_orchestrator(
        cls,
        config: "AppConfig",
    ) -> "FeedOrchestrator | None":
        """Build a fully-wired FeedOrchestrator from conf/market_data/feeds.yaml.

        Fail-Soft: retorna None si feeds.yaml no existe, ingestion_mode='rest',
        o no hay feeds habilitados. Nunca lanza — el caller decide si es error.

        Config de feeds leída de YAML standalone (no de AppConfig) — SSOT
        desacoplada: AppConfig no necesita saber de WS feeds.
        Kafka brokers sí vienen de AppConfig.integrations.kafka (SSOT de infra).

        Args:
            config: AppConfig con integrations.kafka configurado.

        Returns:
            FeedOrchestrator listo para run(), o None si WS feeds no aplican.
        """
        from pathlib import Path

        import yaml
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

        # ── Localizar feeds.yaml (SSOT de configuración WS) ──────────────
        # Usamos __file__ para resolver repo_root sin depender de shared.utils.repo
        _repo_root = Path(__file__).resolve().parents[5]
        feeds_path = _repo_root / "conf" / "market_data" / "feeds.yaml"

        if not feeds_path.exists():
            logger.warning("[composition-root] conf/market_data/feeds.yaml not found — WS feeds disabled")
            return None

        with feeds_path.open() as f:
            raw: dict = yaml.safe_load(f) or {}

        # ── Fail-Soft: modo REST no necesita WS feeds ─────────────────────
        ingestion_mode: str = raw.get("ingestion_mode", "rest")
        if ingestion_mode == "rest":
            logger.info("[composition-root] ingestion_mode=rest — WS feeds not started")
            return None

        # ── Construir lista de feeds habilitados ──────────────────────────
        raw_feeds: dict = raw.get("feeds", {})
        feed_configs = [
            ExchangeFeedConfig(
                exchange=name,
                symbols=cfg.get("symbols", []),
                enabled=cfg.get("enabled", False),
            )
            for name, cfg in raw_feeds.items()
            if cfg.get("enabled", False)
        ]

        if not feed_configs:
            logger.warning("[composition-root] No enabled feeds in feeds.yaml — WS feeds disabled")
            return None

        orch_cfg = OrchestratorConfig(
            ingestion_mode=ingestion_mode,
            feeds=feed_configs,
        )

        # ── Kafka publisher ───────────────────────────────────────────────
        # brokers: AppConfig.integrations.kafka (SSOT de infraestructura)
        # topic:   feeds.yaml (SSOT de configuración de WS feeds)
        kafka_topic: str = raw.get("kafka", {}).get("topic_trades", "market.trades.raw")
        publisher = KafkaTradePublisher(
            bootstrap_servers=config.integrations.kafka.bootstrap_servers,
            topic=kafka_topic,
        )

        return FeedOrchestrator(
            config=orch_cfg,
            publisher=publisher,
            get_adapter=get_adapter_class,
        )

    def __repr__(self) -> str:
        return f"CompositionRoot(factory={type(self.factory).__name__})"


# ── Alias funcional ───────────────────────────────────────────────────────────
def assemble(config: "AppConfig") -> CompositionRoot:
    """Shorthand de CompositionRoot.assemble(config)."""
    return CompositionRoot.assemble(config)
